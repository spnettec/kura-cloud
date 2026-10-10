/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.eclipse.kura.testing.osgi.EquinoxExtension;
import org.eclipse.kura.testing.osgi.EquinoxRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.osgi.framework.Bundle;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;

/** Run seedQueue and replayQueue in separate Maven invocations with the same kura.durability.dir. */
@ExtendWith(EquinoxExtension.class)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class DataServiceProcessRestartProbe {
    private static final String API = "org.eclipse.kura.api";
    private static final String CONFIG = "org.eclipse.kura.configuration.ConfigurationService";
    private static final String DATABASE_FACTORY = "org.eclipse.kura.core.db.H2DbService";
    private static final String DATABASE_PID = "fixture.process.restart.h2";
    private static final String DATA_FACTORY = "org.eclipse.kura.data.DataService";
    private static final String TRANSPORT_FACTORY =
            "org.eclipse.kura.cloudconnection.sparkplug.mqtt.transport.SparkplugDataTransport";
    private static final String DATA_INTERFACE = "org.eclipse.kura.data.DataService";
    private static final String TRANSPORT_INTERFACE = "org.eclipse.kura.data.DataTransportService";
    private static final String ENDPOINT_PID = "fixture.process.restart.sparkplug";
    private static final String TOPIC = "fixture/process/restart/sparkplug";
    private static final byte[] PAYLOAD = "文件数据库跨 JVM 重启回放".getBytes(StandardCharsets.UTF_8);
    private static final Set<String> RESOLVE_ONLY = Set.of("org.eclipse.kura.core", "org.eclipse.kura.core.keystore",
            "org.eclipse.kura.core.inventory", "org.apache.felix.deploymentadmin",
            "org.eclipse.kura.rest.cloudconnection.provider", "org.eclipse.kura.rest.configuration.provider");

    @TempDir Path phaseData;
    private String previousConfiguration;
    private String previousCustomConfiguration;

    @BeforeEach
    void isolateHostConfiguration() throws Exception {
        Path defaults = Files.writeString(phaseData.resolve("kura.properties"), "# Isolated process-restart probe\n");
        previousConfiguration = System.getProperty("kura.configuration");
        previousCustomConfiguration = System.getProperty("kura.custom.configuration");
        System.setProperty("kura.configuration", defaults.toUri().toString());
        System.setProperty("kura.custom.configuration", defaults.toUri().toString());
    }

    @AfterEach
    void restoreHostConfiguration() {
        restoreProperty("kura.configuration", previousConfiguration);
        restoreProperty("kura.custom.configuration", previousCustomConfiguration);
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) { System.clearProperty(key); }
        else { System.setProperty(key, value); }
    }

    @Test
    void seedQueue(EquinoxRuntime runtime) throws Exception {
        Path store = sharedDirectory();
        assertFalse(Files.exists(store.resolve("durable-messages.mv.db")), "Seed requires a fresh file database");
        withRuntime(runtime, store, "tcp://127.0.0.1:1", null, context -> {
            try (var data = runtime.service(DATA_INTERFACE, filter(context.dataPid), Duration.ofSeconds(10))) {
                int id = (Integer) data.call("publish", TOPIC, PAYLOAD, 1, false, 5);
                assertTrue(((List<?>) data.call("getUnpublishedMessageIds", TOPIC)).contains(id));
                Properties seed = new Properties();
                seed.setProperty("jvm.pid", Long.toString(ProcessHandle.current().pid()));
                seed.setProperty("message.id", Integer.toString(id));
                seed.setProperty("data.pid", context.dataPid);
                seed.setProperty("transport.pid", context.transportPid);
                try (var out = Files.newOutputStream(store.resolve("seed.properties"))) {
                    seed.store(out, "Only public process and queue identifiers; no credentials");
                }
            }
        });
        assertTrue(Files.isRegularFile(store.resolve("durable-messages.mv.db")), "H2 file was not created");
    }

    @Test
    void replayQueue(EquinoxRuntime runtime) throws Exception {
        Path store = sharedDirectory();
        Properties seed = new Properties();
        try (var in = Files.newInputStream(store.resolve("seed.properties"))) {
            seed.load(in);
        }
        assertNotEquals(Long.parseLong(seed.getProperty("jvm.pid")), ProcessHandle.current().pid(),
                "Replay must run in a different Java process");
        int expectedId = Integer.parseInt(seed.getProperty("message.id"));
        assertTrue(Files.isRegularFile(store.resolve("durable-messages.mv.db")));

        Server broker = new Server();
        Properties options = new Properties();
        options.setProperty("host", "127.0.0.1");
        options.setProperty("port", "0");
        options.setProperty("allow_anonymous", "true");
        options.setProperty("persistence_enabled", "false");
        options.setProperty("telemetry_enabled", "false");
        try {
            broker.startServer(new MemoryConfig(options), List.of(), null, null, null);
            await(() -> {
                try { return broker.getPort() > 0; }
                catch (ConcurrentModificationException bindingInProgress) { return false; }
            }, "Broker did not bind");
            String uri = "tcp://127.0.0.1:" + broker.getPort();
            LinkedBlockingQueue<MqttMessage> received = new LinkedBlockingQueue<>();
            try (MqttClient observer = new MqttClient(uri, "process-restart-observer", new MemoryPersistence())) {
                observer.setCallback(new MqttCallback() {
                    public void connectionLost(Throwable cause) { }
                    public void deliveryComplete(IMqttDeliveryToken token) { }
                    public void messageArrived(String topic, MqttMessage message) {
                        if (TOPIC.equals(topic)) { received.add(message); }
                    }
                });
                observer.connect();
                observer.subscribe(TOPIC, 1);
                withRuntime(runtime, store, uri, seed, context -> {
                    assertEquals(seed.getProperty("data.pid"), context.dataPid);
                    assertEquals(seed.getProperty("transport.pid"), context.transportPid);
                    try (var data = runtime.service(DATA_INTERFACE, filter(context.dataPid), Duration.ofSeconds(10))) {
                        assertTrue(((List<?>) data.call("getUnpublishedMessageIds", TOPIC)).contains(expectedId),
                                "Queued message ID was not recovered from the file database");
                        assertTrue(received.isEmpty(), "Queued message reached the broker before reconnect");
                        data.call("connect");
                        MqttMessage delivered = received.poll(10, TimeUnit.SECONDS);
                        assertNotNull(delivered, "Recovered message was not replayed");
                        assertArrayEquals(PAYLOAD, delivered.getPayload());
                        assertEquals(1, delivered.getQos());
                        assertFalse(delivered.isRetained());
                        await(() -> !((List<?>) data.call("getUnpublishedMessageIds", TOPIC)).contains(expectedId),
                                "Replayed message stayed in the unpublished queue");
                        data.call("disconnect", 0L);
                        Properties replay = new Properties();
                        replay.setProperty("jvm.pid", Long.toString(ProcessHandle.current().pid()));
                        replay.setProperty("message.id", Integer.toString(expectedId));
                        replay.setProperty("payload.bytes", Integer.toString(delivered.getPayload().length));
                        replay.setProperty("queue.drained", "true");
                        try (var out = Files.newOutputStream(store.resolve("replay.properties"))) {
                            replay.store(out, "Successful replay in a second Java process");
                        }
                    }
                });
                observer.disconnect();
            }
        } finally {
            broker.stopServer();
        }
    }

    private static Path sharedDirectory() throws Exception {
        String value = System.getProperty("kura.durability.dir");
        assertNotNull(value, "Pass -Dkura.durability.dir=/absolute/isolated/path");
        Path store = Path.of(value).toAbsolutePath();
        Files.createDirectories(store);
        return store;
    }

    private void withRuntime(EquinoxRuntime runtime, Path store, String uri, Properties seed,
            Scenario body) throws Exception {
        List<Bundle> bundles = new ArrayList<>();
        try (var paths = Files.list(Path.of("target/it-bundles"))) {
            for (Path jar : paths.filter(p -> p.toString().endsWith(".jar"))
                    .filter(p -> !p.getFileName().toString().equals("org.eclipse.kura.rest.cloudconnection.provider.jar"))
                    .sorted().toList()) {
                bundles.add(runtime.install(jar));
            }
        }
        runtime.resolve(bundles);
        Files.createDirectories(phaseData.resolve("snapshots"));
        Properties properties = new Properties();
        properties.setProperty("kura.snapshots.encrypt", "true");
        try (var system = runtime.register(API, "org.eclipse.kura.system.SystemService", (proxy, method, args) ->
                    switch (method.getName()) {
                        case "getKuraDataDirectory", "getKuraHome", "getKuraConfigDirectory" -> phaseData.toString();
                        case "getKuraSnapshotsDirectory" -> phaseData.resolve("snapshots").toString();
                        case "getKuraSnapshotsCount" -> 10;
                        case "getProperties" -> properties;
                        case "getPrimaryMacAddress" -> "02:00:00:00:00:01";
                        default -> boundaryValue(proxy, method, args);
                    }, Map.of());
             var admin = runtime.register(API, "org.eclipse.kura.system.SystemAdminService",
                     DataServiceProcessRestartProbe::boundaryValue, Map.of());
             var watchdog = runtime.register(API, "org.eclipse.kura.watchdog.WatchdogService",
                     DataServiceProcessRestartProbe::boundaryValue, Map.of());
             var status = runtime.register(API, "org.eclipse.kura.status.CloudConnectionStatusService",
                     DataServiceProcessRestartProbe::boundaryValue, Map.of())) {
            runtime.start(bundles.stream().filter(b -> !RESOLVE_ONLY.contains(b.getSymbolicName())).toList());
            try (var configuration = runtime.service(CONFIG, null, Duration.ofSeconds(10))) {
                configuration.call("createFactoryConfiguration", DATABASE_FACTORY, DATABASE_PID,
                        Map.of("db.connector.url", "jdbc:h2:file:" + store.resolve("durable-messages")), true);
                try (var database = runtime.service("org.eclipse.kura.db.BaseDbService", filter(DATABASE_PID),
                        Duration.ofSeconds(10))) {
                    assertEquals("org.eclipse.kura.db.h2db.provider", database.provider().getSymbolicName());
                    String dataPid;
                    String transportPid;
                    if (seed == null) {
                        try (var factory = runtime.service("org.eclipse.kura.cloudconnection.factory.CloudConnectionFactory",
                                "(service.pid=org.eclipse.kura.cloudconnection.sparkplug.mqtt.factory.SparkplugCloudConnectionFactory)",
                                Duration.ofSeconds(10))) {
                            factory.call("createConfiguration", ENDPOINT_PID, "持久化测试", "跨 JVM 回放");
                            List<?> stack = (List<?>) factory.call("getStackComponentsPids", ENDPOINT_PID);
                            assertEquals(2, stack.size());
                            dataPid = stack.get(0).toString();
                            transportPid = stack.get(1).toString();
                        }
                        updateConfiguration(configuration, transportPid, transportProperties(uri));
                        updateConfiguration(configuration, dataPid, dataProperties(transportPid));
                    } else {
                        dataPid = seed.getProperty("data.pid");
                        transportPid = seed.getProperty("transport.pid");
                        configuration.call("createFactoryConfiguration", TRANSPORT_FACTORY, transportPid,
                                transportProperties(uri), true);
                        configuration.call("createFactoryConfiguration", DATA_FACTORY, dataPid,
                                dataProperties(transportPid), true);
                    }
                    body.run(new Context(dataPid, transportPid));
                }
            }
        }
    }

    private static Map<String, Object> transportProperties(String uri) {
        return Map.of("server.uris", uri, "client.id", "process-restart-client",
                "group.id", "durable-group", "node.id", "durable-node");
    }

    private static Map<String, Object> dataProperties(String transportPid) {
        return Map.of("DataTransportService.target", filter(transportPid),
                "store.db.service.pid", DATABASE_PID);
    }

    private static void updateConfiguration(EquinoxRuntime.Service configuration, String pid,
            Map<String, Object> updates) throws Exception {
        Object current = configuration.call("getComponentConfiguration", pid);
        Class<?> componentConfiguration = configuration.provider()
                .loadClass("org.eclipse.kura.configuration.ComponentConfiguration");
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = new HashMap<>((Map<String, Object>)
                EquinoxRuntime.invoke(componentConfiguration, current, "getConfigurationProperties"));
        properties.putAll(updates);
        configuration.call("updateConfiguration", pid, properties, true);
    }

    private static String filter(String pid) { return "(kura.service.pid=" + pid + ")"; }

    private static void await(Callable<Boolean> condition, String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!condition.call() && System.nanoTime() < deadline) { Thread.sleep(20); }
        assertTrue(condition.call(), message);
    }

    private static Object boundaryValue(Object proxy, Method method, Object[] args) {
        return switch (method.getName()) {
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            case "toString" -> "isolated host boundary";
            default -> {
                Class<?> type = method.getReturnType();
                if (type == boolean.class) { yield false; }
                if (type == int.class) { yield 0; }
                if (type == long.class) { yield 0L; }
                if (type == String.class) { yield "fixture"; }
                if (List.class.isAssignableFrom(type)) { yield List.of(); }
                if (Set.class.isAssignableFrom(type)) { yield Set.of(); }
                yield null;
            }
        };
    }

    private record Context(String dataPid, String transportPid) { }

    @FunctionalInterface
    private interface Scenario { void run(Context context) throws Exception; }
}
