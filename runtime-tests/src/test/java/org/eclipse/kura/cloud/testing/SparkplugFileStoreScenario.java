/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.eclipse.kura.testing.osgi.EquinoxRuntime;
import org.eclipse.kura.testing.osgi.EquinoxRuntime.Service;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;

/** Restarts the real SCR DataService and H2 factory around a queued Sparkplug transport message. */
final class SparkplugFileStoreScenario {
    private static final String BASE = "org.eclipse.kura.cloudconnection.sparkplug.mqtt.";
    private static final String ENDPOINT_PID = "fixture.durable.sparkplug";
    private static final String DATABASE_FACTORY = "org.eclipse.kura.core.db.H2DbService";
    private static final String DATA_FACTORY = "org.eclipse.kura.data.DataService";
    private static final String DATA_INTERFACE = "org.eclipse.kura.data.DataService";
    private static final String TRANSPORT_INTERFACE = "org.eclipse.kura.data.DataTransportService";
    private static final String TOPIC = "fixture/durable/sparkplug";

    static void run(EquinoxRuntime runtime, Service configuration, Path directory, String databasePid) throws Exception {
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
            try (MqttClient observer = new MqttClient(uri, "durability-observer", new MemoryPersistence());
                 var factory = runtime.service("org.eclipse.kura.cloudconnection.factory.CloudConnectionFactory",
                         "(service.pid=" + BASE + "factory.SparkplugCloudConnectionFactory)", Duration.ofSeconds(10))) {
                observer.setCallback(new MqttCallback() {
                    public void connectionLost(Throwable cause) { }
                    public void deliveryComplete(IMqttDeliveryToken token) { }
                    public void messageArrived(String topic, MqttMessage message) {
                        if (TOPIC.equals(topic)) { received.add(message); }
                    }
                });
                observer.connect();
                observer.subscribe(TOPIC, 1);
                factory.call("createConfiguration", ENDPOINT_PID, "持久化测试", "文件数据库重启回放");
                List<?> stack = (List<?>) factory.call("getStackComponentsPids", ENDPOINT_PID);
                assertEquals(2, stack.size());
                String dataPid = stack.get(0).toString();
                String transportPid = stack.get(1).toString();
                try (var transport = runtime.service(TRANSPORT_INTERFACE, filter(transportPid), Duration.ofSeconds(10))) {
                    assertEquals("org.eclipse.kura.cloudconnection.sparkplug.mqtt.provider",
                            transport.provider().getSymbolicName());
                    Object transportConfiguration = configuration.call("getComponentConfiguration", transportPid);
                    Class<?> componentConfiguration = configuration.provider()
                            .loadClass("org.eclipse.kura.configuration.ComponentConfiguration");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> transportProperties = new HashMap<>((Map<String, Object>)
                            EquinoxRuntime.invoke(componentConfiguration, transportConfiguration,
                                    "getConfigurationProperties"));
                    transportProperties.putAll(Map.of("server.uris", uri, "client.id", "durable-sparkplug-client",
                            "group.id", "durable-group", "node.id", "durable-node"));
                    configuration.call("updateConfiguration", transportPid, transportProperties, true);
                    byte[] payload = "文件持久化重启后同一条消息".getBytes(StandardCharsets.UTF_8);
                    int id;
                    try (var data = runtime.service(DATA_INTERFACE, filter(dataPid), Duration.ofSeconds(10))) {
                        assertEquals("org.eclipse.kura.cloud.base.provider", data.provider().getSymbolicName());
                        id = (Integer) data.call("publish", TOPIC, payload, 1, false, 5);
                        assertTrue(((List<?>) data.call("getUnpublishedMessageIds", TOPIC)).contains(id));
                        assertTrue(received.isEmpty(), "Offline message reached the broker before restart");
                    }

                    configuration.call("deleteFactoryConfiguration", dataPid, true);
                    await(() -> !runtime.hasService(DATA_INTERFACE, filter(dataPid)), "DataService did not stop");
                    configuration.call("deleteFactoryConfiguration", databasePid, true);
                    await(() -> !runtime.hasService("org.eclipse.kura.db.BaseDbService", filter(databasePid)),
                            "H2 service did not stop");
                    assertTrue(Files.isRegularFile(directory.resolve("durable-messages.mv.db")),
                            "H2 did not write a file database");

                    configuration.call("createFactoryConfiguration", DATABASE_FACTORY, databasePid,
                            Map.of("db.connector.url", "jdbc:h2:file:" + directory.resolve("durable-messages")), true);
                    try (var database = runtime.service("org.eclipse.kura.db.BaseDbService",
                            filter(databasePid), Duration.ofSeconds(10))) {
                        assertEquals("org.eclipse.kura.db.h2db.provider", database.provider().getSymbolicName());
                        configuration.call("createFactoryConfiguration", DATA_FACTORY, dataPid,
                                Map.of("DataTransportService.target", filter(transportPid),
                                        "store.db.service.pid", databasePid), true);
                        try (var restored = runtime.service(DATA_INTERFACE, filter(dataPid), Duration.ofSeconds(10))) {
                            assertTrue(((List<?>) restored.call("getUnpublishedMessageIds", TOPIC)).contains(id),
                                    "Queued message ID changed or disappeared across service restart");
                            assertTrue(received.isEmpty(), "Queued message was sent before reconnect");
                            restored.call("connect");
                            MqttMessage delivered = received.poll(10, TimeUnit.SECONDS);
                            assertNotNull(delivered, "Persisted message was not replayed");
                            assertArrayEquals(payload, delivered.getPayload());
                            assertEquals(1, delivered.getQos());
                            assertFalse(delivered.isRetained());
                            await(() -> !((List<?>) restored.call("getUnpublishedMessageIds", TOPIC)).contains(id),
                                    "Replayed message stayed in the unpublished queue");
                            restored.call("disconnect", 0L);
                        }
                    }
                }
                factory.call("deleteConfiguration", ENDPOINT_PID);
                observer.disconnect();
            }
        } finally {
            broker.stopServer();
        }
    }

    private static String filter(String pid) { return "(kura.service.pid=" + pid + ")"; }

    private static void await(Callable<Boolean> condition, String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!condition.call() && System.nanoTime() < deadline) { Thread.sleep(20); }
        assertTrue(condition.call(), message);
    }
}
