/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.osgi.framework.Bundle;

/** Factory creation crosses real SCR/ConfigAdmin, bundle classloaders and cloud services. */
@ExtendWith(EquinoxExtension.class)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class CloudFactoryRuntimeIT {
    private static final String API = "org.eclipse.kura.api";
    private static final String CONFIG = "org.eclipse.kura.configuration.ConfigurationService";
    private static final String CLOUD = "org.eclipse.kura.cloud.CloudService";
    private static final String PUBLISHER = "org.eclipse.kura.cloudconnection.publisher.CloudPublisher";
    private static final String CLOUD_BUNDLE = "org.eclipse.kura.cloudconnection.kapua.mqtt.provider";
    private static final String CLOUD_PID = "fixture.custom.connection";
    private static final String CLOUD_NAME = "测试云连接";
    private static final String CLOUD_DESCRIPTION = "保留本地名称与描述";
    private static final String DATABASE_PID = "org.eclipse.kura.db.H2DbService";
    private static final Set<String> RESOLVE_ONLY = Set.of("org.eclipse.kura.core", "org.eclipse.kura.core.keystore",
            "org.eclipse.kura.core.inventory", "org.apache.felix.deploymentadmin",
            "org.eclipse.kura.rest.cloudconnection.provider", "org.eclipse.kura.rest.configuration.provider");
    @TempDir Path data;
    private String previousConfiguration;
    private String previousCustomConfiguration;

    @BeforeEach
    void isolateHostConfiguration() throws Exception {
        Path defaults = Files.writeString(data.resolve("kura.properties"), "# Isolated cloud test defaults\n");
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
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    enum Scenario { STACK, PUBLISHER, MQTT_JSON, MQTT_PROTOBUF, MQTT_TAMPER, SPARKPLUG, DURABILITY, REST, REST_EARLY, TLS, WSS, SPARKPLUG_TLS }

    @ParameterizedTest(name = "real factory scenario: {0}")
    @EnumSource(value = Scenario.class, names = { "SPARKPLUG", "DURABILITY", "REST", "REST_EARLY", "TLS", "WSS", "SPARKPLUG_TLS" }, mode = EnumSource.Mode.EXCLUDE)
    void serviceExistsThroughRealFactory(Scenario scenario, EquinoxRuntime runtime) throws Exception {
        verifyFactoryScenario(scenario, runtime);
    }

    @Test
    void sparkplugFactoryPipeline(EquinoxRuntime runtime) throws Exception {
        verifyFactoryScenario(Scenario.SPARKPLUG, runtime);
    }

    @Test
    void sparkplugFileStoreRestart(EquinoxRuntime runtime) throws Exception {
        verifyFactoryScenario(Scenario.DURABILITY, runtime);
    }

    @Test
    void restFactoryPipeline(EquinoxRuntime runtime) throws Exception {
        verifyFactoryScenario(Scenario.REST, runtime);
    }

    @Test
    void restInitialStartup(EquinoxRuntime runtime) throws Exception {
        verifyFactoryScenario(Scenario.REST_EARLY, runtime);
    }

    @Test
    void tlsFilesystemPipeline(EquinoxRuntime runtime) throws Exception {
        verifyFactoryScenario(Scenario.TLS, runtime);
    }

    @Test
    void wssFilesystemPipeline(EquinoxRuntime runtime) throws Exception {
        verifyFactoryScenario(Scenario.WSS, runtime);
    }

    @Test
    void sparkplugTlsFilesystemPipeline(EquinoxRuntime runtime) throws Exception {
        verifyFactoryScenario(Scenario.SPARKPLUG_TLS, runtime);
    }

    private void verifyFactoryScenario(Scenario scenario, EquinoxRuntime runtime) throws Exception {
        boolean withPublisher = scenario != Scenario.STACK;
        assertThrows(ClassNotFoundException.class, () -> Class.forName(CONFIG));
        List<Bundle> bundles = new ArrayList<>();
        try (var paths = Files.list(Path.of("target/it-bundles"))) {
            for (Path jar : paths.filter(p -> p.toString().endsWith(".jar"))
                    .filter(p -> scenario == Scenario.REST_EARLY || !p.getFileName().toString().equals("org.eclipse.kura.rest.cloudconnection.provider.jar")).sorted().toList()) {
                bundles.add(runtime.install(jar));
            }
        }
        runtime.resolve(bundles);
        Files.createDirectories(data.resolve("snapshots"));
        if (scenario == Scenario.REST_EARLY) {
            try (var source = getClass().getResourceAsStream("/rest-role-snapshot.xml")) {
                assertNotNull(source);
                Files.copy(source, data.resolve("snapshots/snapshot_0.xml"));
            }
        }
        Properties properties = new Properties();
        properties.setProperty("kura.snapshots.encrypt", "true");
        try (var system = runtime.register(API, "org.eclipse.kura.system.SystemService", (proxy, method, args) ->
                    switch (method.getName()) {
                        case "getKuraDataDirectory", "getKuraHome", "getKuraConfigDirectory" -> data.toString();
                        case "getKuraSnapshotsDirectory" -> data.resolve("snapshots").toString();
                        case "getKuraSnapshotsCount" -> 10;
                        case "getProperties" -> properties;
                        case "getPrimaryMacAddress" -> "02:00:00:00:00:01";
                        default -> boundaryValue(proxy, method, args);
                    }, Map.of());
             var admin = runtime.register(API, "org.eclipse.kura.system.SystemAdminService",
                     CloudFactoryRuntimeIT::boundaryValue, Map.of());
             var watchdog = runtime.register(API, "org.eclipse.kura.watchdog.WatchdogService",
                     CloudFactoryRuntimeIT::boundaryValue, Map.of());
             var status = runtime.register(API, "org.eclipse.kura.status.CloudConnectionStatusService",
                     CloudFactoryRuntimeIT::boundaryValue, Map.of())) {
            runtime.start(bundles.stream().filter(b -> !RESOLVE_ONLY.contains(b.getSymbolicName())).toList());
            try (var configuration = runtime.service(CONFIG, null, Duration.ofSeconds(10));
                 var factory = runtime.service("org.eclipse.kura.cloudconnection.factory.CloudConnectionFactory",
                         "(service.pid=org.eclipse.kura.core.cloud.factory.DefaultCloudServiceFactory)", Duration.ofSeconds(10))) {
                assertEquals(CLOUD, factory.call("getFactoryPid"));
                configuration.call("createFactoryConfiguration", "org.eclipse.kura.core.db.H2DbService", DATABASE_PID,
                        Map.of("db.connector.url", scenario == Scenario.DURABILITY
                                ? "jdbc:h2:file:" + data.resolve("durable-messages")
                                : "jdbc:h2:mem:cloudFactory"), true);
                try (var database = runtime.service("org.eclipse.kura.db.BaseDbService",
                        "(kura.service.pid=" + DATABASE_PID + ")", Duration.ofSeconds(10));
                     var connection = (Connection) database.call("getConnection");
                     var statement = connection.createStatement();
                     var result = statement.executeQuery("SELECT 42")) {
                    assertEquals("org.eclipse.kura.db.h2db.provider", database.provider().getSymbolicName());
                    assertTrue(result.next());
                    assertEquals(42, result.getInt(1));
                }
                assertTrue(((Set<?>) configuration.call("getFactoryComponentPids"))
                        .contains("org.eclipse.kura.core.db.H2DbService"), "H2 database factory must be discoverable");
                assertTrue(((Set<?>) configuration.call("getFactoryComponentPids"))
                        .contains("org.eclipse.kura.core.db.H2DbServer"), "H2 server remains a separate factory");
                verifyDatabaseLocalization(runtime, bundles);
                Object defaults = configuration.call("getDefaultComponentConfiguration", "org.eclipse.kura.core.db.H2DbService");
                Map<?, ?> defaultProperties = (Map<?, ?>) EquinoxRuntime.invoke(
                        configuration.provider().loadClass("org.eclipse.kura.configuration.ComponentConfiguration"),
                        defaults, "getConfigurationProperties");
                assertEquals("jdbc:h2:mem:kuradb", defaultProperties.get("db.connector.url"));
                assertEquals("SA", defaultProperties.get("db.user"));
                assertEquals(900, defaultProperties.get("db.checkpoint.interval.seconds"));
                assertEquals(15, defaultProperties.get("db.defrag.interval.minutes"));
                assertEquals(10, defaultProperties.get("db.connection.pool.max.size"));
                if (scenario == Scenario.TLS || scenario == Scenario.WSS || scenario == Scenario.SPARKPLUG_TLS) {
                    CloudTlsRuntimeScenario.run(runtime, configuration, data, scenario == Scenario.SPARKPLUG_TLS,
                            scenario == Scenario.WSS);
                    configuration.call("deleteFactoryConfiguration", DATABASE_PID, true);
                    return;
                }
                if (scenario == Scenario.REST || scenario == Scenario.REST_EARLY) {
                    if (scenario == Scenario.REST_EARLY) {
                        assertEquals(Bundle.ACTIVE, runtime.bundle("org.eclipse.kura.rest.cloudconnection.provider").getState(),
                                "Initial-start scenario must exercise REST before the late-arrival helper");
                    }
                    CloudRestRuntimeScenario.run(runtime);
                    configuration.call("deleteFactoryConfiguration", DATABASE_PID, true);
                    return;
                }
                if (scenario == Scenario.SPARKPLUG) {
                    SparkplugRuntimeScenario.run(runtime, configuration);
                    configuration.call("deleteFactoryConfiguration", DATABASE_PID, true);
                    return;
                }
                if (scenario == Scenario.DURABILITY) {
                    SparkplugFileStoreScenario.run(runtime, configuration, data, DATABASE_PID);
                    configuration.call("deleteFactoryConfiguration", DATABASE_PID, true);
                    return;
                }
                assertFalse(runtime.hasService(CLOUD, "(kura.service.pid=" + CLOUD_PID + ")"));
                factory.call("createConfiguration", CLOUD_PID, CLOUD_NAME, CLOUD_DESCRIPTION);
                List<?> stack;
                try (var cloud = runtime.service(CLOUD, "(kura.service.pid=" + CLOUD_PID + ")", Duration.ofSeconds(10))) {
                    assertEquals(CLOUD_BUNDLE, cloud.provider().getSymbolicName());
                    assertEquals(CLOUD_PID, cloud.property("kura.service.pid"));
                    assertEquals(CLOUD, cloud.property("service.factoryPid"));
                    assertEquals(CLOUD_NAME, cloud.property("kura.cloud.factory.name"));
                    assertEquals(CLOUD_DESCRIPTION, cloud.property("kura.cloud.factory.desc"));
                    assertEquals(CLOUD_NAME, factory.call("getCloudName", CLOUD_PID));
                    Object cloudConfig = configuration.call("getComponentConfiguration", CLOUD_PID);
                    Map<?, ?> configProperties = (Map<?, ?>) EquinoxRuntime.invoke(
                            cloud.provider().loadClass("org.eclipse.kura.configuration.ComponentConfiguration"),
                            cloudConfig, "getConfigurationProperties");
                    assertEquals(CLOUD_PID, configProperties.get("kura.service.pid"));
                    assertEquals(CLOUD_NAME, configProperties.get("kura.cloud.factory.name"));
                    assertEquals(CLOUD_DESCRIPTION, configProperties.get("kura.cloud.factory.desc"));
                    assertFalse((Boolean) cloud.call("isConnected"));
                    assertEquals(API, runtime.packageProvider(CLOUD_BUNDLE, "org.eclipse.kura.cloud"));
                    stack = (List<?>) factory.call("getStackComponentsPids", CLOUD_PID);
                    assertEquals(3, stack.size());
                    assertEquals(CLOUD_PID, stack.getFirst());
                    assertTrue(stack.stream().allMatch(String.class::isInstance));
                    assertEquals(Set.of(CLOUD_PID), factory.call("getManagedCloudConnectionPids"));
                    assertEquals("(kura.service.pid=" + stack.get(1) + ")", cloud.property("DataService.target"));
                    try (var dataService = runtime.service("org.eclipse.kura.data.DataService",
                                 "(kura.service.pid=" + stack.get(1) + ")", Duration.ofSeconds(5));
                         var transport = runtime.service("org.eclipse.kura.data.DataTransportService",
                                 "(kura.service.pid=" + stack.get(2) + ")", Duration.ofSeconds(5))) {
                        assertEquals("org.eclipse.kura.cloud.base.provider", dataService.provider().getSymbolicName());
                        assertEquals("org.eclipse.kura.cloud.base.provider", transport.provider().getSymbolicName());
                        assertEquals("(kura.service.pid=" + stack.get(2) + ")",
                                dataService.property("DataTransportService.target"));
                    }
                    if (withPublisher) {
                        configuration.call("createFactoryConfiguration", "org.eclipse.kura.cloud.publisher.CloudPublisher",
                                "fixture.publisher", Map.of("cloud.endpoint.service.pid", CLOUD_PID), true);
                        try (var publisher = runtime.service(PUBLISHER, "(kura.service.pid=fixture.publisher)", Duration.ofSeconds(10))) {
                            assertEquals(CLOUD_BUNDLE, publisher.provider().getSymbolicName());
                            assertEquals(CLOUD_PID, publisher.property("cloud.endpoint.service.pid"));
                            // A bound publisher rejects null input after resolving its real CloudService.
                            // An unbound publisher would instead throw SERVICE_UNAVAILABLE.
                            assertThrows(IllegalArgumentException.class, () -> publisher.call("publish", (Object) null));
                            if (scenario == Scenario.MQTT_JSON || scenario == Scenario.MQTT_PROTOBUF
                                    || scenario == Scenario.MQTT_TAMPER) {
                                CloudMqttScenario.run(runtime, configuration, cloud, publisher, stack,
                                        scenario == Scenario.MQTT_PROTOBUF ? "kura-protobuf" : "simple-json",
                                        scenario == Scenario.MQTT_TAMPER);
                            }
                        }
                        configuration.call("deleteFactoryConfiguration", "fixture.publisher", true);
                        awaitAbsent(() -> runtime.hasService(PUBLISHER, "(kura.service.pid=fixture.publisher)"));
                    }
                }
                factory.call("deleteConfiguration", CLOUD_PID);
                for (Object pid : stack) {
                    awaitAbsent(() -> runtime.hasService("org.eclipse.kura.configuration.ConfigurableComponent",
                            "(kura.service.pid=" + pid + ")"));
                }
                assertEquals(Set.of(), factory.call("getManagedCloudConnectionPids"));
                configuration.call("deleteFactoryConfiguration", DATABASE_PID, true);
            } catch (Exception | Error failure) {
                runtime.diagnose("cloud-factory-scenario");
                throw failure;
            }
        }
    }

    private static void verifyDatabaseLocalization(EquinoxRuntime runtime, List<Bundle> bundles) throws Exception {
        Bundle database = bundles.stream().filter(b -> "org.eclipse.kura.db.h2db.provider".equals(b.getSymbolicName()))
                .findFirst().orElseThrow();
        try (var metatype = runtime.service("org.osgi.service.metatype.MetaTypeService", null, Duration.ofSeconds(5))) {
            Object information = metatype.call("getMetaTypeInformation", database);
            Class<?> informationContract = metatype.provider().loadClass("org.osgi.service.metatype.MetaTypeInformation");
            Class<?> definitionContract = metatype.provider().loadClass("org.osgi.service.metatype.ObjectClassDefinition");
            for (var locale : Map.of("en", "DbService", "zh", "H2 数据库服务").entrySet()) {
                Object definition = EquinoxRuntime.invoke(informationContract, information, "getObjectClassDefinition",
                        "org.eclipse.kura.core.db.H2DbService", locale.getKey());
                assertEquals(locale.getValue(), EquinoxRuntime.invoke(definitionContract, definition, "getName"));
                Object[] attributes = (Object[]) EquinoxRuntime.invoke(definitionContract, definition, "getAttributeDefinitions", -1);
                assertEquals(6, attributes.length);
            }
        }
    }

    private static void awaitAbsent(Callable<Boolean> present) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (present.call() && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertFalse(present.call(), "Deleted factory service is still registered");
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
}
