/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import org.eclipse.kura.testing.osgi.EquinoxRuntime;
import org.eclipse.kura.testing.osgi.EquinoxRuntime.Service;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/** Sparkplug services are created by SCR; only the external broker and observer run here. */
final class SparkplugRuntimeScenario {
    private static final String BASE = "org.eclipse.kura.cloudconnection.sparkplug.mqtt.";
    private static final String BUNDLE = "org.eclipse.kura.cloudconnection.sparkplug.mqtt.provider";
    private static final String PID = "fixture.custom.sparkplug";
    private static final String DEVICE = "fixture.sparkplug.device";
    private static final String MANAGER = "org.eclipse.kura.cloudconnection.CloudConnectionManager";
    private static final String PUBLISHER = "org.eclipse.kura.cloudconnection.publisher.CloudPublisher";
    private static final String USER = "sparkplug-user";
    private static final String PASSWORD = "isolated-sparkplug-password";
    private static final String CLIENT = "sparkplug-factory-client";
    private static final String PREFIX = "spBv1.0/fixture-group/";

    static void run(EquinoxRuntime runtime, Service configuration) throws Exception {
        try (var factory = runtime.service("org.eclipse.kura.cloudconnection.factory.CloudConnectionFactory",
                "(service.pid=" + BASE + "factory.SparkplugCloudConnectionFactory)", Duration.ofSeconds(10))) {
            assertEquals(BASE + "endpoint.SparkplugCloudEndpoint", factory.call("getFactoryPid"));
            factory.call("createConfiguration", PID, "测试 Sparkplug", "自定义服务标识与中文描述");
            List<?> stack = (List<?>) factory.call("getStackComponentsPids", PID);
            assertEquals(2, stack.size(), "Local Sparkplug contract returns the two child services");
            try (var endpoint = runtime.service(MANAGER, filter(PID), Duration.ofSeconds(10));
                 var data = runtime.service("org.eclipse.kura.data.DataService", filter(stack.get(0)), Duration.ofSeconds(10));
                 var transport = runtime.service("org.eclipse.kura.data.DataTransportService", filter(stack.get(1)), Duration.ofSeconds(10))) {
                assertEquals(PID, endpoint.property("kura.service.pid"));
                assertEquals(BASE + "endpoint.SparkplugCloudEndpoint", endpoint.property("service.factoryPid"));
                assertEquals("测试 Sparkplug", factory.call("getCloudName", PID));
                assertEquals("测试 Sparkplug", endpoint.property("kura.cloud.factory.name"));
                assertEquals("自定义服务标识与中文描述", endpoint.property("kura.cloud.factory.desc"));
                assertEquals(Set.of(PID), factory.call("getManagedCloudConnectionPids"));
                assertEquals(filter(stack.get(0)), endpoint.property("DataService.target"));
                assertEquals(filter(stack.get(1)), data.property("DataTransportService.target"));
                assertEquals("org.eclipse.kura.cloud.base.provider", data.provider().getSymbolicName());
                assertEquals(BUNDLE, transport.provider().getSymbolicName());
                assertEquals("org.eclipse.osgi", runtime.packageProvider(BUNDLE, "javax.net.ssl"));
                configuration.call("createFactoryConfiguration", BASE + "device.SparkplugDevice", DEVICE,
                        Map.of("cloud.endpoint.service.pid", PID, "device.id", "fixture-device"), true);
                try (var device = runtime.service(PUBLISHER, filter(DEVICE), Duration.ofSeconds(10))) {
                    verifyPipeline(configuration, endpoint, transport, device, stack.get(1).toString());
                }
                configuration.call("deleteFactoryConfiguration", DEVICE, true);
                await(() -> !runtime.hasService(PUBLISHER, filter(DEVICE)), "Device remained registered");
            }
            factory.call("deleteConfiguration", PID);
            await(() -> !runtime.hasService(MANAGER, filter(PID)), "Endpoint remained registered");
            for (Object child : stack) {
                await(() -> !runtime.hasService("org.eclipse.kura.configuration.ConfigurableComponent", filter(child)),
                        "Child remained registered: " + child);
            }
            assertEquals(Set.of(), factory.call("getManagedCloudConnectionPids"));
        }
    }

    private static void verifyPipeline(Service configuration, Service endpoint, Service transport, Service device,
            String transportPid) throws Exception {
        Server broker = new Server();
        AtomicBoolean authenticated = new AtomicBoolean();
        Properties options = new Properties();
        options.setProperty("host", "127.0.0.1");
        options.setProperty("port", "0");
        options.setProperty("allow_anonymous", "false");
        options.setProperty("persistence_enabled", "false");
        options.setProperty("telemetry_enabled", "false");
        Map<String, LinkedBlockingQueue<MqttMessage>> messages = new ConcurrentHashMap<>();
        try {
            broker.startServer(new MemoryConfig(options), List.of(), null, (client, user, password) -> {
                boolean accepted = USER.equals(user) && Arrays.equals(PASSWORD.getBytes(StandardCharsets.UTF_8), password);
                if (accepted && CLIENT.equals(client)) { authenticated.set(true); }
                return accepted;
            }, null);
            await(() -> {
                try { return broker.getPort() > 0; }
                catch (ConcurrentModificationException bindingInProgress) { return false; }
            }, "Broker did not bind a loopback port");
            String uri = "tcp://127.0.0.1:" + broker.getPort();
            try (MqttClient observer = new MqttClient(uri, "sparkplug-observer", new MemoryPersistence())) {
                observer.setCallback(new MqttCallback() {
                    public void connectionLost(Throwable cause) { }
                    public void deliveryComplete(IMqttDeliveryToken token) { }
                    public void messageArrived(String topic, MqttMessage message) {
                        messages.computeIfAbsent(topic, key -> new LinkedBlockingQueue<>()).add(message);
                    }
                });
                MqttConnectOptions connection = new MqttConnectOptions();
                connection.setUserName(USER);
                connection.setPassword(PASSWORD.toCharArray());
                observer.connect(connection);
                observer.subscribe("spBv1.0/#", 1);
                Class<?> passwordType = endpoint.provider().loadClass("org.eclipse.kura.configuration.Password");
                Object password = passwordType.getConstructor(char[].class).newInstance((Object) PASSWORD.toCharArray());
                update(configuration, transportPid, Map.of("server.uris", uri, "client.id", CLIENT,
                        "group.id", "fixture-group", "node.id", "fixture-node", "username", USER,
                        "password", password, "connection.timeout", 3));
                await(() -> CLIENT.equals(transport.call("getClientId")), "Transport configuration did not arrive");
                LinkedBlockingQueue<String> transitions = new LinkedBlockingQueue<>();
                Class<?> listenerType = endpoint.provider().loadClass("org.eclipse.kura.cloudconnection.listener.CloudConnectionListener");
                Object listener = Proxy.newProxyInstance(listenerType.getClassLoader(), new Class<?>[] { listenerType },
                        (proxy, method, args) -> switch (method.getName()) {
                            case "onConnectionEstablished", "onDisconnected", "onConnectionLost" -> {
                                transitions.add(method.getName()); yield null;
                            }
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            case "toString" -> "Sparkplug connection observer";
                            default -> throw new AssertionError(method);
                        });
                device.call("registerCloudConnectionListener", listener);
                try {
                    endpoint.call("connect");
                    assertEquals("onConnectionEstablished", transitions.poll(10, TimeUnit.SECONDS));
                    assertTrue(authenticated.get(), "Actual Sparkplug client was not authenticated");
                    verifyPayload(endpoint, take(messages, "NBIRTH/fixture-node"), 0, false, false);
                    publish(endpoint, device, false);
                    verifyPayload(endpoint, take(messages, "DBIRTH/fixture-node/fixture-device"), 1, true, false);
                    publish(endpoint, device, false);
                    verifyPayload(endpoint, take(messages, "DDATA/fixture-node/fixture-device"), 2, true, false);
                    publish(endpoint, device, true);
                    verifyPayload(endpoint, take(messages, "DBIRTH/fixture-node/fixture-device"), 3, true, true);
                    endpoint.call("disconnect");
                    assertEquals("onDisconnected", transitions.poll(10, TimeUnit.SECONDS));
                    endpoint.call("connect");
                    assertEquals("onConnectionEstablished", transitions.poll(10, TimeUnit.SECONDS));
                    verifyPayload(endpoint, take(messages, "NBIRTH/fixture-node"), 0, false, false);
                    publish(endpoint, device, true);
                    verifyPayload(endpoint, take(messages, "DBIRTH/fixture-node/fixture-device"), 1, true, true);
                    assertEquals("测试 Sparkplug", endpoint.property("kura.cloud.factory.name"));
                    assertEquals("自定义服务标识与中文描述", endpoint.property("kura.cloud.factory.desc"));
                } finally {
                    device.call("unregisterCloudConnectionListener", listener);
                    if ((Boolean) endpoint.call("isConnected")) { endpoint.call("disconnect"); }
                    observer.disconnect();
                }
            }
        } finally {
            broker.stopServer();
        }
    }

    private static void publish(Service endpoint, Service device, boolean changed) throws Exception {
        Class<?> payloadType = endpoint.provider().loadClass("org.eclipse.kura.message.KuraPayload");
        Object payload = payloadType.getConstructor().newInstance();
        EquinoxRuntime.invoke(payloadType, payload, "setTimestamp", new Date(1700000000000L));
        EquinoxRuntime.invoke(payloadType, payload, "setBody", "真实 Sparkplug 数据".getBytes(StandardCharsets.UTF_8));
        EquinoxRuntime.invoke(payloadType, payload, "addMetric", "metric.string", "中文数据");
        if (changed) { EquinoxRuntime.invoke(payloadType, payload, "addMetric", "metric.int", 12); }
        Class<?> messageType = endpoint.provider().loadClass("org.eclipse.kura.cloudconnection.message.KuraMessage");
        assertNull(device.call("publish", messageType.getConstructor(payloadType).newInstance(payload)),
                "Sparkplug QoS 0 publication has no delivery ID");
    }

    private static void verifyPayload(Service endpoint, MqttMessage message, long sequence, boolean device,
            boolean changed) throws Exception {
        assertEquals(0, message.getQos());
        assertFalse(message.isRetained());
        Class<?> type = endpoint.provider().loadClass("org.eclipse.tahu.protobuf.SparkplugBProto$Payload");
        Object payload = type.getMethod("parseFrom", byte[].class).invoke(null, message.getPayload());
        assertEquals(sequence, EquinoxRuntime.invoke(type, payload, "getSeq"));
        if (!device) { return; }
        assertEquals(1700000000000L, EquinoxRuntime.invoke(type, payload, "getTimestamp"));
        Object body = EquinoxRuntime.invoke(type, payload, "getBody");
        assertArrayEquals("真实 Sparkplug 数据".getBytes(StandardCharsets.UTF_8),
                (byte[]) EquinoxRuntime.invoke(endpoint.provider().loadClass("com.google.protobuf.ByteString"), body, "toByteArray"));
        List<?> metrics = (List<?>) EquinoxRuntime.invoke(type, payload, "getMetricsList");
        assertEquals(changed ? 2 : 1, metrics.size());
        Class<?> metricType = endpoint.provider().loadClass("org.eclipse.tahu.protobuf.SparkplugBProto$Payload$Metric");
        for (Object metric : metrics) {
            String name = (String) EquinoxRuntime.invoke(metricType, metric, "getName");
            if ("metric.string".equals(name)) {
                assertEquals("中文数据", EquinoxRuntime.invoke(metricType, metric, "getStringValue"));
            } else {
                assertEquals("metric.int", name);
                assertEquals(12, EquinoxRuntime.invoke(metricType, metric, "getIntValue"));
            }
        }
    }

    private static MqttMessage take(Map<String, LinkedBlockingQueue<MqttMessage>> messages, String suffix) throws Exception {
        MqttMessage message = messages.computeIfAbsent(PREFIX + suffix, key -> new LinkedBlockingQueue<>())
                .poll(10, TimeUnit.SECONDS);
        assertNotNull(message, "Missing Sparkplug publication: " + suffix);
        return message;
    }

    @SuppressWarnings("unchecked")
    private static void update(Service configuration, String pid, Map<String, Object> changes) throws Exception {
        Object current = configuration.call("getComponentConfiguration", pid);
        Class<?> type = configuration.provider().loadClass("org.eclipse.kura.configuration.ComponentConfiguration");
        Map<String, Object> properties = new HashMap<>((Map<String, Object>)
                EquinoxRuntime.invoke(type, current, "getConfigurationProperties"));
        properties.putAll(changes);
        configuration.call("updateConfiguration", pid, properties, true);
    }

    private static String filter(Object pid) { return "(kura.service.pid=" + pid + ")"; }

    private static void await(Callable<Boolean> condition, String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.call() && System.nanoTime() < deadline) { Thread.sleep(20); }
        assertTrue(condition.call(), message);
    }
}
