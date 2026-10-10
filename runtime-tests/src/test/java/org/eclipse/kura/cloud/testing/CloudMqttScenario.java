/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import org.eclipse.kura.testing.osgi.EquinoxRuntime;
import org.eclipse.kura.testing.osgi.EquinoxRuntime.Service;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/** Real broker on the controller side, business services only in their OSGi bundles. */
final class CloudMqttScenario {
    private static final String USER = "factory-user";
    private static final String PASSWORD = "isolated-test-password";
    private static final String CLIENT = "factory-client";
    private static final String TOPIC = "factory-account/" + CLIENT + "/fixture-app/factory-data";
    private static final String BIRTH_TOPIC = "EDC/factory-account/" + CLIENT + "/MQTT/BIRTH";

    static void run(EquinoxRuntime runtime, Service configuration, Service cloud, Service publisher,
            List<?> stack, String encoding, boolean withTamper, boolean withCoreHandlers) throws Exception {
        Server broker = new Server();
        AtomicBoolean authenticatedCloud = new AtomicBoolean();
        Properties options = new Properties();
        options.setProperty("host", "127.0.0.1");
        options.setProperty("port", "0");
        options.setProperty("allow_anonymous", "false");
        options.setProperty("persistence_enabled", "false");
        options.setProperty("telemetry_enabled", "false");
        if (withCoreHandlers) {
            // The JSON bundle inventory exceeds Moquette's 8092-byte default after MQTT payload encoding.
            options.setProperty("netty.mqtt.message_size", "131072");
        }
        try {
            broker.startServer(new MemoryConfig(options), List.of(), null, (clientId, username, password) -> {
                boolean accepted = USER.equals(username)
                        && Arrays.equals(PASSWORD.getBytes(StandardCharsets.UTF_8), password);
                if (accepted && CLIENT.equals(clientId)) { authenticatedCloud.set(true); }
                return accepted;
            }, null);
            await(() -> {
                try { return broker.getPort() > 0; }
                catch (ConcurrentModificationException bindingInProgress) { return false; }
            }, "Broker did not bind a loopback port");
            String uri = "tcp://127.0.0.1:" + broker.getPort();
            LinkedBlockingQueue<MqttMessage> received = new LinkedBlockingQueue<>();
            LinkedBlockingQueue<MqttMessage> births = new LinkedBlockingQueue<>();
            try (MqttClient observer = new MqttClient(uri, "factory-observer", new MemoryPersistence());
                 var tamper = withTamper ? new CloudTamperScenario(runtime, cloud) : null;
                 var manager = runtime.service("org.eclipse.kura.cloudconnection.CloudConnectionManager",
                         "(kura.service.pid=" + stack.get(0) + ")", Duration.ofSeconds(5));
                 var transport = runtime.service("org.eclipse.kura.data.DataTransportService",
                         "(kura.service.pid=" + stack.get(2) + ")", Duration.ofSeconds(5));
                 var data = runtime.service("org.eclipse.kura.data.DataService",
                         "(kura.service.pid=" + stack.get(1) + ")", Duration.ofSeconds(5))) {
                observer.setCallback(new MqttCallback() {
                    public void connectionLost(Throwable cause) { }
                    public void deliveryComplete(IMqttDeliveryToken token) { }
                    public void messageArrived(String topic, MqttMessage message) {
                        if (TOPIC.equals(topic)) { received.add(message); }
                        if (BIRTH_TOPIC.equals(topic)) { births.add(message); }
                    }
                });
                MqttConnectOptions connection = new MqttConnectOptions();
                connection.setUserName(USER);
                connection.setPassword(PASSWORD.toCharArray());
                connection.setConnectionTimeout(3);
                observer.connect(connection);
                observer.subscribe(TOPIC, 1);
                if (withTamper) { observer.subscribe(BIRTH_TOPIC, 1); }
                try {
                    Class<?> passwordType = cloud.provider().loadClass("org.eclipse.kura.configuration.Password");
                    Object password = passwordType.getConstructor(char[].class).newInstance((Object) PASSWORD.toCharArray());
                    update(configuration, stack.get(2).toString(), Map.of("broker-url", uri, "username", USER,
                            "password", password, "client-id", CLIENT, "topic.context.account-name", "factory-account",
                            "timeout", 3, "lwt.topic", "EDC/factory-account/" + CLIENT + "/MQTT/LWT"));
                    update(configuration, stack.get(0).toString(), Map.of("topic.control-prefix", "EDC",
                            "payload.encoding", encoding, "encode.gzip", false));
                    update(configuration, "fixture.publisher", Map.of("appId", "fixture-app", "app.topic", "factory-data",
                            "qos", 1, "retain", false));
                    await(() -> uri.equals(transport.call("getBrokerUrl")) && CLIENT.equals(transport.call("getClientId"))
                            && encoding.equals(cloud.property("payload.encoding"))
                            && Integer.valueOf(1).equals(publisher.property("qos")), "Configuration updates did not arrive");
                    verifyStoredCredential(runtime, stack.get(2).toString());

                    assertFalse((Boolean) cloud.call("isConnected"));
                    Class<?> payloadType = cloud.provider().loadClass("org.eclipse.kura.message.KuraPayload");
                    Object payload = payloadType.getConstructor().newInstance();
                    byte[] body = "工厂离线队列 → MQTT".getBytes(StandardCharsets.UTF_8);
                    EquinoxRuntime.invoke(payloadType, payload, "setBody", body);
                    EquinoxRuntime.invoke(payloadType, payload, "addMetric", "temperature", 23);
                    Class<?> messageType = cloud.provider().loadClass("org.eclipse.kura.cloudconnection.message.KuraMessage");
                    Object message = messageType.getConstructor(payloadType, Map.class).newInstance(payload, Map.of());
                    LinkedBlockingQueue<String> confirmations = new LinkedBlockingQueue<>();
                    Class<?> listenerType = cloud.provider().loadClass("org.eclipse.kura.cloudconnection.listener.CloudDeliveryListener");
                    Object listener = Proxy.newProxyInstance(listenerType.getClassLoader(), new Class<?>[] {listenerType},
                            (proxy, method, args) -> switch (method.getName()) {
                                case "onMessageConfirmed" -> { confirmations.add((String) args[0]); yield null; }
                                case "hashCode" -> System.identityHashCode(proxy);
                                case "equals" -> proxy == args[0];
                                case "toString" -> "MQTT confirmation observer";
                                default -> throw new AssertionError(method);
                            });
                    publisher.call("registerCloudDeliveryListener", listener);
                    String id = (String) publisher.call("publish", message);
                    assertNotNull(id);
                    assertTrue(((List<?>) data.call("getUnpublishedMessageIds", ".*fixture-app/factory-data"))
                            .contains(Integer.valueOf(id)), "Offline publication did not enter the real message store");
                    assertTrue(received.isEmpty());

                    manager.call("connect");
                    await(() -> (Boolean) cloud.call("isConnected"), "Cloud did not connect");
                    assertTrue(authenticatedCloud.get(), "Broker did not authenticate the actual cloud transport");
                    MqttMessage delivered = received.poll(10, TimeUnit.SECONDS);
                    assertNotNull(delivered, "Stored publication was not delivered by MQTT");
                    assertEquals(1, delivered.getQos());
                    assertFalse(delivered.isRetained());
                    Object decoded;
                    if ("kura-protobuf".equals(encoding)) {
                        try (var decoder = runtime.service("org.eclipse.kura.cloud.CloudPayloadProtoBufDecoder",
                                "(kura.service.pid=" + stack.get(0) + ")", Duration.ofSeconds(5))) {
                            decoded = decoder.call("buildFromByteArray", delivered.getPayload());
                        }
                    } else {
                        try (var decoder = runtime.service("org.eclipse.kura.marshalling.Unmarshaller",
                                "(kura.service.pid=org.eclipse.kura.json.marshaller.unmarshaller.provider)", Duration.ofSeconds(5))) {
                            decoded = decoder.call("unmarshal", new String(delivered.getPayload(), StandardCharsets.UTF_8), payloadType);
                        }
                    }
                    assertArrayEquals(body, (byte[]) EquinoxRuntime.invoke(payloadType, decoded, "getBody"));
                    assertEquals(23, ((Number) EquinoxRuntime.invoke(payloadType, decoded, "getMetric", "temperature")).intValue());
                    assertEquals(id, confirmations.poll(10, TimeUnit.SECONDS), "Publisher did not confirm the stored message ID");
                    publisher.call("unregisterCloudDeliveryListener", listener);
                    await(() -> ((List<?>) data.call("getUnpublishedMessageIds", ".*fixture-app/factory-data")).isEmpty(),
                            "Delivered publication remained unpublished");
                    assertEquals("测试云连接", cloud.property("kura.cloud.factory.name"));
                    assertEquals("保留本地名称与描述", cloud.property("kura.cloud.factory.desc"));
                    if (tamper != null) { tamper.verifyBirthRepublishing(births); }
                    verifyClientRoundTrip(cloud, payloadType);
                    if (withCoreHandlers) {
                        CoreProtocolMqttScenario.run(runtime, cloud, uri, USER, PASSWORD, CLIENT);
                    }
                } finally {
                    try {
                        if ((Boolean) cloud.call("isConnected")) { manager.call("disconnect"); }
                    } finally {
                        if (observer.isConnected()) { observer.disconnect(); }
                    }
                }
            }
        } finally {
            broker.stopServer();
        }
    }

    private static void verifyStoredCredential(EquinoxRuntime runtime, String transportPid) throws Exception {
        try (var admin = runtime.service("org.osgi.service.cm.ConfigurationAdmin", null, Duration.ofSeconds(5));
             var crypto = runtime.service("org.eclipse.kura.crypto.CryptoService", null, Duration.ofSeconds(5))) {
            Object[] configs = (Object[]) admin.call("listConfigurations", "(kura.service.pid=" + transportPid + ")");
            assertNotNull(configs);
            assertEquals(1, configs.length);
            Dictionary<?, ?> properties = (Dictionary<?, ?>) EquinoxRuntime.invoke(
                    admin.provider().loadClass("org.osgi.service.cm.Configuration"), configs[0], "getProperties");
            String encrypted = assertInstanceOf(String.class, properties.get("password"));
            assertNotEquals(PASSWORD, encrypted, "ConfigurationAdmin must not persist the clear-text password");
            assertArrayEquals(PASSWORD.toCharArray(), (char[]) crypto.call("decryptAes", encrypted.toCharArray()));
        }
    }

    private record Delivery(int id, String topic) { }
    private record Arrival(boolean control, String device, String topic, Object payload, int qos, boolean retained) { }

    private static void verifyClientRoundTrip(Service cloud, Class<?> payloadType) throws Exception {
        LinkedBlockingQueue<Delivery> published = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Delivery> confirmed = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Arrival> arrivals = new LinkedBlockingQueue<>();
        Class<?> listenerType = cloud.provider().loadClass("org.eclipse.kura.cloud.CloudClientListener");
        Object listener = Proxy.newProxyInstance(listenerType.getClassLoader(), new Class<?>[] {listenerType},
                (proxy, method, args) -> switch (method.getName()) {
                    case "onMessagePublished" -> { published.add(new Delivery((Integer) args[0], (String) args[1])); yield null; }
                    case "onMessageConfirmed" -> { confirmed.add(new Delivery((Integer) args[0], (String) args[1])); yield null; }
                    case "onMessageArrived", "onControlMessageArrived" -> {
                        arrivals.add(new Arrival("onControlMessageArrived".equals(method.getName()),
                                (String) args[0], (String) args[1], args[2], (Integer) args[3], (Boolean) args[4]));
                        yield null;
                    }
                    case "onConnectionLost", "onConnectionEstablished" -> null;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "CloudClient callback observer";
                    default -> throw new AssertionError(method);
                });
        Object client = cloud.call("newCloudClient", "fixture-roundtrip");
        Class<?> clientType = cloud.provider().loadClass("org.eclipse.kura.cloud.CloudClient");
        EquinoxRuntime.invoke(clientType, client, "addCloudClientListener", listener);
        try {
            EquinoxRuntime.invoke(clientType, client, "subscribe", "normal", 1);
            EquinoxRuntime.invoke(clientType, client, "controlSubscribe", "control", 1);
            Object normal = payload(payloadType, "数据往返", 12);
            Object control = payload(payloadType, "控制往返", 34);
            int normalId = (Integer) EquinoxRuntime.invoke(clientType, client, "publish", "normal", normal, 1, false, 5);
            int controlId = (Integer) EquinoxRuntime.invoke(clientType, client, "controlPublish", "control", control, 1, false, 5);
            Set<Delivery> expected = Set.of(new Delivery(normalId, "normal"), new Delivery(controlId, "control"));
            assertEquals(expected, Set.of(take(published), take(published)));
            assertEquals(expected, Set.of(take(confirmed), take(confirmed)));
            Set<Boolean> routes = new HashSet<>();
            for (int i = 0; i < 2; i++) {
                Arrival arrival = take(arrivals);
                assertTrue(routes.add(arrival.control()));
                assertEquals(CLIENT, arrival.device());
                assertEquals(arrival.control() ? "control" : "normal", arrival.topic());
                assertEquals(1, arrival.qos());
                assertFalse(arrival.retained());
                assertEquals(new Date(1700000000000L), EquinoxRuntime.invoke(payloadType, arrival.payload(), "getTimestamp"));
                assertEquals(arrival.control() ? 34 : 12,
                        ((Number) EquinoxRuntime.invoke(payloadType, arrival.payload(), "getMetric", "value")).intValue());
                assertArrayEquals((arrival.control() ? "控制往返" : "数据往返").getBytes(StandardCharsets.UTF_8),
                        (byte[]) EquinoxRuntime.invoke(payloadType, arrival.payload(), "getBody"));
            }
            EquinoxRuntime.invoke(clientType, client, "unsubscribe", "normal");
            EquinoxRuntime.invoke(clientType, client, "controlUnsubscribe", "control");
        } finally {
            try { EquinoxRuntime.invoke(clientType, client, "removeCloudClientListener", listener); }
            finally { EquinoxRuntime.invoke(clientType, client, "release"); }
        }
    }

    private static Object payload(Class<?> type, String body, int value) throws Exception {
        Object payload = type.getConstructor().newInstance();
        EquinoxRuntime.invoke(type, payload, "setBody", body.getBytes(StandardCharsets.UTF_8));
        EquinoxRuntime.invoke(type, payload, "setTimestamp", new Date(1700000000000L));
        EquinoxRuntime.invoke(type, payload, "addMetric", "value", value);
        return payload;
    }

    private static <T> T take(LinkedBlockingQueue<T> queue) throws InterruptedException {
        T event = queue.poll(10, TimeUnit.SECONDS);
        assertNotNull(event, "Timed out waiting for a CloudClient callback");
        return event;
    }

    @SuppressWarnings("unchecked")
    private static void update(Service configuration, String pid, Map<String, Object> changes) throws Exception {
        Object current = configuration.call("getComponentConfiguration", pid);
        Class<?> contract = configuration.provider().loadClass("org.eclipse.kura.configuration.ComponentConfiguration");
        Map<String, Object> properties = new HashMap<>((Map<String, Object>)
                EquinoxRuntime.invoke(contract, current, "getConfigurationProperties"));
        properties.putAll(changes);
        configuration.call("updateConfiguration", pid, properties, true);
    }

    private static void await(Callable<Boolean> condition, String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.call() && System.nanoTime() < deadline) { Thread.sleep(20); }
        assertTrue(condition.call(), message);
    }
}
