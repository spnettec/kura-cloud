/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.eclipse.kura.testing.osgi.EquinoxRuntime;
import org.eclipse.kura.testing.osgi.EquinoxRuntime.Service;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.osgi.framework.Bundle;

/** A real broker connects SCR-bound core handlers to the Kapua CloudService. */
final class CoreProtocolMqttScenario {
    private static final String ACCOUNT = "factory-account";
    private static final String OBSERVER = "core-protocol-observer";
    private static final String JSON_PID = "org.eclipse.kura.json.marshaller.unmarshaller.provider";

    private CoreProtocolMqttScenario() { }

    static void run(EquinoxRuntime runtime, Service cloud, String brokerUri, String user, String password,
            String clientId) throws Exception {
        assertEquals(Bundle.ACTIVE, runtime.bundle("org.eclipse.kura.core.configuration").getState());
        try (var inventory = runtime.service("org.eclipse.kura.core.inventory.InventoryHandlerV1", null,
                     Duration.ofSeconds(10));
             var marshaller = runtime.service("org.eclipse.kura.marshalling.Marshaller",
                     "(kura.service.pid=" + JSON_PID + ")", Duration.ofSeconds(5));
             var unmarshaller = runtime.service("org.eclipse.kura.marshalling.Unmarshaller",
                     "(kura.service.pid=" + JSON_PID + ")", Duration.ofSeconds(5));
             MqttClient observer = new MqttClient(brokerUri, OBSERVER, new MemoryPersistence())) {
            assertEquals("org.eclipse.kura.core.inventory", inventory.provider().getSymbolicName());
            LinkedBlockingQueue<MqttMessage> replies = new LinkedBlockingQueue<>();
            observer.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) { }
                public void deliveryComplete(IMqttDeliveryToken token) { }
                public void messageArrived(String topic, MqttMessage message) { replies.add(message); }
            });
            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName(user);
            options.setPassword(password.toCharArray());
            options.setConnectionTimeout(3);
            observer.connect(options);
            try {
                assertTrue((Boolean) cloud.call("isConnected"));
                Class<?> payloadType = cloud.provider().loadClass("org.eclipse.kura.message.KuraPayload");
                String configuration = request(observer, replies, marshaller, unmarshaller, payloadType,
                        clientId, "CONF-V1", "configurations/fixture.custom.connection");
                assertTrue(configuration.contains("fixture.custom.connection"),
                        "The real configuration handler must return its SCR factory configuration");
                String packages = request(observer, replies, marshaller, unmarshaller, payloadType,
                        clientId, "INVENTORY-V1", "systemPackages");
                assertTrue(packages.contains("\"systemPackages\""),
                        "The real inventory handler must return its system package response");
                String bundles = request(observer, replies, marshaller, unmarshaller, payloadType,
                        clientId, "INVENTORY-V1", "bundles");
                assertTrue(bundles.contains("org.eclipse.kura.core.inventory"),
                        "The real inventory handler must enumerate its installed Equinox bundle");
            } finally {
                observer.disconnect();
            }
        }
    }

    private static String request(MqttClient observer, LinkedBlockingQueue<MqttMessage> replies,
            Service marshaller, Service unmarshaller, Class<?> payloadType, String clientId,
            String appId, String resource) throws Exception {
        String id = UUID.randomUUID().toString();
        String replyTopic = "EDC/" + ACCOUNT + "/" + OBSERVER + "/" + appId + "/REPLY/" + id;
        observer.subscribe(replyTopic, 1);
        try {
            Object payload = payloadType.getConstructor().newInstance();
            EquinoxRuntime.invoke(payloadType, payload, "addMetric", "request.id", id);
            EquinoxRuntime.invoke(payloadType, payload, "addMetric", "requester.client.id", OBSERVER);
            String json = (String) marshaller.call("marshal", payload);
            String topic = "EDC/" + ACCOUNT + "/" + clientId + "/" + appId + "/GET/" + resource;
            observer.publish(topic, json.getBytes(StandardCharsets.UTF_8), 1, false);
            MqttMessage reply = replies.poll(10, TimeUnit.SECONDS);
            assertNotNull(reply, "No MQTT reply from SCR handler " + appId);
            Object decoded = unmarshaller.call("unmarshal",
                    new String(reply.getPayload(), StandardCharsets.UTF_8), payloadType);
            assertEquals(200, ((Number) EquinoxRuntime.invoke(payloadType, decoded,
                    "getMetric", "response.code")).intValue(), appId + " rejected the MQTT request");
            byte[] body = (byte[]) EquinoxRuntime.invoke(payloadType, decoded, "getBody");
            assertNotNull(body, appId + " returned no response body");
            return new String(body, StandardCharsets.UTF_8);
        } finally {
            observer.unsubscribe(replyTopic);
        }
    }
}
