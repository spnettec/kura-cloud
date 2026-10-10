/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.kura.testing.osgi.EquinoxRuntime;
import org.eclipse.kura.testing.osgi.EquinoxRuntime.Service;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/** A simulated sensor registered through OSGi; SCR, EventAdmin and the birth scheduler are real. */
final class CloudTamperScenario implements AutoCloseable {
    private static final String PID = "fixture.tamper.sensor";
    private static final String TOPIC = "org/eclipse/kura/security/tamper/detection/TamperEvent/TAMPER_STATUS_CHANGED";
    private final EquinoxRuntime runtime;
    private final Service cloud;
    private final Class<?> statusType;
    private final AtomicBoolean tampered = new AtomicBoolean();
    private final AtomicInteger statusReads = new AtomicInteger();
    private final AutoCloseable registration;

    CloudTamperScenario(EquinoxRuntime runtime, Service cloud) throws Exception {
        this.runtime = runtime;
        this.cloud = cloud;
        this.statusType = cloud.provider().loadClass("org.eclipse.kura.security.tamper.detection.TamperStatus");
        this.registration = runtime.register("org.eclipse.kura.api",
                "org.eclipse.kura.security.tamper.detection.TamperDetectionService", (proxy, method, args) ->
                        switch (method.getName()) {
                            case "getDisplayName" -> "隔离测试传感器";
                            case "getTamperStatus" -> { statusReads.incrementAndGet(); yield status(); }
                            case "resetTamperStatus" -> { tampered.set(false); yield null; }
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            case "toString" -> PID;
                            default -> throw new AssertionError(method);
                        }, Map.of("kura.service.pid", PID));
    }

    void verifyBirthRepublishing(LinkedBlockingQueue<MqttMessage> births) throws Exception {
        assertEquals(Boolean.TRUE, cloud.property("republish.mqtt.birth.cert.on.tamper.event"));
        assertEquals("NOT_TAMPERED", tamperMetric(take(births, 10)));
        int initialReads = statusReads.get();
        assertTrue(initialReads > 0, "SCR did not bind the registered sensor to CloudService");
        assertTrue(births.isEmpty(), "Unexpected extra initial birth message");
        tampered.set(true);
        try (var handler = runtime.service("org.osgi.service.event.EventHandler", "(event.topics=" + TOPIC + ")",
                     Duration.ofSeconds(5));
             var events = runtime.service("org.osgi.service.event.EventAdmin", null, Duration.ofSeconds(5))) {
            assertEquals(cloud.provider(), handler.provider());
            Class<?> eventType = cloud.provider().loadClass("org.eclipse.kura.security.tamper.detection.TamperEvent");
            Object event = eventType.getConstructor(String.class, statusType).newInstance(PID, status());
            events.call("sendEvent", event);
            // Keep the production 30-second delay; do not replace its executor or invoke private methods.
            assertNull(births.poll(500, TimeUnit.MILLISECONDS), "Tamper event bypassed delayed birth publication");
            assertEquals("TAMPERED", tamperMetric(take(births, 40)));
            assertTrue(statusReads.get() > initialReads, "Republished birth did not read the current sensor state");
        }
    }

    private Object status() throws Exception {
        return statusType.getConstructor(boolean.class, Map.class).newInstance(tampered.get(), Map.of());
    }

    private String tamperMetric(MqttMessage message) throws Exception {
        assertEquals(1, message.getQos());
        Class<?> payloadType = cloud.provider().loadClass("org.eclipse.kura.message.KuraPayload");
        try (var decoder = runtime.service("org.eclipse.kura.marshalling.Unmarshaller",
                "(kura.service.pid=org.eclipse.kura.json.marshaller.unmarshaller.provider)", Duration.ofSeconds(5))) {
            Object payload = decoder.call("unmarshal", new String(message.getPayload(), StandardCharsets.UTF_8), payloadType);
            return (String) EquinoxRuntime.invoke(payloadType, payload, "getMetric", "tamper_status");
        }
    }

    private static MqttMessage take(LinkedBlockingQueue<MqttMessage> births, int timeout) throws InterruptedException {
        MqttMessage result = births.poll(timeout, TimeUnit.SECONDS);
        assertNotNull(result, "Expected birth publication was not delivered by MQTT");
        return result;
    }

    @Override
    public void close() throws Exception {
        registration.close();
        assertFalse(runtime.hasService("org.eclipse.kura.security.tamper.detection.TamperDetectionService",
                "(kura.service.pid=" + PID + ")"));
    }
}
