/*******************************************************************************
 * Copyright (c) 2017, 2026 Eurotech and/or its affiliates and others
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *  Eurotech
 ******************************************************************************/
package org.eclipse.kura.core.cloud;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.eclipse.kura.cloud.CloudClient;
import org.eclipse.kura.cloud.CloudClientListener;
import org.eclipse.kura.core.data.BaseCloudTests;
import org.eclipse.kura.core.testutil.TestUtil;
import org.eclipse.kura.message.KuraPayload;
import org.eclipse.kura.system.SystemAdminService;
import org.eclipse.kura.system.SystemService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.event.EventAdmin;

/** Adapted from core/test/CloudServiceTest: real SQL, MQTT and Protobuf, controlled OSGi boundaries. */
@Timeout(60)
class CloudServiceMqttTest extends BaseCloudTests {

    private CloudServiceImpl cloud;
    private ComponentContext cloudContext;
    private ServiceRegistration<?> registration;
    private ExecutorService birthPublisher;
    private record Delivery(int id, String topic) { }
    private record Arrival(boolean control, String device, String topic, KuraPayload payload, int qos, boolean retain) { }

    @BeforeEach
    void startCloudService() throws Exception {
        this.cloudContext = mock(ComponentContext.class);
        BundleContext registry = mock(BundleContext.class);
        when(this.cloudContext.getBundleContext()).thenReturn(registry);
        Dictionary<String, Object> properties = new Hashtable<>();
        properties.put("kura.service.pid", "test.cloud");
        when(this.cloudContext.getProperties()).thenReturn(properties);
        this.registration = mock(ServiceRegistration.class);
        when(registry.registerService(anyString(), any(), any(Dictionary.class))).thenReturn(this.registration);
        this.cloud = new CloudServiceImpl();
        this.birthPublisher = (ExecutorService) TestUtil.getFieldValue(this.cloud, "scheduledBirthPublisher");
        this.cloud.setDataService(this.dataService);
        this.cloud.setSystemService(mock(SystemService.class));
        this.cloud.setSystemAdminService(mock(SystemAdminService.class));
        this.cloud.setEventAdmin(mock(EventAdmin.class));
        // Moquette 0.18 rejects client publications beginning with '$'. Exercise Kura's configurable prefix.
        this.cloud.activate(this.cloudContext, Map.of("kura.service.pid", "test.cloud", "payload.encoding", "kura-protobuf",
                "topic.control-prefix", "EDC"));
    }

    @AfterEach
    void stopCloudService() throws Exception {
        if (this.cloud != null) {
            this.cloud.deactivate(this.cloudContext);
            assertTrue(this.birthPublisher.awaitTermination(5, TimeUnit.SECONDS));
            verify(this.registration, org.mockito.Mockito.times(2)).unregister();
        }
    }

    @Test
    void testService() throws Exception {
        BlockingQueue<Delivery> published = new LinkedBlockingQueue<>();
        BlockingQueue<Delivery> confirmed = new LinkedBlockingQueue<>();
        BlockingQueue<Arrival> arrivals = new LinkedBlockingQueue<>();
        CloudClientListener listener = mock(CloudClientListener.class);
        doAnswer(call -> published.add(new Delivery(call.getArgument(0), call.getArgument(1))))
                .when(listener).onMessagePublished(anyInt(), anyString());
        doAnswer(call -> confirmed.add(new Delivery(call.getArgument(0), call.getArgument(1))))
                .when(listener).onMessageConfirmed(anyInt(), anyString());
        doAnswer(call -> arrivals.add(new Arrival(false, call.getArgument(0), call.getArgument(1),
                call.getArgument(2), call.getArgument(3), call.getArgument(4))))
                .when(listener).onMessageArrived(anyString(), anyString(), any(KuraPayload.class), anyInt(), anyBoolean());
        doAnswer(call -> arrivals.add(new Arrival(true, call.getArgument(0), call.getArgument(1),
                call.getArgument(2), call.getArgument(3), call.getArgument(4))))
                .when(listener).onControlMessageArrived(anyString(), anyString(), any(KuraPayload.class), anyInt(), anyBoolean());
        CloudClient client = this.cloud.newCloudClient("testService");
        client.addCloudClientListener(listener);
        try {
            assertFalse(client.isConnected());
            this.cloud.connect();
            assertTrue(client.isConnected());
            client.subscribe("test", 1);
            Date timestamp = new Date(1700000000000L);
            KuraPayload payload = payload("payload 数据", timestamp, 12);
            KuraPayload control = payload("control 数据", timestamp, 34);
            int normalId = client.publish("test", payload, 1, false, 5);
            int controlId = client.controlPublish("control_test", control, 1, false, 5);
            Set<Delivery> expected = Set.of(new Delivery(normalId, "test"), new Delivery(controlId, "control_test"));
            assertEquals(expected, Set.of(take(published), take(published)));
            assertEquals(expected, Set.of(take(confirmed), take(confirmed)));
            Set<Boolean> routes = new HashSet<>();
            for (int i = 0; i < 2; i++) {
                Arrival event = take(arrivals);
                assertTrue(routes.add(event.control()));
                assertEquals("test-client", event.device());
                assertEquals(event.control() ? "control_test" : "test", event.topic());
                KuraPayload original = event.control() ? control : payload;
                assertArrayEquals(original.getBody(), event.payload().getBody());
                assertEquals(timestamp, event.payload().getTimestamp());
                assertEquals(original.getMetric("value"), event.payload().getMetric("value"));
                assertEquals(1, event.qos());
                assertFalse(event.retain());
            }
            client.unsubscribe("test");
        } finally {
            client.removeCloudClientListener(listener);
            client.release();
        }
    }

    private static KuraPayload payload(String body, Date timestamp, int value) {
        KuraPayload result = new KuraPayload();
        result.setBody(body.getBytes(StandardCharsets.UTF_8));
        result.setTimestamp(timestamp);
        result.addMetric("value", value);
        return result;
    }

    private static <T> T take(BlockingQueue<T> queue) throws InterruptedException {
        T value = queue.poll(10, TimeUnit.SECONDS);
        assertNotNull(value, "Timed out waiting for CloudClient callback");
        return value;
    }
}
