/*******************************************************************************
 * Copyright (c) 2024, 2026 Eurotech and/or its affiliates and others
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *  Eurotech
 *******************************************************************************/
package org.eclipse.kura.cloudconnection.sparkplug.mqtt.provider.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.eclipse.kura.cloudconnection.CloudConnectionConstants;
import org.eclipse.kura.cloudconnection.CloudConnectionManager;
import org.eclipse.kura.cloudconnection.message.KuraMessage;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.device.SparkplugDevice;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.endpoint.SparkplugCloudEndpoint;
import org.eclipse.kura.configuration.ConfigurationService;
import org.eclipse.kura.data.DataService;
import org.eclipse.kura.data.transport.listener.DataTransportListener;
import org.eclipse.kura.message.KuraPayload;
import org.eclipse.kura.message.KuraPosition;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload;
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload.Metric;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.event.EventAdmin;

class SparkplugDeviceTest extends SparkplugIntegrationTest {

    private static final String BIRTH = "spBv1.0/g1/DBIRTH/n1/d1";
    private static final String DATA = "spBv1.0/g1/DDATA/n1/d1";
    private SparkplugCloudEndpoint endpoint;
    private SparkplugDevice device;
    private MqttCallback callback;
    private List<ExecutorService> workers;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void activateDevice() throws Exception {
        this.workers = new ArrayList<>();
        try (MockedStatic<Executors> factory = mockStatic(Executors.class, CALLS_REAL_METHODS)) {
            factory.when(Executors::newVirtualThreadPerTaskExecutor).thenAnswer(invocation -> {
                ExecutorService executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
                this.workers.add(executor);
                return executor;
            });
            this.endpoint = new SparkplugCloudEndpoint();
            this.device = new SparkplugDevice();
        }
        DataService data = mock(DataService.class);
        when(data.isConnected()).thenAnswer(invocation -> this.transport.isConnected());
        doAnswer(invocation -> { connectTransport(); return null; }).when(data).connect();
        doAnswer(invocation -> { this.transport.disconnect(invocation.getArgument(0)); return null; })
                .when(data).disconnect(anyLong());
        when(data.publish(anyString(), any(byte[].class), anyInt(), anyBoolean(), anyInt())).thenAnswer(invocation -> {
            this.transport.publish(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2),
                    invocation.getArgument(3));
            return 0;
        });
        this.endpoint.setDataService(data);
        this.endpoint.setEventAdmin(mock(EventAdmin.class));
        this.endpoint.activate(Map.of(ConfigurationService.KURA_SERVICE_PID, "test.endpoint"));
        DataTransportListener bridge = mock(DataTransportListener.class);
        doAnswer(invocation -> { this.endpoint.onConnectionEstablished(); return null; })
                .when(bridge).onConnectionEstablished(anyBoolean());
        doAnswer(invocation -> { this.endpoint.onDisconnected(); return null; }).when(bridge).onDisconnected();
        this.transport.addDataTransportListener(bridge);

        BundleContext bundle = mock(BundleContext.class);
        ServiceReference<CloudConnectionManager> reference = mock(ServiceReference.class);
        when(reference.getProperty(Constants.OBJECTCLASS)).thenReturn(new String[] { CloudConnectionManager.class.getName() });
        when(reference.getProperty(ConfigurationService.KURA_SERVICE_PID)).thenReturn("test.endpoint");
        when(bundle.createFilter(anyString())).thenAnswer(invocation -> FrameworkUtil.createFilter(invocation.getArgument(0)));
        when(bundle.getServiceReferences(nullable(String.class), anyString())).thenReturn(new ServiceReference<?>[] { reference });
        when(bundle.getService(reference)).thenReturn(this.endpoint);
        ComponentContext context = mock(ComponentContext.class);
        when(context.getBundleContext()).thenReturn(bundle);
        this.device.activate(context, Map.of(SparkplugDevice.KEY_DEVICE_ID, "d1",
                CloudConnectionConstants.CLOUD_ENDPOINT_SERVICE_PID_PROP_NAME.value(), "test.endpoint"));

        this.callback = mock(MqttCallback.class);
        this.peer.setCallback(this.callback);
        this.peer.subscribe("spBv1.0/#", 1);
        configureTransport("");
    }

    @AfterEach
    void deactivateDevice() throws Exception {
        try {
            this.device.deactivate();
            this.endpoint.deactivate();
        } finally {
            for (ExecutorService worker : this.workers) {
                worker.shutdownNow();
                assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void shouldPublishDeviceBirth() throws Exception {
        this.endpoint.connect();
        publish(metrics());
        assertSequence("spBv1.0/g1/NBIRTH/n1", 0);
        assertSequence(BIRTH, 1);
    }

    @Test
    void shouldPublishDeviceDataMessageOnUnchangedMetrics() throws Exception {
        this.endpoint.connect();
        publish(metrics());
        publish(metrics());
        assertSequence(BIRTH, 1);
        assertSequence(DATA, 2);
    }

    @Test
    void shouldRepublishDeviceBirthOnChangedMetrics() throws Exception {
        this.endpoint.connect();
        publish(metrics());
        KuraPayload changed = metrics();
        changed.addMetric("metric.int", 12);
        publish(changed);
        List<Payload> births = received(BIRTH, 2);
        assertEquals(List.of(1L, 2L), births.stream().map(Payload::getSeq).toList());
        assertEquals(Set.of("metric.string"), births.get(0).getMetricsList().stream().map(Metric::getName).collect(Collectors.toSet()));
        assertEquals(Set.of("metric.string", "metric.int"), births.get(1).getMetricsList().stream().map(Metric::getName).collect(Collectors.toSet()));
    }

    @Test
    void shouldRepublishDeviceBirthOnReconnection() throws Exception {
        this.endpoint.connect();
        publish(metrics());
        received(BIRTH, 1);
        this.endpoint.disconnect();
        this.endpoint.connect();
        publish(metrics());
        assertEquals(List.of(0L, 0L), received("spBv1.0/g1/NBIRTH/n1", 2).stream().map(Payload::getSeq).toList());
        assertEquals(List.of(1L, 1L), received(BIRTH, 2).stream().map(Payload::getSeq).toList());
    }

    @Test
    void shouldPublishTimestamp() throws Exception {
        this.endpoint.connect();
        KuraPayload payload = new KuraPayload();
        payload.setTimestamp(new Date(123456789L));
        publish(payload);
        assertEquals(123456789L, received(BIRTH, 1).getFirst().getTimestamp());
    }

    @Test
    void shouldPublishBody() throws Exception {
        this.endpoint.connect();
        KuraPayload payload = new KuraPayload();
        byte[] body = "设备消息".getBytes(StandardCharsets.UTF_8);
        payload.setBody(body);
        publish(payload);
        assertArrayEquals(body, received(BIRTH, 1).getFirst().getBody().toByteArray());
    }

    @Test
    void shouldFlattenKuraPositionIntoMetrics() throws Exception {
        this.endpoint.connect();
        KuraPosition position = new KuraPosition();
        position.setAltitude(699.3);
        position.setHeading(200.0);
        position.setLatitude(30.5);
        position.setLongitude(120.0);
        position.setPrecision(89.98);
        position.setSatellites(12);
        position.setSpeed(30.2);
        position.setStatus(1);
        position.setTimestamp(new Date(123456789L));
        KuraPayload payload = new KuraPayload();
        payload.setPosition(position);
        publish(payload);
        Map<String, Metric> metrics = received(BIRTH, 1).getFirst().getMetricsList().stream()
                .collect(Collectors.toMap(Metric::getName, metric -> metric));
        assertEquals(9, metrics.size());
        assertEquals(699.3, metrics.get("kura.position.altitude").getDoubleValue());
        assertEquals(200.0, metrics.get("kura.position.heading").getDoubleValue());
        assertEquals(30.5, metrics.get("kura.position.latitude").getDoubleValue());
        assertEquals(120.0, metrics.get("kura.position.longitude").getDoubleValue());
        assertEquals(89.98, metrics.get("kura.position.precision").getDoubleValue());
        assertEquals(12, metrics.get("kura.position.satellites").getIntValue());
        assertEquals(30.2, metrics.get("kura.position.speed").getDoubleValue());
        assertEquals(1, metrics.get("kura.position.status").getIntValue());
        assertEquals(123456789L, metrics.get("kura.position.timestamp").getLongValue());
    }

    private KuraPayload metrics() {
        KuraPayload payload = new KuraPayload();
        payload.addMetric("metric.string", "test string");
        return payload;
    }

    private void publish(KuraPayload payload) throws Exception {
        this.device.publish(new KuraMessage(payload));
    }

    private void assertSequence(String topic, long sequence) throws Exception {
        assertEquals(sequence, received(topic, 1).getFirst().getSeq());
    }

    private List<Payload> received(String topic, int count) throws Exception {
        ArgumentCaptor<MqttMessage> messages = ArgumentCaptor.forClass(MqttMessage.class);
        verify(this.callback, timeout(5000).times(count)).messageArrived(eq(topic), messages.capture());
        List<Payload> payloads = new ArrayList<>();
        for (MqttMessage message : messages.getAllValues()) {
            assertEquals(0, message.getQos());
            assertFalse(message.isRetained());
            payloads.add(Payload.parseFrom(message.getPayload()));
        }
        return payloads;
    }
}
