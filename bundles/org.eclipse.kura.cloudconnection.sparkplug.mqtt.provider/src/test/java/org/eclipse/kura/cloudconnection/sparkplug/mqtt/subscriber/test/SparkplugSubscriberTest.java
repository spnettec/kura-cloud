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
package org.eclipse.kura.cloudconnection.sparkplug.mqtt.subscriber.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.kura.KuraException;
import org.eclipse.kura.cloudconnection.listener.CloudConnectionListener;
import org.eclipse.kura.cloudconnection.message.KuraMessage;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.endpoint.SparkplugCloudEndpoint;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.subscriber.SparkplugSubscriber;
import org.eclipse.kura.cloudconnection.subscriber.CloudSubscriber;
import org.eclipse.kura.cloudconnection.subscriber.listener.CloudSubscriberListener;
import org.eclipse.kura.data.DataService;
import org.eclipse.kura.message.KuraPayload;
import org.eclipse.tahu.protobuf.SparkplugBProto;
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload;
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload.DataSet;
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload.Metric;
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload.Metric.MetricValueExtension;
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload.Template;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.eclipse.kura.cloudconnection.CloudConnectionConstants;
import org.eclipse.kura.configuration.ConfigurationService;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.event.EventAdmin;

import com.google.protobuf.ByteString;

public class SparkplugSubscriberTest {

    private DataService dataService = mock(DataService.class);
    private SparkplugCloudEndpoint endpoint = null;
    private SparkplugSubscriber sub1 = null;
    private SparkplugSubscriber sub2 = null;
    private SparkplugSubscriber sub3 = null;
    private SparkplugSubscriber sub4 = null;
    private CloudSubscriberListener subListener = mock(CloudSubscriberListener.class);
    private CloudConnectionListener cloudConnectionListener = mock(CloudConnectionListener.class);

    private final List<ExecutorService> executors = new ArrayList<>();

    @BeforeEach
    void activateFixtures() throws Exception {
        // Observe ownership while retaining real virtual-thread execution.
        try (MockedStatic<Executors> factory = mockStatic(Executors.class, CALLS_REAL_METHODS)) {
            factory.when(Executors::newVirtualThreadPerTaskExecutor).thenAnswer(invocation -> {
                ExecutorService executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
                this.executors.add(executor);
                return executor;
            });
            this.endpoint = new SparkplugCloudEndpoint();
            this.sub1 = new SparkplugSubscriber();
            this.sub2 = new SparkplugSubscriber();
            this.sub3 = new SparkplugSubscriber();
            this.sub4 = new SparkplugSubscriber();
        }
        this.endpoint.setDataService(this.dataService);
        this.endpoint.setEventAdmin(mock(EventAdmin.class));
        this.endpoint.activate(Map.of(ConfigurationService.KURA_SERVICE_PID, "test.endpoint"));
        BundleContext bundle = mock(BundleContext.class);
        when(bundle.createFilter(anyString())).thenAnswer(invocation -> FrameworkUtil.createFilter(invocation.getArgument(0)));
        ComponentContext context = mock(ComponentContext.class);
        when(context.getBundleContext()).thenReturn(bundle);
        for (SparkplugSubscriber subscriber : List.of(this.sub1, this.sub2, this.sub3, this.sub4)) {
            subscriber.activate(context, Map.of(ConfigurationService.KURA_SERVICE_PID, "test.subscriber",
                    CloudConnectionConstants.CLOUD_ENDPOINT_SERVICE_PID_PROP_NAME.value(), "test.endpoint",
                    SparkplugSubscriber.KEY_TOPIC_FILTER, "initial/topic", SparkplugSubscriber.KEY_QOS, 0));
        }
    }

    @AfterEach
    void releaseFixtures() throws Exception {
        try {
            for (SparkplugSubscriber subscriber : List.of(this.sub1, this.sub2, this.sub3, this.sub4)) {
                subscriber.deactivate();
            }
            this.endpoint.deactivate();
        } finally {
            for (ExecutorService executor : this.executors) {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    private void finishCallbacks() throws InterruptedException {
        // Drain endpoint dispatch before subscriber dispatch; shutdown is graceful here.
        for (ExecutorService executor : this.executors) {
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /*
     * Scenarios
     */

    @Test
    public void shouldForwardMessagesToCorrectSubscribers() throws Exception {
        givenInitialSetup(true);
        givenRegisterSubscriber("a/b/c", 0, this.sub1);
        givenRegisterSubscriber("a/+/c", 0, this.sub2);
        givenRegisterSubscriber("a/#", 0, this.sub3);
        givenRegisterSubscriber("a/b", 0, this.sub4);
        givenRegisterCloudSubscriberListener(this.sub1, this.subListener);
        givenRegisterCloudSubscriberListener(this.sub2, this.subListener);
        givenRegisterCloudSubscriberListener(this.sub3, this.subListener);
        givenRegisterCloudSubscriberListener(this.sub4, this.subListener);

        whenOnMessageArrivedWithAllSupportedMetrics("a/b/c", 0);

        thenSubscriberListenerNotifiedOnMessageArrived(this.subListener, 3);
    }

    @Test
    public void shouldNotForwardMessagesToUnsubscribed() throws Exception {
        givenInitialSetup(true);
        givenRegisterSubscriber("a/b/c", 0, this.sub1);
        givenRegisterSubscriber("a/+/c", 0, this.sub2);
        givenRegisterSubscriber("a/#", 0, this.sub3);
        givenRegisterSubscriber("a/b", 0, this.sub4);
        givenRegisterCloudSubscriberListener(this.sub1, this.subListener);
        givenRegisterCloudSubscriberListener(this.sub2, this.subListener);
        givenRegisterCloudSubscriberListener(this.sub3, this.subListener);
        givenRegisterCloudSubscriberListener(this.sub4, this.subListener);
        givenUnregisterSubscriber(this.sub1);
        givenUnregisterSubscriber(this.sub2);

        whenOnMessageArrivedWithAllSupportedMetrics("a/b/c", 0);

        thenSubscriberListenerNotifiedOnMessageArrived(this.subListener, 1);
    }

    @Test
    public void shouldSubscribeOnConnectionEstabilished() throws Exception {
        givenInitialSetup(false);
        givenRegisterSubscriber("a/b/c", 0, this.sub1);
        givenRegisterSubscriber("a/+/c", 0, this.sub2);
        givenRegisterSubscriber("a/#", 1, this.sub3);
        givenRegisterSubscriber("a/#", 0, this.sub4);

        whenEndpointEstablishConnection();

        thenSubscribed("a/b/c", 0);
        thenSubscribed("a/+/c", 0);
        thenSubscribed("a/#", 1);
        thenSubscribed("a/#", 0);
    }

    @Test
    public void shouldNotifyCloudConnectionListenerOnDisconnected() throws InterruptedException {
        givenCloudConnectionListener(this.sub1, this.cloudConnectionListener);

        whenOnDisconnected(this.sub1);

        thenConnectionListenerNotifiedOnDisconnected(this.cloudConnectionListener);
    }

    @Test
    public void shouldNotifyCloudConnectionListenerOnConnectionLost() throws InterruptedException {
        givenCloudConnectionListener(this.sub1, this.cloudConnectionListener);

        whenOnConnectionLost(this.sub1);

        thenConnectionListenerNotifiedOnConnectionLost(this.cloudConnectionListener);
    }

    @Test
    public void shouldNotifyCloudConnectionListenerOnConnectionEstablished() throws InterruptedException {
        givenCloudConnectionListener(this.sub1, this.cloudConnectionListener);

        whenOnConnectionEstablished(this.sub1);

        thenConnectionListenerNotifiedOnConnectionEstablished(this.cloudConnectionListener);
    }

    /*
     * Steps
     */

    /*
     * Given
     */

    private void givenInitialSetup(boolean isConnected) {
        when(this.dataService.isConnected()).thenReturn(isConnected);
        this.endpoint.setDataService(this.dataService);

        EventAdmin mockEventAdmin = mock(EventAdmin.class);
        this.endpoint.setEventAdmin(mockEventAdmin);
    }

    private void givenRegisterSubscriber(String topicFilter, int qos, CloudSubscriberListener listener)
            throws Exception {
        Map<String, Object> properties = new HashMap<>();
        properties.put(SparkplugSubscriber.KEY_TOPIC_FILTER, topicFilter);
        properties.put(SparkplugSubscriber.KEY_QOS, qos);

        this.endpoint.registerSubscriber(properties, listener);
    }

    private void givenUnregisterSubscriber(CloudSubscriberListener listener) {

        this.endpoint.unregisterSubscriber(listener);
    }

    private void givenRegisterCloudSubscriberListener(CloudSubscriber subscriber, CloudSubscriberListener listener)
            throws Exception {
        subscriber.registerCloudSubscriberListener(listener);
    }

    private void givenCloudConnectionListener(CloudSubscriber subscriber, CloudConnectionListener listener) {
        subscriber.registerCloudConnectionListener(listener);
    }

    /*
     * When
     */

    private void whenOnMessageArrivedWithAllSupportedMetrics(String topic, int qos) {
        SparkplugBProto.Payload.Builder builder = Payload.newBuilder();

        builder.addMetrics(getBooleanMetric("metric.boolean", true));
        builder.addMetrics(getBytesMetric("metric.bytes", "test".getBytes(StandardCharsets.UTF_8)));
        builder.addMetrics(getDatasetMetric("metric.dataset", DataSet.getDefaultInstance()));
        builder.addMetrics(getDoubleMetric("metric.double", 12.3));
        builder.addMetrics(getExtensionValueMetric("metric.extension", MetricValueExtension.getDefaultInstance()));
        builder.addMetrics(getFloatMetric("metric.float", 12f));
        builder.addMetrics(getIntegerMetric("metric.int", 11));
        builder.addMetrics(getLongMetric("metric.long", 123L));
        builder.addMetrics(getStringMetric("metric.string", "test"));
        builder.addMetrics(getTemplateMetric("metric.template", Template.getDefaultInstance()));

        builder.setBody(ByteString.copyFrom("example".getBytes(StandardCharsets.UTF_8)));
        builder.setTimestamp(1000L);
        builder.setSeq(100L);

        this.endpoint.onMessageArrived(topic, builder.build().toByteArray(), qos, false);
    }

    private void whenEndpointEstablishConnection() {
        when(this.dataService.isConnected()).thenReturn(true);
        this.endpoint.onConnectionEstablished();
    }

    private void whenOnDisconnected(SparkplugSubscriber subscriber) {
        subscriber.onDisconnected();
    }

    private void whenOnConnectionLost(SparkplugSubscriber subscriber) {
        subscriber.onConnectionLost();
    }

    private void whenOnConnectionEstablished(SparkplugSubscriber subscriber) {
        subscriber.onConnectionEstablished();
    }

    /*
     * Then
     */

    private void thenSubscriberListenerNotifiedOnMessageArrived(CloudSubscriberListener listener,
            int expectedTimes) throws InterruptedException {
        finishCallbacks();
        ArgumentCaptor<KuraMessage> messages = ArgumentCaptor.forClass(KuraMessage.class);
        verify(listener, times(expectedTimes)).onMessageArrived(messages.capture());
        verifyNoMoreInteractions(listener);
        for (KuraMessage message : messages.getAllValues()) {
            KuraPayload payload = message.getPayload();
            assertArrayEquals("example".getBytes(StandardCharsets.UTF_8), payload.getBody());
            assertEquals(1000L, payload.getTimestamp().getTime());
            assertEquals(100L, payload.getMetric("seq"));
            assertEquals(11, payload.metrics().size());
            assertEquals(true, payload.getMetric("metric.boolean"));
            assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), (byte[]) payload.getMetric("metric.bytes"));
            assertArrayEquals(DataSet.getDefaultInstance().toByteArray(), (byte[]) payload.getMetric("metric.dataset"));
            assertEquals(12.3, payload.getMetric("metric.double"));
            assertArrayEquals(MetricValueExtension.getDefaultInstance().toByteArray(),
                    (byte[]) payload.getMetric("metric.extension"));
            assertEquals(12F, payload.getMetric("metric.float"));
            assertEquals(11, payload.getMetric("metric.int"));
            assertEquals(123L, payload.getMetric("metric.long"));
            assertEquals("test", payload.getMetric("metric.string"));
            assertArrayEquals(Template.getDefaultInstance().toByteArray(), (byte[]) payload.getMetric("metric.template"));
        }
    }

    private void thenConnectionListenerNotifiedOnDisconnected(CloudConnectionListener subscriberListener) throws InterruptedException {
        finishCallbacks();
        verify(subscriberListener).onDisconnected();
        verifyNoMoreInteractions(subscriberListener);
    }

    private void thenConnectionListenerNotifiedOnConnectionLost(CloudConnectionListener subscriberListener) throws InterruptedException {
        finishCallbacks();
        verify(subscriberListener).onConnectionLost();
        verifyNoMoreInteractions(subscriberListener);
    }

    private void thenConnectionListenerNotifiedOnConnectionEstablished(CloudConnectionListener subscriberListener) throws InterruptedException {
        finishCallbacks();
        verify(subscriberListener).onConnectionEstablished();
        verifyNoMoreInteractions(subscriberListener);
    }

    private void thenSubscribed(String expectedTopicFilter, int expectedQos) throws KuraException {
        verify(this.dataService, times(1)).subscribe(expectedTopicFilter, expectedQos);
    }



    /*
     * Utils
     */

    private Metric getBooleanMetric(String name, boolean value) {
        Payload.Metric.Builder metricBuilder = Payload.Metric.newBuilder();
        metricBuilder.setName(name);
        metricBuilder.setBooleanValue(value);
        return metricBuilder.build();
    }

    private Metric getBytesMetric(String name, byte[] value) {
        Payload.Metric.Builder metricBuilder = Payload.Metric.newBuilder();
        metricBuilder.setName(name);
        metricBuilder.setBytesValue(ByteString.copyFrom(value));
        return metricBuilder.build();
    }

    private Metric getDatasetMetric(String name, DataSet value) {
        Payload.Metric.Builder metricBuilder = Payload.Metric.newBuilder();
        metricBuilder.setName(name);
        metricBuilder.setDatasetValue(value);
        return metricBuilder.build();
    }

    private Metric getDoubleMetric(String name, double value) {
        Payload.Metric.Builder metricBuilder = Payload.Metric.newBuilder();
        metricBuilder.setName(name);
        metricBuilder.setDoubleValue(value);
        return metricBuilder.build();
    }

    private Metric getExtensionValueMetric(String name, MetricValueExtension value) {
        Payload.Metric.Builder metricBuilder = Payload.Metric.newBuilder();
        metricBuilder.setName(name);
        metricBuilder.setExtensionValue(value);
        return metricBuilder.build();
    }

    private Metric getFloatMetric(String name, float value) {
        Payload.Metric.Builder metricBuilder = Payload.Metric.newBuilder();
        metricBuilder.setName(name);
        metricBuilder.setFloatValue(value);
        return metricBuilder.build();
    }

    private Metric getIntegerMetric(String name, int value) {
        Payload.Metric.Builder metricBuilder = Payload.Metric.newBuilder();
        metricBuilder.setName(name);
        metricBuilder.setIntValue(value);
        return metricBuilder.build();
    }

    private Metric getLongMetric(String name, long value) {
        Payload.Metric.Builder metricBuilder = Payload.Metric.newBuilder();
        metricBuilder.setName(name);
        metricBuilder.setLongValue(value);
        return metricBuilder.build();
    }

    private Metric getStringMetric(String name, String value) {
        Payload.Metric.Builder metricBuilder = Payload.Metric.newBuilder();
        metricBuilder.setName(name);
        metricBuilder.setStringValue(value);
        return metricBuilder.build();
    }

    private Metric getTemplateMetric(String name, Template value) {
        Payload.Metric.Builder metricBuilder = Payload.Metric.newBuilder();
        metricBuilder.setName(name);
        metricBuilder.setTemplateValue(value);
        return metricBuilder.build();
    }

}
