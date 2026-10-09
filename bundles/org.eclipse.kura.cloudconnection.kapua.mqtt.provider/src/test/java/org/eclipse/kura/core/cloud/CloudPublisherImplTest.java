/*******************************************************************************
 * Copyright (c) 2018, 2026 Eurotech and/or its affiliates and others
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.eclipse.kura.cloudconnection.CloudConnectionManager;
import org.eclipse.kura.cloudconnection.CloudConnectionConstants;
import org.eclipse.kura.configuration.ConfigurationService;
import org.eclipse.kura.cloudconnection.message.KuraMessage;
import org.eclipse.kura.message.KuraPayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.osgi.service.component.ComponentContext;

class CloudPublisherImplTest {
    private final CloudServiceImpl cloud = mock(CloudServiceImpl.class);
    private final ComponentContext component = mock(ComponentContext.class);
    private final TestPublisher publisher = new TestPublisher();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void activate() throws Exception {
        BundleContext bundle = mock(BundleContext.class);
        ServiceReference<CloudConnectionManager> reference = mock(ServiceReference.class);
        when(reference.getProperty(Constants.OBJECTCLASS)).thenReturn(new String[] {CloudConnectionManager.class.getName()});
        when(reference.getProperty(ConfigurationService.KURA_SERVICE_PID)).thenReturn("selected.cloud");
        when(bundle.createFilter(anyString())).thenAnswer(call -> FrameworkUtil.createFilter(call.getArgument(0)));
        when(bundle.getServiceReferences(nullable(String.class), anyString())).thenReturn(new ServiceReference<?>[] {reference});
        when(bundle.getService(reference)).thenReturn(cloud);
        when(component.getBundleContext()).thenReturn(bundle);
        when(cloud.publish(any())).thenAnswer(call -> {
            KuraMessage message = call.getArgument(0);
            return Integer.valueOf(0).equals(message.getProperties().get("QOS")) ? null : "message-1";
        });
        publisher.start(component, properties(0));
    }

    @AfterEach
    void deactivate() {
        publisher.stop(component);
    }

    @Test
    void shouldBindThroughServiceTracker() {
        verify(cloud).registerCloudConnectionListener(publisher);
        verify(cloud).registerCloudPublisherDeliveryListener(publisher);
    }

    @Test
    void testPublishNullMessage() {
        assertThrows(IllegalArgumentException.class, () -> publisher.publish(null));
    }

    @Test
    void testPublishQos0() throws Exception {
        KuraPayload payload = new KuraPayload();
        assertNull(publisher.publish(new KuraMessage(payload)));
        assertPublished(payload, 0);
    }

    @Test
    void testPublishQos1() throws Exception {
        publisher.updated(properties(1));
        KuraPayload payload = new KuraPayload();
        assertEquals("message-1", publisher.publish(new KuraMessage(payload)));
        assertPublished(payload, 1);
    }

    private void assertPublished(KuraPayload payload, int qos) throws Exception {
        ArgumentCaptor<KuraMessage> sent = ArgumentCaptor.forClass(KuraMessage.class);
        verify(cloud).publish(sent.capture());
        assertSame(payload, sent.getValue().getPayload());
        assertEquals(Map.of("APP_TOPIC", "A1/$assetName", "APP_ID", "W1", "QOS", qos,
                "RETAIN", false, "PRIORITY", 7, "CONTROL", false), sent.getValue().getProperties());
    }

    private static Map<String, Object> properties(int qos) {
        return Map.of(CloudConnectionConstants.CLOUD_ENDPOINT_SERVICE_PID_PROP_NAME.value(), "selected.cloud", "qos", qos);
    }

    private static class TestPublisher extends org.eclipse.kura.core.cloud.publisher.CloudPublisherImpl {
        void start(ComponentContext context, Map<String, Object> properties) {
            super.activate(context, properties);
        }
        void stop(ComponentContext context) {
            super.deactivate(context);
        }
    }
}
