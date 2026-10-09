/*******************************************************************************
 * Copyright (c) 2024 Eurotech and/or its affiliates and others
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
package org.eclipse.kura.cloudconnection.sparkplug.mqtt.utils.test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;

import org.eclipse.kura.cloudconnection.CloudConnectionManager;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.endpoint.SparkplugCloudEndpoint;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.utils.SparkplugCloudEndpointTracker;
import org.eclipse.kura.configuration.ConfigurationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;

class SparkplugCloudEndpointTrackerTest {

    private final BundleContext bundle = mock(BundleContext.class);
    private final SparkplugCloudEndpoint endpoint = mock(SparkplugCloudEndpoint.class);
    private ServiceReference<CloudConnectionManager> reference;
    private SparkplugCloudEndpointTracker tracker;
    private Consumer<SparkplugCloudEndpoint> removed;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void startTracking() throws Exception {
        this.reference = mock(ServiceReference.class);
        this.removed = mock(Consumer.class);
        when(this.reference.getProperty(Constants.OBJECTCLASS))
                .thenReturn(new String[] { CloudConnectionManager.class.getName() });
        when(this.reference.getProperty(ConfigurationService.KURA_SERVICE_PID)).thenReturn("test.endpoint");
        when(this.bundle.createFilter(anyString()))
                .thenAnswer(invocation -> FrameworkUtil.createFilter(invocation.getArgument(0)));
        when(this.bundle.getServiceReferences(nullable(String.class), anyString()))
                .thenReturn(new ServiceReference<?>[] { this.reference });
        when(this.bundle.getService(this.reference)).thenReturn(this.endpoint);
        this.tracker = new SparkplugCloudEndpointTracker(this.bundle, endpoint -> { }, this.removed, "test.endpoint");
        this.tracker.startEndpointTracker();
        verify(this.bundle).getService(this.reference);
    }

    @AfterEach
    void closeTracker() {
        this.tracker.stopEndpointTracker();
    }

    @Test
    void shouldReleaseServiceReferenceWhenTrackerCloses() {
        this.tracker.stopEndpointTracker();
        verify(this.removed).accept(this.endpoint);
        verify(this.bundle).ungetService(this.reference);
    }

    @Test
    void shouldReleaseUnregisteredServiceExactlyOnce() throws Exception {
        ArgumentCaptor<ServiceListener> listener = ArgumentCaptor.forClass(ServiceListener.class);
        verify(this.bundle).addServiceListener(listener.capture(), anyString());
        listener.getValue().serviceChanged(new ServiceEvent(ServiceEvent.UNREGISTERING, this.reference));
        this.tracker.stopEndpointTracker();
        verify(this.removed).accept(this.endpoint);
        verify(this.bundle, times(1)).ungetService(this.reference);
    }
}
