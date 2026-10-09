/*******************************************************************************
 * Copyright (c) 2023, 2026 Eurotech and/or its affiliates and others
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
package org.eclipse.kura.internal.rest.cloudconnection.provider.test;

import static org.eclipse.kura.configuration.ConfigurationService.KURA_SERVICE_PID;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.eclipse.kura.KuraConnectException;
import org.eclipse.kura.KuraException;
import org.eclipse.kura.KuraRuntimeException;
import org.eclipse.kura.cloud.CloudService;
import org.eclipse.kura.cloudconnection.CloudConnectionManager;
import org.eclipse.kura.data.DataService;
import org.eclipse.kura.internal.rest.cloudconnection.provider.CloudConnectionManagerBridge;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;

@SuppressWarnings("deprecation")
class CloudConnectionManagerBridgeTest {

    private enum Operation { CONNECT, DISCONNECT, STATUS, FAILED_CONNECT }

    private static final String ENDPOINT = "custom.cloud";
    private final BundleContext context = mock(BundleContext.class);
    private MockedStatic<FrameworkUtil> framework;
    private CloudConnectionManagerBridge bridge;

    @BeforeEach
    void setUp() throws Exception {
        Bundle bundle = mock(Bundle.class);
        when(bundle.getBundleContext()).thenReturn(this.context);
        this.framework = mockStatic(FrameworkUtil.class, CALLS_REAL_METHODS);
        this.framework.when(() -> FrameworkUtil.getBundle(CloudConnectionManagerBridge.class)).thenReturn(bundle);
        when(this.context.getServiceReferences(CloudService.class, null)).thenReturn(List.of());
        when(this.context.getServiceReferences(CloudConnectionManager.class, null)).thenReturn(List.of());
        this.bridge = new CloudConnectionManagerBridge();
    }

    @AfterEach
    void tearDown() {
        this.framework.close();
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    void releasesOnlyAcquiredDataService(Operation operation) throws Exception {
        ServiceReference<CloudService> other = reference("other.cloud");
        ServiceReference<CloudService> cloud = reference("legacy." + ENDPOINT);
        ServiceReference<DataService> data = reference("linked.data");
        DataService service = mock(DataService.class);
        String target = "(kura.service.pid=linked.data)";
        when(cloud.getProperty("DataService.target")).thenReturn(target);
        when(this.context.getServiceReferences(CloudService.class, null)).thenReturn(List.of(other, cloud));
        when(this.context.getServiceReferences(DataService.class, target)).thenReturn(List.of(data));
        when(this.context.getService(data)).thenReturn(service);
        when(service.isConnected()).thenReturn(true);
        if (operation == Operation.FAILED_CONNECT) {
            doThrow(new KuraConnectException("expected connection failure")).when(service).connect();
            assertThrows(KuraException.class, () -> this.bridge.connectCloudEndpoint(ENDPOINT));
        } else {
            invoke(operation);
        }
        verifyOperation(service, operation);
        verify(this.context).ungetService(data);
        verify(this.context, never()).getService(cloud);
        verify(this.context, never()).ungetService(cloud);
        verify(this.context, never()).getService(other);
        verify(this.context, never()).ungetService(other);
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    void releasesOnlyAcquiredConnectionManager(Operation operation) throws Exception {
        ServiceReference<CloudConnectionManager> other = reference("other.cloud");
        ServiceReference<CloudConnectionManager> selected = reference(ENDPOINT);
        CloudConnectionManager service = mock(CloudConnectionManager.class);
        when(this.context.getServiceReferences(CloudConnectionManager.class, null)).thenReturn(List.of(other, selected));
        when(this.context.getService(selected)).thenReturn(service);
        when(service.isConnected()).thenReturn(true);
        if (operation == Operation.FAILED_CONNECT) {
            doThrow(new KuraConnectException("expected connection failure")).when(service).connect();
            assertThrows(KuraRuntimeException.class, () -> this.bridge.connectCloudEndpoint(ENDPOINT));
        } else {
            invoke(operation);
        }
        switch (operation) {
        case CONNECT, FAILED_CONNECT -> verify(service).connect();
        case DISCONNECT -> verify(service).disconnect();
        case STATUS -> verify(service).isConnected();
        }
        verify(this.context).ungetService(selected);
        verify(this.context, never()).getService(other);
        verify(this.context, never()).ungetService(other);
    }

    private void invoke(Operation operation) throws KuraException {
        switch (operation) {
        case CONNECT -> this.bridge.connectCloudEndpoint(ENDPOINT);
        case DISCONNECT -> this.bridge.disconnectCloudEndpoint(ENDPOINT);
        case STATUS -> assertTrue(this.bridge.isConnectedCloudEndpoint(ENDPOINT));
        default -> throw new IllegalArgumentException();
        }
    }

    private void verifyOperation(DataService service, Operation operation) throws Exception {
        switch (operation) {
        case CONNECT, FAILED_CONNECT -> verify(service).connect();
        case DISCONNECT -> verify(service).disconnect(10);
        case STATUS -> verify(service).isConnected();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> ServiceReference<T> reference(String pid) {
        ServiceReference<T> reference = mock(ServiceReference.class);
        when(reference.getProperty(KURA_SERVICE_PID)).thenReturn(pid);
        return reference;
    }
}
