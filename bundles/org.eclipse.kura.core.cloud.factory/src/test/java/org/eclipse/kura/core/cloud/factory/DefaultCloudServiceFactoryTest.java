/*******************************************************************************
 * Copyright (c) 2022, 2026 Eurotech and/or its affiliates and others
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

package org.eclipse.kura.core.cloud.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.kura.cloudconnection.CloudConnectionManager;
import org.eclipse.kura.configuration.ComponentConfiguration;
import org.eclipse.kura.configuration.ConfigurationService;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;

class DefaultCloudServiceFactoryTest {

    private static final String CLOUD = "org.eclipse.kura.cloud.CloudService";
    private static final String DATA = "org.eclipse.kura.data.DataService";
    private static final String TRANSPORT = "org.eclipse.kura.core.data.transport.mqtt.MqttDataTransport";
    private final ConfigurationService configurations = mock(ConfigurationService.class);
    private final DefaultCloudServiceFactory factory = new DefaultCloudServiceFactory();

    DefaultCloudServiceFactoryTest() {
        factory.setConfigurationService(configurations);
    }

    @Test
    void shouldCreateLinkedConfigurationStack() throws Exception {
        assertCreatedStack(CLOUD + "-1", null, null);
    }

    @Test
    void shouldPreserveLocalCustomPidAndLocalizedMetadata() throws Exception {
        assertCreatedStack("custom.connection", "云连接", "工厂描述");
    }

    private void assertCreatedStack(String pid, String name, String description) throws Exception {
        List<String> factories = new ArrayList<>();
        List<String> pids = new ArrayList<>();
        doAnswer(invocation -> {
            String factoryPid = invocation.getArgument(0);
            String servicePid = invocation.getArgument(1);
            Map<String, Object> properties = invocation.getArgument(2);
            boolean snapshot = invocation.getArgument(3);
            factories.add(factoryPid);
            pids.add(servicePid);
            if (TRANSPORT.equals(factoryPid)) {
                assertTrue(servicePid.startsWith(TRANSPORT + "-"));
                assertEquals(null, properties);
                assertFalse(snapshot);
            } else if (DATA.equals(factoryPid)) {
                assertTrue(servicePid.startsWith(DATA + "-"));
                assertEquals(Map.of("DataTransportService.target", target(pids.get(0))), properties);
                assertFalse(snapshot);
            } else {
                assertEquals(pid, servicePid);
                Map<String, Object> expected = new HashMap<>();
                expected.put("DataService.target", target(pids.get(1)));
                if (name != null) {
                    expected.put(ConfigurationService.KURA_CLOUD_FACTORY_NAME, name);
                    expected.put(ConfigurationService.KURA_CLOUD_FACTORY_DESC, description);
                }
                assertEquals(expected, properties);
                assertTrue(snapshot);
            }
            return null;
        }).when(configurations).createFactoryConfiguration(anyString(), anyString(), any(), anyBoolean());
        if (name == null) {
            factory.createConfiguration(pid);
        } else {
            factory.createConfiguration(pid, name, description);
        }
        assertEquals(List.of(TRANSPORT, DATA, CLOUD), factories);
    }

    @Test
    void shouldDeleteLinkedStackWithOneFinalSnapshot() throws Exception {
        linkedStack(CLOUD + "-1");
        factory.deleteConfiguration(CLOUD + "-1");
        InOrder order = inOrder(configurations);
        order.verify(configurations).deleteFactoryConfiguration(CLOUD + "-1", false);
        order.verify(configurations).deleteFactoryConfiguration("custom.data", false);
        order.verify(configurations).deleteFactoryConfiguration("custom.transport", true);
    }

    @Test
    void shouldDeleteCustomPidWithNoLinkedStack() throws Exception {
        configuration("custom.connection", Map.of());
        factory.deleteConfiguration("custom.connection");
        verify(configurations).deleteFactoryConfiguration("custom.connection", true);
    }

    @Test
    void shouldResolveStackFromReferenceTargets() throws Exception {
        linkedStack(CLOUD + "-1");
        assertEquals(List.of(CLOUD + "-1", "custom.data", "custom.transport"),
                factory.getStackComponentsPids(CLOUD + "-1"));
    }

    @Test
    void shouldResolveCustomPidStack() throws Exception {
        linkedStack("custom.connection");
        assertEquals(List.of("custom.connection", "custom.data", "custom.transport"),
                factory.getStackComponentsPids("custom.connection"));
    }

    @Test
    void shouldReturnNoManagedConnectionsForEmptyRegistry() throws Exception {
        activate(List.of());
        assertEquals(Set.of(), factory.getManagedCloudConnectionPids());
    }

    @Test
    void shouldReturnManagedConnections() throws Exception {
        activate(List.of(reference(CLOUD + "-FOO", CLOUD), reference(CLOUD, CLOUD)));
        assertEquals(Set.of(CLOUD + "-FOO", CLOUD), factory.getManagedCloudConnectionPids());
    }

    @Test
    void shouldSelectByLocalServiceFactoryPid() throws Exception {
        activate(List.of(reference(CLOUD + "-OK", CLOUD), reference("custom.connection", CLOUD),
                reference("unrelated", "other.factory"), reference(null, CLOUD), reference(42, CLOUD)));
        assertEquals(Set.of(CLOUD + "-OK", "custom.connection"), factory.getManagedCloudConnectionPids());
    }

    private void linkedStack(String pid) throws Exception {
        configuration(pid, Map.of("DataService.target", target("custom.data")));
        configuration("custom.data", Map.of("DataTransportService.target", target("custom.transport")));
    }

    private void configuration(String pid, Map<String, Object> properties) throws Exception {
        ComponentConfiguration configuration = mock(ComponentConfiguration.class);
        when(configuration.getConfigurationProperties()).thenReturn(properties);
        when(configurations.getComponentConfiguration(pid)).thenReturn(configuration);
    }

    private static String target(String pid) {
        return "(" + ConfigurationService.KURA_SERVICE_PID + "=" + pid + ")";
    }

    private void activate(Collection<ServiceReference<CloudConnectionManager>> references) throws Exception {
        BundleContext bundle = mock(BundleContext.class);
        when(bundle.getServiceReferences(CloudConnectionManager.class, null)).thenReturn(references);
        ComponentContext component = mock(ComponentContext.class);
        when(component.getBundleContext()).thenReturn(bundle);
        factory.activate(component);
    }

    @SuppressWarnings("unchecked")
    private ServiceReference<CloudConnectionManager> reference(Object pid, String factoryPid) {
        ServiceReference<CloudConnectionManager> reference = mock(ServiceReference.class);
        when(reference.getProperty(ConfigurationService.KURA_SERVICE_PID)).thenReturn(pid);
        when(reference.getProperty(ConfigurationAdmin.SERVICE_FACTORYPID)).thenReturn(factoryPid);
        return reference;
    }
}
