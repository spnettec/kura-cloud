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
 *******************************************************************************/
package org.eclipse.kura.cloudconnection.sparkplug.mqtt.factory.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.kura.cloudconnection.sparkplug.mqtt.factory.SparkplugCloudConnectionFactory;
import org.eclipse.kura.configuration.ComponentConfiguration;
import org.eclipse.kura.configuration.ConfigurationService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

class SparkplugCloudConnectionFactoryTest {

    private static final String CLOUD =
            "org.eclipse.kura.cloudconnection.sparkplug.mqtt.endpoint.SparkplugCloudEndpoint";
    private static final String DATA = "org.eclipse.kura.data.DataService";
    private static final String TRANSPORT =
            "org.eclipse.kura.cloudconnection.sparkplug.mqtt.transport.SparkplugDataTransport";
    private final ConfigurationService configurations = mock(ConfigurationService.class);
    private final SparkplugCloudConnectionFactory factory = new SparkplugCloudConnectionFactory();

    SparkplugCloudConnectionFactoryTest() {
        this.factory.setConfigurationService(this.configurations);
    }

    @Test
    void shouldReturnCorrectFactoryPid() {
        assertEquals(CLOUD, this.factory.getFactoryPid());
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "-test" })
    void shouldCreateLinkedStackWithOrWithoutEndpointSuffix(String suffix) throws Exception {
        assertCreatedStack(CLOUD + suffix, null, null);
    }

    @Test
    void shouldPreserveCustomPidAndLocalizedMetadata() throws Exception {
        assertCreatedStack("custom.sparkplug", "云连接", "工厂描述");
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldGenerateEndpointPidWhenMissing(String pid) throws Exception {
        assertCreatedStack(pid, null, null);
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "-test" })
    void shouldResolveStackFromConfiguredTargets(String suffix) throws Exception {
        linkedStack(CLOUD + suffix);
        // The local factory returns the linked data and transport PIDs, excluding the endpoint.
        assertEquals(List.of("custom.data", "custom.transport"), this.factory.getStackComponentsPids(CLOUD + suffix));
    }

    @Test
    void shouldDeleteLinkedStackWithOneFinalSnapshot() throws Exception {
        linkedStack(CLOUD + "-test");
        this.factory.deleteConfiguration(CLOUD + "-test");

        InOrder order = inOrder(this.configurations);
        order.verify(this.configurations).deleteFactoryConfiguration(CLOUD + "-test", false);
        order.verify(this.configurations).deleteFactoryConfiguration("custom.data", false);
        order.verify(this.configurations).deleteFactoryConfiguration("custom.transport", true);
        order.verifyNoMoreInteractions();
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
                assertTrue(servicePid.matches(TRANSPORT.replace(".", "\\.") + "-[0-9]+"));
                assertNull(properties);
                assertFalse(snapshot);
            } else if (DATA.equals(factoryPid)) {
                assertTrue(servicePid.matches(DATA.replace(".", "\\.") + "-[0-9]+"));
                assertEquals(Map.of("DataTransportService.target", target(pids.get(0))), properties);
                assertFalse(snapshot);
            } else {
                if (pid == null || pid.isEmpty()) {
                    assertTrue(servicePid.matches(CLOUD.replace(".", "\\.") + "-Cloud-[0-9]+"));
                } else {
                    assertEquals(pid, servicePid);
                }
                Map<String, Object> expected = new HashMap<>();
                expected.put("DataService.target", target(pids.get(1)));
                expected.put("DataTransportService.target", target(pids.get(0)));
                if (name != null) {
                    expected.put(ConfigurationService.KURA_CLOUD_FACTORY_NAME, name);
                    expected.put(ConfigurationService.KURA_CLOUD_FACTORY_DESC, description);
                }
                assertEquals(expected, properties);
                assertTrue(snapshot);
            }
            return null;
        }).when(this.configurations).createFactoryConfiguration(anyString(), anyString(), any(), anyBoolean());
        if (name == null) {
            this.factory.createConfiguration(pid);
        } else {
            this.factory.createConfiguration(pid, name, description);
        }
        assertEquals(List.of(TRANSPORT, DATA, CLOUD), factories);
    }

    private void linkedStack(String pid) throws Exception {
        configuration(pid, Map.of("DataService.target", target("custom.data")));
        configuration("custom.data", Map.of("DataTransportService.target", target("custom.transport")));
    }

    private void configuration(String pid, Map<String, Object> properties) throws Exception {
        ComponentConfiguration configuration = mock(ComponentConfiguration.class);
        when(configuration.getConfigurationProperties()).thenReturn(properties);
        when(this.configurations.getComponentConfiguration(pid)).thenReturn(configuration);
    }

    private static String target(String pid) {
        return "(" + ConfigurationService.KURA_SERVICE_PID + "=" + pid + ")";
    }
}
