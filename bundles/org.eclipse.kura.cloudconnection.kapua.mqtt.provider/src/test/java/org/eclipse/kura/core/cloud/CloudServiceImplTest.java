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
 *******************************************************************************/
package org.eclipse.kura.core.cloud;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

import org.eclipse.kura.cloud.CloudClient;
import org.eclipse.kura.cloudconnection.request.RequestHandler;
import org.eclipse.kura.configuration.ConfigurationService;
import org.eclipse.kura.data.DataService;
import org.eclipse.kura.system.SystemService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;

class CloudServiceImplTest {
    private final CloudServiceImpl cloudService = new CloudServiceImpl();
    private final ComponentContext context = mock(ComponentContext.class);

    @BeforeEach
    void activate() {
        DataService dataService = mock(DataService.class);
        cloudService.setDataService(dataService);
        cloudService.setSystemService(mock(SystemService.class));
        BundleContext bundle = mock(BundleContext.class);
        when(bundle.registerService(anyString(), any(), any(Dictionary.class)))
                .thenReturn(mock(ServiceRegistration.class));
        when(context.getBundleContext()).thenReturn(bundle);
        Dictionary<String, Object> properties = new Hashtable<>();
        properties.put(ConfigurationService.KURA_SERVICE_PID, "test.cloud");
        when(context.getProperties()).thenReturn(properties);
        cloudService.activate(context, Map.of(ConfigurationService.KURA_SERVICE_PID, "test.cloud",
                "topic.control-prefix", "$TEST"));
    }

    @AfterEach
    void deactivate() {
        cloudService.deactivate(context);
    }

    @Test
    void testNewCloudClientDataServiceNotConnected() throws Exception {
        CloudClient client = cloudService.newCloudClient("testAPP");
        assertNotNull(client);
        assertEquals("testAPP", client.getApplicationId());
    }

    @Test
    void testGetCloudApplicationIdentifiersEmpty() {
        assertArrayEquals(new String[0], cloudService.getCloudApplicationIdentifiers());
    }

    @Test
    void testGetCloudApplicationIdentifiersOneCloudClient() throws Exception {
        cloudService.newCloudClient("testAPP");
        assertArrayEquals(new String[] {"testAPP"}, cloudService.getCloudApplicationIdentifiers());
    }

    @Test
    void testGetCloudApplicationIdentifiersOneRequestHandler() {
        cloudService.registerRequestHandler("testAPP", mock(RequestHandler.class));
        assertArrayEquals(new String[] {"testAPP"}, cloudService.getCloudApplicationIdentifiers());
    }

    @Test
    void testGetCloudServiceOptions() {
        CloudServiceOptions options = cloudService.getCloudServiceOptions();
        assertNotNull(options);
        assertEquals("$TEST", options.getTopicControlPrefix());
    }
}
