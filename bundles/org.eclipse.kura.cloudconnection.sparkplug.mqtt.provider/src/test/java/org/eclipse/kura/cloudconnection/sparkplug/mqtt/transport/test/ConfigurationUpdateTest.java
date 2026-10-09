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
package org.eclipse.kura.cloudconnection.sparkplug.mqtt.transport.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.Map;

import org.eclipse.kura.data.transport.listener.DataTransportListener;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.transport.SparkplugDataTransport;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.transport.SparkplugDataTransportOptions;
import org.eclipse.kura.ssl.SslManagerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class ConfigurationUpdateTest {

    private final SparkplugDataTransport transport = new SparkplugDataTransport();
    private final DataTransportListener listener = mock(DataTransportListener.class);
    private final SslManagerService sslManagerService = mock(SslManagerService.class);

    @BeforeEach
    void setUp() {
        this.transport.addDataTransportListener(this.listener);
    }

    @AfterEach
    void tearDown() {
        this.transport.deactivate();
        this.transport.removeDataTransportListener(this.listener);
    }

    @Test
    void shouldUpdateConfigurationOnSetSslManager() {
        updateConfiguration();
        this.transport.setSslManagerService(this.sslManagerService);

        assertConfigurationUpdates(2);
        assertEquals("test.client", this.transport.getClientId());
        assertFalse(this.transport.isConnected());
    }

    @Test
    void shouldNotUpdateConfigurationOnNewSslManagerButNullOptions() {
        this.transport.setSslManagerService(this.sslManagerService);

        verifyNoInteractions(this.listener);
    }

    @Test
    void shouldUpdateConfigurationOnUnsetSslManager() {
        this.transport.setSslManagerService(this.sslManagerService);
        updateConfiguration();
        this.transport.unsetSslManagerService(this.sslManagerService);

        assertConfigurationUpdates(2);
    }

    @Test
    void shouldNotThrowExceptionsOnDisconnectWithUnconfiguredService() {
        assertDoesNotThrow(() -> this.transport.disconnect(0));
        verifyNoInteractions(this.listener);
    }

    private void updateConfiguration() {
        this.transport.update(Map.of(
                SparkplugDataTransportOptions.KEY_CLIENT_ID, "test.client",
                SparkplugDataTransportOptions.KEY_CONNECTION_TIMEOUT, 30,
                SparkplugDataTransportOptions.KEY_GROUP_ID, "g1",
                SparkplugDataTransportOptions.KEY_NODE_ID, "n1",
                SparkplugDataTransportOptions.KEY_PRIMARY_HOST_APPLICATION_ID, "",
                SparkplugDataTransportOptions.KEY_SERVER_URIS, "tcp://localhost:1883",
                SparkplugDataTransportOptions.KEY_KEEP_ALIVE, 60));
    }

    private void assertConfigurationUpdates(int count) {
        InOrder updates = inOrder(this.listener);
        for (int i = 0; i < count; i++) {
            updates.verify(this.listener).onConfigurationUpdating(false);
            updates.verify(this.listener).onConfigurationUpdated(false);
        }
        updates.verifyNoMoreInteractions();
    }
}
