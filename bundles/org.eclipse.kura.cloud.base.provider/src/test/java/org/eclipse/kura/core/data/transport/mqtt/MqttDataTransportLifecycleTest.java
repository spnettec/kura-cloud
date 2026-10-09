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
package org.eclipse.kura.core.data.transport.mqtt;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.kura.status.CloudConnectionStatusService;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.osgi.service.component.ComponentContext;

class MqttDataTransportLifecycleTest {

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void deactivationClosesOwnedClientEvenAfterExplicitDisconnect(boolean disconnectFirst) throws Exception {
        AtomicBoolean connected = new AtomicBoolean();
        IMqttToken token = mock(IMqttToken.class);
        try (MockedConstruction<MqttAsyncClient> clients = mockConstruction(MqttAsyncClient.class, (client, context) -> {
            when(client.isConnected()).thenAnswer(invocation -> connected.get());
            when(client.connect(any(MqttConnectOptions.class))).thenAnswer(invocation -> {
                connected.set(true);
                return token;
            });
            when(client.disconnect(anyLong())).thenAnswer(invocation -> {
                connected.set(false);
                return token;
            });
            doAnswer(invocation -> {
                connected.set(false);
                return null;
            }).when(client).disconnectForcibly(anyLong(), anyLong());
        })) {
            MqttDataTransport transport = new MqttDataTransport();
            ComponentContext context = mock(ComponentContext.class);
            transport.setCloudConnectionStatusService(mock(CloudConnectionStatusService.class));
            transport.activate(context, Map.of("broker-url", "mqtt://127.0.0.1:1", "client-id", "test",
                    "keep-alive", 30, "timeout", 2, "clean-session", true, "protocol-version", 4,
                    "in-flight.persistence", "memory"));
            transport.connect();
            if (disconnectFirst) {
                transport.disconnect(0);
            }
            transport.deactivate(context);
            transport.deactivate(context);
            MqttAsyncClient client = clients.constructed().getFirst();
            verify(client).close();
            verify(client).setCallback(null);
        }
    }
}
