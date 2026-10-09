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
package org.eclipse.kura.core.cloud.publisher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.kura.KuraException;
import org.eclipse.kura.cloudconnection.message.KuraMessage;
import org.eclipse.kura.core.cloud.CloudServiceImpl;
import org.eclipse.kura.core.cloud.CloudServiceOptions;
import org.eclipse.kura.message.KuraPayload;
import org.eclipse.kura.system.SystemService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

public class NotificationPublisherImplTest {

    private static final String MESSAGE_TYPE_KEY = "messageType";

    private static final String REQUESTOR_CLIENT_ID_KEY = "requestorClientId";

    private static final String APP_ID_KEY = "appId";

    @Test
    public void testPublishNullCloudService() throws KuraException {
        NotificationPublisherImpl notificationPublisherImpl = new NotificationPublisherImpl(null);

        KuraException failure = assertThrows(KuraException.class,
                () -> notificationPublisherImpl.publish(new KuraMessage(null)));
        assertEquals(org.eclipse.kura.KuraErrorCode.SERVICE_UNAVAILABLE, failure.getCode());

    }

    @Test
    public void testPublishNullMessage() throws KuraException {
        CloudServiceImpl cloudServiceImpl = mock(CloudServiceImpl.class);
        NotificationPublisherImpl notificationPublisherImpl = new NotificationPublisherImpl(cloudServiceImpl);

        assertThrows(IllegalArgumentException.class, () -> notificationPublisherImpl.publish(null));

    }

    @Test
    public void testPublishNullProps() throws KuraException {
        CloudServiceImpl cloudServiceImpl = mock(CloudServiceImpl.class);
        NotificationPublisherImpl notificationPublisherImpl = new NotificationPublisherImpl(cloudServiceImpl);

        KuraPayload payload = new KuraPayload();
        KuraMessage message = new KuraMessage(payload);
        assertThrows(IllegalArgumentException.class, () -> notificationPublisherImpl.publish(message));

    }

    @Test
    public void testPublish() throws KuraException {
        CloudServiceImpl cloudServiceImpl = mock(CloudServiceImpl.class);
        SystemService systemService = mock(SystemService.class);

        Map<String, Object> optionsProps = new HashMap<>();
        CloudServiceOptions options = new CloudServiceOptions(optionsProps, systemService);

        when(cloudServiceImpl.getCloudServiceOptions()).thenReturn(options);
        when(cloudServiceImpl.publish(ArgumentMatchers.any())).thenReturn("1");

        NotificationPublisherImpl notificationPublisherImpl = new NotificationPublisherImpl(cloudServiceImpl);

        Map<String, Object> properties = new HashMap<>();
        properties.put(APP_ID_KEY, "appId");
        properties.put(MESSAGE_TYPE_KEY, "messageType");
        properties.put(REQUESTOR_CLIENT_ID_KEY, "requestorClientId");
        KuraPayload payload = new KuraPayload();
        KuraMessage message = new KuraMessage(payload, properties);
        String messageId = notificationPublisherImpl.publish(message);

        assertNotNull(messageId);
        assertEquals("1", messageId);
        ArgumentCaptor<KuraMessage> sent = ArgumentCaptor.forClass(KuraMessage.class);
        verify(cloudServiceImpl).publish(sent.capture());
        assertSame(payload, sent.getValue().getPayload());
        assertEquals(Map.of("FULL_TOPIC", "$EDC/#account-name/requestorClientId/appId/NOTIFY/#client-id/messageType",
                "QOS", 0, "RETAIN", false, "PRIORITY", 1, "CONTROL", true), sent.getValue().getProperties());
    }

}
