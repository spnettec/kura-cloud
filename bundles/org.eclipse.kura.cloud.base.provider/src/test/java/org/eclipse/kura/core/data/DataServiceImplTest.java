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
package org.eclipse.kura.core.data;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.eclipse.kura.KuraConnectException;
import org.eclipse.kura.KuraStoreException;
import org.eclipse.kura.data.listener.DataServiceListener;
import org.eclipse.kura.data.DataTransportService;
import org.eclipse.kura.data.DataTransportToken;
import org.eclipse.kura.message.store.StoredMessage;
import org.eclipse.kura.message.store.provider.MessageStore;
import org.eclipse.kura.message.store.provider.MessageStoreProvider;
import org.eclipse.kura.status.CloudConnectionStatusEnum;
import org.eclipse.kura.status.CloudConnectionStatusService;
import org.eclipse.kura.watchdog.WatchdogService;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;

class DataServiceImplTest {

    private final DataServiceImpl service = new DataServiceImpl();
    private final DataTransportService transport = mock(DataTransportService.class);
    private final CloudConnectionStatusService status = mock(CloudConnectionStatusService.class);
    private final WatchdogService watchdog = mock(WatchdogService.class);
    private final MessageStoreProvider provider = mock(MessageStoreProvider.class);
    private final MessageStore store = mock(MessageStore.class);
    private final ComponentContext context = mock(ComponentContext.class);
    private final DataServiceListener listener = mock(DataServiceListener.class);
    private final ExecutorService publisher = mock(ExecutorService.class);
    private final List<ScheduledExecutorService> schedulers = new ArrayList<>();
    private final List<Runnable> reconnectTasks = new ArrayList<>();
    private final List<ScheduledFuture<?>> reconnectFutures = new ArrayList<>();
    private final Map<String, Object> properties = new HashMap<>();
    private MockedStatic<Executors> executors;
    private boolean active;

    @BeforeEach
    void prepare() throws Exception {
        this.service.setDataTransportService(this.transport);
        this.service.setCloudConnectionStatusService(this.status);
        this.service.setWatchdogService(this.watchdog);
        when(this.context.getBundleContext()).thenReturn(mock(BundleContext.class));
        when(this.provider.openMessageStore(anyString())).thenReturn(this.store);
        when(this.store.getInFlightMessages()).thenReturn(List.of());
        when(this.store.getNextMessage()).thenReturn(Optional.empty());
        this.properties.put("kura.service.pid", "foo");
        this.properties.put("connect.auto-on-startup", false);
        this.properties.put("enable.recovery.on.connection.failure", true);
        this.executors = mockStatic(Executors.class, CALLS_REAL_METHODS);
        this.executors.when(Executors::newSingleThreadScheduledExecutor).thenAnswer(invocation -> scheduler());
        this.executors.when(() -> Executors.newSingleThreadExecutor(any(ThreadFactory.class))).thenReturn(this.publisher);
    }

    private ScheduledExecutorService scheduler() throws InterruptedException {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        when(scheduler.awaitTermination(anyLong(), any())).thenReturn(true);
        when(scheduler.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), eq(TimeUnit.SECONDS)))
                .thenAnswer(invocation -> {
                    ScheduledFuture<?> future = mock(ScheduledFuture.class);
                    this.reconnectTasks.add(invocation.getArgument(0));
                    this.reconnectFutures.add(future);
                    return future;
                });
        this.schedulers.add(scheduler);
        return scheduler;
    }

    @AfterEach
    void deactivate() {
        try {
            if (this.active) {
                this.service.deactivate(this.context);
                verify(this.publisher).shutdownNow();
                verify(this.schedulers.get(0)).shutdownNow();
                verify(this.schedulers.get(1)).shutdownNow();
            }
        } finally {
            this.executors.close();
        }
    }

    private void activate(boolean bindStore) throws Exception {
        this.service.activate(this.context, this.properties);
        this.active = true;
        this.service.addDataServiceListener(this.listener);
        if (bindStore) {
            this.service.setMessageStoreProvider(this.provider);
        }
    }

    @Test
    void shouldHandleMessageStoreConnectedEventWithDtDisconnected() throws Exception {
        this.properties.put("connect.auto-on-startup", true);
        activate(true);
        this.service.connected();
        assertEquals(2, this.reconnectTasks.size());
        verify(this.reconnectFutures.getFirst()).cancel(true);
    }

    @Test
    void shouldHandleMessageStoreDisconnectedEventWithDtConnected() throws Exception {
        when(this.transport.isConnected()).thenReturn(true);
        activate(true);
        this.service.disconnected();
        verify(this.transport).disconnect(anyLong());
        verify(this.store).close();
    }

    @Test
    void shouldNotAllowNegativePriority() throws Exception {
        activate(true);
        assertThrows(IllegalArgumentException.class, () -> this.service.publish("foo", new byte[4], 0, false, -1));
        verify(this.store, never()).store(anyString(), any(), anyInt(), anyBoolean(), anyInt());
    }

    @Test
    void shouldStoreMessagesWithNullPayload() throws Exception {
        assertStoredPayload(null);
    }

    @Test
    void shouldStoreMessagesWithPayloadSizeLessThanConfiguredThreshold() throws Exception {
        assertStoredPayload(new byte[] { 1, 2, 3 });
    }

    @Test
    void shouldStoreMessagesWithPayloadSizeEqualToConfiguredThreshold() throws Exception {
        assertStoredPayload(new byte[] { 1, 2, 3, 4 });
    }

    private void assertStoredPayload(byte[] payload) throws Exception {
        this.properties.put("maximum.payload.size", 4L);
        activate(true);
        when(this.store.store(anyString(), nullable(byte[].class), anyInt(), anyBoolean(), anyInt())).thenReturn(42);
        assertEquals(42, this.service.publish("foo", payload, 0, false, 9));
        verify(this.store).store("foo", payload, 0, false, 9);
    }

    @Test
    void shouldNotDisconnectOnConfigChange() throws Exception {
        this.properties.put("connect.auto-on-startup", true);
        activate(true);
        when(this.transport.isConnected()).thenReturn(true);
        this.properties.put("enable.recovery.on.connection.failure", true);
        this.service.updated(this.properties);
        verify(this.transport, never()).connect();
        verify(this.transport, never()).disconnect(anyLong());
        verify(this.status).updateStatus(this.service, CloudConnectionStatusEnum.SLOW_BLINKING);
    }

    @Test
    void shouldNotStoreMessagesWithPayloadSizeGreaterThanConfiguredThreshold() throws Exception {
        this.properties.put("maximum.payload.size", 4L);
        activate(true);
        KuraStoreException failure = assertThrows(KuraStoreException.class,
                () -> this.service.publish("foo", new byte[5], 0, false, 9));
        assertTrue(failure.getMessage().contains("size exceeds"));
        verify(this.store, never()).store(anyString(), any(), anyInt(), anyBoolean(), anyInt());
    }

    @Test
    void testStartDbStore() throws Exception {
        DataTransportToken token = new DataTransportToken(1234, "session");
        StoredMessage message = new StoredMessage.Builder(123).withTopic("stored.topic").withDataTransportToken(token).build();
        when(this.store.getInFlightMessages()).thenReturn(List.of(message));
        when(this.store.get(123)).thenReturn(Optional.of(message));
        activate(true);
        verify(this.provider).openMessageStore("foo");
        assertTrue(this.service.hasInFlightMessages());
        this.service.onMessageConfirmed(token);
        verify(this.store).markAsConfirmed(123);
        verify(this.listener).onMessageConfirmed(123, "stored.topic");
        assertFalse(this.service.hasInFlightMessages());
    }

    @Test
    void testConnectionEstablished() throws Exception {
        connectionEstablished(false, false);
    }

    @Test
    void testConnectionEstablishedErrorLog() throws Exception {
        connectionEstablished(false, true);
    }

    @Test
    void testConnectionEstablishedWithPublish() throws Exception {
        connectionEstablished(true, false);
    }

    @Test
    void testConnectionEstablishedWithPublishErrorLog() throws Exception {
        connectionEstablished(true, true);
    }

    private void connectionEstablished(boolean republish, boolean fail) throws Exception {
        this.properties.put("in-flight-messages.republish-on-new-session", republish);
        when(this.store.getInFlightMessages()).thenReturn(List.of(inFlightMessage()));
        activate(true);
        if (fail) {
            if (republish) {
                doThrow(new KuraStoreException("test")).when(this.store).unpublishAllInFlighMessages();
            } else {
                doThrow(new KuraStoreException("test")).when(this.store).dropAllInFlightMessages();
            }
        }
        this.service.onConnectionEstablished(true);
        verify(this.status).updateStatus(this.service, CloudConnectionStatusEnum.ON);
        verify(this.listener).onConnectionEstablished();
        if (republish) {
            verify(this.store).unpublishAllInFlighMessages();
        } else {
            verify(this.store).dropAllInFlightMessages();
        }
        assertEquals(fail, this.service.hasInFlightMessages());
        if (fail) {
            verify(this.transport).disconnect(anyLong());
        }
    }

    @Test
    void testActivateAndConnect() throws Exception {
        this.properties.put("connect.auto-on-startup", true);
        this.properties.put("connect.retry-interval", 2);
        activate(false);
        assertDoesNotThrow(this::runReconnect);
        verify(this.transport, never()).connect();
        this.service.setMessageStoreProvider(this.provider);
        RuntimeException stop = assertThrows(RuntimeException.class, this::runReconnect);
        assertTrue(stop.getMessage().contains("Connected"));
        verify(this.transport).connect();
        verify(this.watchdog).unregisterCriticalComponent(this.service);
    }

    @Test
    void testActivateAndFailConnecting() throws Exception {
        this.properties.put("connect.auto-on-startup", true);
        this.properties.put("connect.retry-interval", 1);
        this.properties.put("connection.recovery.max.failures", 4);
        activate(true);
        doThrow(connectFailure(MqttException.REASON_CODE_FAILED_AUTHENTICATION))
                .doThrow(connectFailure(MqttException.REASON_CODE_INVALID_CLIENT_ID))
                .doThrow(connectFailure(MqttException.REASON_CODE_NOT_AUTHORIZED))
                .doThrow(connectFailure(MqttException.REASON_CODE_BROKER_UNAVAILABLE))
                .doThrow(new KuraConnectException("ordinary failure")).when(this.transport).connect();
        for (int attempt = 0; attempt < 10; attempt++) {
            assertDoesNotThrow(this::runReconnect);
        }
        verify(this.transport, times(10)).connect();
        verify(this.watchdog, times(8)).checkin(this.service);
    }

    @Test
    void testMessageConfirmedNoMessageFound() throws Exception {
        activate(true);
        assertDoesNotThrow(() -> this.service.onMessageConfirmed(new DataTransportToken(1234, "session")));
        verify(this.store, never()).markAsConfirmed(anyInt());
        verify(this.listener, never()).onMessageConfirmed(anyInt(), anyString());
    }

    @Test
    void testMessageConfirmedConfirmedMessageException() throws Exception {
        when(this.store.getInFlightMessages()).thenReturn(List.of(inFlightMessage()));
        activate(true);
        doThrow(new KuraStoreException("test")).when(this.store).markAsConfirmed(123);
        this.service.onMessageConfirmed(new DataTransportToken(1234, "session"));
        verify(this.store).markAsConfirmed(123);
        verify(this.transport).disconnect(anyLong());
        verify(this.listener, never()).onMessageConfirmed(anyInt(), anyString());
    }

    @ParameterizedTest
    @ValueSource(strings = { "unpublished", "inflight", "dropped" })
    void shouldFilterMessageIdsWithinTheCorrectQueue(String queue) throws Exception {
        activate(false);
        when(this.store.getUnpublishedMessages()).thenReturn(List.of(message(1, "other"), message(2, "match")));
        when(this.store.getInFlightMessages()).thenReturn(List.of(message(3, "other"), message(4, "match")));
        when(this.store.getDroppedMessages()).thenReturn(List.of(message(5, "other"), message(6, "match")));
        this.service.setMessageStoreProvider(this.provider);
        switch (queue) {
        case "unpublished":
            assertEquals(List.of(2), this.service.getUnpublishedMessageIds("mat.*"));
            break;
        case "inflight":
            assertEquals(List.of(4), this.service.getInFlightMessageIds("mat.*"));
            break;
        case "dropped":
            assertEquals(List.of(6), this.service.getDroppedInFlightMessageIds("mat.*"));
            break;
        default:
            throw new AssertionError(queue);
        }
    }

    private static StoredMessage message(int id, String topic) {
        return new StoredMessage.Builder(id).withTopic(topic).build();
    }

    private static StoredMessage inFlightMessage() {
        return new StoredMessage.Builder(123).withDataTransportToken(new DataTransportToken(1234, "session")).build();
    }

    private static KuraConnectException connectFailure(int reason) {
        return new KuraConnectException(new MqttException(reason), "test");
    }

    private void runReconnect() {
        String originalName = Thread.currentThread().getName();
        try {
            this.reconnectTasks.getLast().run();
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }
}
