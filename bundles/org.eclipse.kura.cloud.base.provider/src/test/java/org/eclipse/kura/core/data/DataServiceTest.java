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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.eclipse.kura.data.listener.DataServiceListener;
import org.eclipse.kura.message.store.StoredMessage;
import org.eclipse.kura.message.store.provider.MessageStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(60)
class DataServiceTest extends BaseCloudTests {

    private final BlockingQueue<String> connections = new LinkedBlockingQueue<>();
    private final BlockingQueue<Published> published = new LinkedBlockingQueue<>();
    private final BlockingQueue<Published> confirmed = new LinkedBlockingQueue<>();
    private final BlockingQueue<Arrived> arrived = new LinkedBlockingQueue<>();
    private record Published(int id, String topic) { }
    private record Arrived(String topic, byte[] payload, int qos, boolean retained) { }

    @BeforeEach
    void listen() {
        this.connections.clear();
        this.published.clear();
        this.confirmed.clear();
        this.arrived.clear();
        DataServiceListener listener = mock(DataServiceListener.class);
        doAnswer(invocation -> this.connections.add("connected")).when(listener).onConnectionEstablished();
        doAnswer(invocation -> this.connections.add("disconnecting")).when(listener).onDisconnecting();
        doAnswer(invocation -> this.connections.add("disconnected")).when(listener).onDisconnected();
        doAnswer(invocation -> this.published.add(new Published(invocation.getArgument(0), invocation.getArgument(1))))
                .when(listener).onMessagePublished(anyInt(), anyString());
        doAnswer(invocation -> this.confirmed.add(new Published(invocation.getArgument(0), invocation.getArgument(1))))
                .when(listener).onMessageConfirmed(anyInt(), anyString());
        doAnswer(invocation -> this.arrived.add(new Arrived(invocation.getArgument(0), invocation.getArgument(1),
                invocation.getArgument(2), invocation.getArgument(3))))
                .when(listener).onMessageArrived(anyString(), any(byte[].class), anyInt(), anyBoolean());
        this.dataService.addDataServiceListener(listener);
    }

    @Test
    void testConnect() throws Exception {
        this.dataService.connect();
        assertTrue(this.dataService.isConnected());
        assertEquals("connected", take(this.connections));
    }

    @Test
    void testDisconnect() throws Exception {
        this.dataService.connect();
        assertEquals("connected", take(this.connections));
        this.dataService.disconnect(0);
        assertFalse(this.dataService.isConnected());
        assertEquals("disconnecting", take(this.connections));
        assertEquals("disconnected", take(this.connections));
        this.dataService.connect();
        assertTrue(this.dataService.isConnected());
        assertEquals("connected", take(this.connections));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2 })
    void testPublish(int qos) throws Exception {
        this.dataService.connect();
        Set<Integer> ids = new HashSet<>();
        byte[] payload = "Lorem ipsum 数据".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 100; i++) {
            assertTrue(ids.add(this.dataService.publish("test/publish", payload, qos, false, 5)));
        }
        Set<Integer> publishedIds = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Published event = take(this.published);
            assertEquals("test/publish", event.topic());
            assertTrue(publishedIds.add(event.id()));
        }
        assertEquals(ids, publishedIds);
        if (qos > 0) {
            Set<Integer> confirmedIds = new HashSet<>();
            for (int i = 0; i < 100; i++) {
                Published event = take(this.confirmed);
                assertEquals("test/publish", event.topic());
                assertTrue(confirmedIds.add(event.id()));
            }
            assertEquals(ids, confirmedIds);
        }
        MessageStore store = this.database.openMessageStore(DATA_PID);
        try {
            for (int id : ids) {
                StoredMessage stored = store.get(id).orElseThrow();
                assertArrayEquals(payload, stored.getPayload());
                assertEquals(qos, stored.getQos());
                assertTrue(stored.getPublishedOn().isPresent());
                if (qos > 0) {
                    assertTrue(stored.getConfirmedOn().isPresent());
                }
            }
        } finally {
            store.close();
        }
    }

    @Test
    void testQueuedPublishPriority() throws Exception {
        Set<Integer> high = new HashSet<>();
        Set<Integer> low = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            low.add(this.dataService.publish("test/low", new byte[] { 1 }, 1, false, 5));
        }
        for (int i = 0; i < 100; i++) {
            high.add(this.dataService.publish("test/high", new byte[] { 2 }, 1, false, 0));
        }
        assertEquals(200, this.dataService.getUnpublishedMessageIds("test/.*").size());
        Set<Integer> expectedConfirmations = new HashSet<>(high);
        expectedConfirmations.addAll(low);
        this.dataService.connect();
        for (int i = 0; i < 200; i++) {
            Published event = take(this.published);
            assertEquals(i < 100 ? "test/high" : "test/low", event.topic());
            assertTrue((i < 100 ? high : low).remove(event.id()));
        }
        assertTrue(high.isEmpty());
        assertTrue(low.isEmpty());
        Set<Integer> confirmations = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            assertTrue(confirmations.add(take(this.confirmed).id()));
        }
        assertEquals(expectedConfirmations, confirmations);
    }

    @Test
    void testSubscribe() throws Exception {
        this.dataService.connect();
        String topic = "#account-name/#client-id/test";
        this.dataService.subscribe(topic, 0);
        byte[] payload = "round-trip 数据".getBytes(StandardCharsets.UTF_8);
        this.dataService.publish(topic, payload, 0, false, 0);
        Arrived message = take(this.arrived);
        assertEquals("test-account/test-client/test", message.topic());
        assertArrayEquals(payload, message.payload());
        assertEquals(0, message.qos());
        assertFalse(message.retained());
        this.dataService.unsubscribe(topic);
    }

    private <T> T take(BlockingQueue<T> queue) throws InterruptedException {
        T event = queue.poll(10, TimeUnit.SECONDS);
        assertNotNull(event, "Timed out waiting for pipeline event");
        return event;
    }
}
