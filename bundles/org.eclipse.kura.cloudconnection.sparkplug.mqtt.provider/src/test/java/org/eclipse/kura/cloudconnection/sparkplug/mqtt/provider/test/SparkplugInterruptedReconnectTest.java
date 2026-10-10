/*******************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *******************************************************************************/
package org.eclipse.kura.cloudconnection.sparkplug.mqtt.provider.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.kura.KuraConnectException;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.transport.SparkplugDataTransport;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.transport.SparkplugMqttClient;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SparkplugInterruptedReconnectTest extends SparkplugIntegrationTest {

    private record Attempt(Throwable failure, boolean interrupted) {
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldAbortAlreadyInterruptedConnection(boolean previouslyConnected) throws Exception {
        configureTransport("");
        if (previouslyConnected) {
            connectTransport();
            assertTrue(this.transport.isConnected());
            this.transport.disconnect(0);
        }
        SparkplugMqttClient session = session();
        MqttAsyncClient previous = networkClient(session);
        AtomicReference<Attempt> outcome = new AtomicReference<>();
        Thread worker = Thread.ofVirtual().unstarted(() -> connect(outcome, true));
        try {
            worker.start();
            worker.join(10_000);
            assertFalse(worker.isAlive(), "Interrupted connect must finish");
            assertCancelled(session, previous, outcome.get());
        } finally {
            stopWorker(worker);
            closeNetworkClients(session, previous);
        }
    }

    @Test
    void shouldAbortWhenInterruptedDuringReconnectDelay() throws Exception {
        configureTransport("");
        connectTransport();
        assertTrue(this.transport.isConnected());
        this.transport.disconnect(0);
        SparkplugMqttClient session = session();
        MqttAsyncClient previous = networkClient(session);
        CountDownLatch delayRequested = new CountDownLatch(1);
        setField(session, "randomDelayGenerator", new Random() {
            private static final long serialVersionUID = 1L;

            @Override
            public int nextInt(int bound) {
                delayRequested.countDown();
                return bound - 1;
            }
        });
        AtomicReference<Attempt> outcome = new AtomicReference<>();
        Thread worker = Thread.ofVirtual().unstarted(() -> connect(outcome, false));
        try {
            worker.start();
            assertTrue(delayRequested.await(5, TimeUnit.SECONDS), "Production reconnect delay must be reached");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (worker.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(1);
            }
            assertSame(Thread.State.TIMED_WAITING, worker.getState(), "Interrupt the actual Thread.sleep");
            worker.interrupt();
            worker.join(10_000);
            assertFalse(worker.isAlive(), "Cancelled reconnect must finish");
            assertCancelled(session, previous, outcome.get());
        } finally {
            stopWorker(worker);
            closeNetworkClients(session, previous);
        }
    }

    private void connect(AtomicReference<Attempt> outcome, boolean alreadyInterrupted) {
        if (alreadyInterrupted) {
            Thread.currentThread().interrupt();
        }
        Throwable failure = null;
        try {
            this.transport.connect();
        } catch (Throwable error) {
            failure = error;
        } finally {
            outcome.set(new Attempt(failure, Thread.currentThread().isInterrupted()));
        }
    }

    private void assertCancelled(SparkplugMqttClient session, MqttAsyncClient previous, Attempt attempt)
            throws Exception {
        assertSame(previous, networkClient(session), "Cancellation must not create or replace the Paho client");
        KuraConnectException failure = assertInstanceOf(KuraConnectException.class, attempt.failure());
        assertInstanceOf(InterruptedException.class, failure.getCause());
        assertTrue(attempt.interrupted(), "Preserve the interrupt for the executor's cancellation contract");
        assertFalse(this.transport.isConnected());
        assertTrue(this.peer.isConnected(), "The independent broker observer remains connected");
    }

    private SparkplugMqttClient session() throws Exception {
        Field field = SparkplugDataTransport.class.getDeclaredField("client");
        field.setAccessible(true);
        return (SparkplugMqttClient) field.get(this.transport);
    }

    private static MqttAsyncClient networkClient(SparkplugMqttClient session) throws Exception {
        Field field = SparkplugMqttClient.class.getDeclaredField("client");
        field.setAccessible(true);
        return (MqttAsyncClient) field.get(session);
    }

    private static void setField(SparkplugMqttClient session, String name, Object value) throws Exception {
        Field field = SparkplugMqttClient.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(session, value);
    }

    private static void stopWorker(Thread worker) throws InterruptedException {
        if (worker.isAlive()) {
            worker.interrupt();
            worker.join(10_000);
        }
        assertFalse(worker.isAlive(), "Owned reconnect worker must stop");
    }

    private static void closeNetworkClients(SparkplugMqttClient session, MqttAsyncClient previous) throws Exception {
        MqttAsyncClient current = networkClient(session);
        try {
            if (current != null) {
                current.disconnectForcibly(0, 1_000, false);
                current.close(true);
            }
        } finally {
            if (previous != null && previous != current) {
                previous.disconnectForcibly(0, 1_000, false);
                previous.close(true);
            }
        }
    }
}
