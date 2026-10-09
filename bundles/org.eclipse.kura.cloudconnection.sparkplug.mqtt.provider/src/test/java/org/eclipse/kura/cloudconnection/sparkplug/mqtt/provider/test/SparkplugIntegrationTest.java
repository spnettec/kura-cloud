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
package org.eclipse.kura.cloudconnection.sparkplug.mqtt.provider.test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.eclipse.kura.KuraConnectException;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.transport.SparkplugDataTransport;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.transport.SparkplugDataTransportOptions;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import io.moquette.broker.Server;

/** Real loopback MQTT fixture. Equinox/SCR assembly is outside this fixture. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class SparkplugIntegrationTest {

    private Server broker;
    protected String brokerUri;
    protected SparkplugDataTransport transport;
    protected MqttClient peer;
    private List<ExecutorService> dispatchers;
    private BlockingQueue<Processing> processing;

    private record Processing(String topic, Future<?> completion) {
    }

    @BeforeAll
    void startBroker() throws Exception {
        Properties options = new Properties();
        options.setProperty("host", "127.0.0.1");
        options.setProperty("port", "0");
        options.setProperty("allow_anonymous", "true");
        options.setProperty("persistence_enabled", "false");
        options.setProperty("telemetry_enabled", "false");
        this.broker = new Server();
        this.broker.startServer(options);
        this.brokerUri = "tcp://127.0.0.1:" + awaitBrokerPort();
    }

    private int awaitBrokerPort() throws InterruptedException {
        // Moquette 0.18 completes its port-map listener asynchronously after bind.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            try {
                int port = this.broker.getPort();
                if (port > 0) {
                    return port;
                }
            } catch (ConcurrentModificationException bindingInProgress) {
                // Retry only this third-party startup race, never test assertions.
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }
        throw new IllegalStateException("Moquette did not report its bound loopback port");
    }

    @AfterAll
    void stopBroker() {
        if (this.broker != null) {
            this.broker.stopServer();
        }
    }

    @BeforeEach
    void createMqttClients() throws Exception {
        this.dispatchers = new ArrayList<>();
        this.processing = new LinkedBlockingQueue<>();
        this.transport = spy(new SparkplugDataTransport());
        doAnswer(invocation -> {
            invocation.callRealMethod();
            Future<?> completed = this.dispatchers.getLast().submit(() -> { });
            this.processing.add(new Processing(invocation.getArgument(0), completed));
            return null;
        }).when(this.transport).messageArrived(anyString(), any(MqttMessage.class));
        this.peer = new MqttClient(this.brokerUri, "sparkplug-test-peer", new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(false);
        options.setCleanSession(true);
        options.setConnectionTimeout(5);
        this.peer.connect(options);
    }

    @AfterEach
    void closeMqttClients() throws Exception {
        try {
            if (this.transport != null) {
                this.transport.deactivate();
            }
        } finally {
            try {
                if (this.peer != null) {
                    if (this.peer.isConnected()) {
                        this.peer.disconnect();
                    }
                    this.peer.close();
                }
            } finally {
                for (ExecutorService dispatcher : this.dispatchers) {
                    dispatcher.shutdownNow();
                    assertTrue(dispatcher.awaitTermination(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    protected void configureTransport(String primaryHost) {
        configureTransport(primaryHost, this.brokerUri);
    }

    protected void configureTransport(String primaryHost, String uri) {
        // Remove random reconnect jitter only; actual Paho sockets and MQTT callbacks remain real.
        try (MockedConstruction<Random> random = mockConstruction(Random.class,
                (mock, context) -> when(mock.nextInt(anyInt())).thenReturn(0))) {
            this.transport.update(Map.of(SparkplugDataTransportOptions.KEY_GROUP_ID, "g1",
                    SparkplugDataTransportOptions.KEY_NODE_ID, "n1",
                    SparkplugDataTransportOptions.KEY_PRIMARY_HOST_APPLICATION_ID, primaryHost,
                    SparkplugDataTransportOptions.KEY_SERVER_URIS, uri,
                    SparkplugDataTransportOptions.KEY_CLIENT_ID, "sparkplug-test-node",
                    SparkplugDataTransportOptions.KEY_USERNAME, "mqtt",
                    SparkplugDataTransportOptions.KEY_KEEP_ALIVE, 60,
                    SparkplugDataTransportOptions.KEY_CONNECTION_TIMEOUT, 5));
        }
    }

    protected void connectTransport() throws KuraConnectException {
        try (MockedStatic<Executors> factories = mockStatic(Executors.class, CALLS_REAL_METHODS)) {
            factories.when(() -> Executors.newSingleThreadExecutor(any(ThreadFactory.class)))
                    .thenAnswer(invocation -> {
                        ExecutorService dispatcher = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                                new LinkedBlockingQueue<>(), (ThreadFactory) invocation.getArgument(0));
                        this.dispatchers.add(dispatcher);
                        return dispatcher;
                    });
            this.transport.connect();
        }
    }

    protected void awaitProcessing(String topic) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            Processing event = this.processing.poll(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            if (event == null) {
                break;
            }
            event.completion().get(5, TimeUnit.SECONDS);
            if (event.topic().equals(topic)) {
                return;
            }
        }
        fail("No completed transport dispatch for " + topic);
    }
}
