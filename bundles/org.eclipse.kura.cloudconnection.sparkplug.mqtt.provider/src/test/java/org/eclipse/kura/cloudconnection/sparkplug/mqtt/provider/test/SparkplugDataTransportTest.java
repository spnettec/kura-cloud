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
 ******************************************************************************/
package org.eclipse.kura.cloudconnection.sparkplug.mqtt.provider.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;

import org.eclipse.kura.KuraConnectException;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.message.SparkplugBProtobufPayloadBuilder;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.message.SparkplugPayloads;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.message.SparkplugTopics;
import org.eclipse.kura.data.transport.listener.DataTransportListener;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.tahu.protobuf.SparkplugBProto.Payload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SparkplugDataTransportTest extends SparkplugIntegrationTest {

    private DataTransportListener listener;
    private MqttCallback callback;

    @BeforeEach
    void observeMqtt() throws Exception {
        this.listener = mock(DataTransportListener.class);
        this.callback = mock(MqttCallback.class);
        this.transport.addDataTransportListener(this.listener);
        this.peer.setCallback(this.callback);
        this.peer.subscribe("spBv1.0/#", 1);
    }

    @Test
    void shouldSendNodeBirthWithoutPrimaryHost() throws Exception {
        connect("");
        verify(this.listener).onConnectionEstablished(true);
        assertTrue(this.transport.isConnected());
        assertBirth(0, 1);
    }

    @Test
    void shouldNotSendNodeBirthWithPrimaryHostOffline() throws Exception {
        connect("h1");
        state("h1", false, 1000);
        verify(this.listener, never()).onConnectionEstablished(true);
        assertFalse(this.transport.isConnected());
    }

    @Test
    void shouldSendNodeBirthWithPrimaryHostOnline() throws Exception {
        connect("h1");
        state("h1", true, 1000);
        verify(this.listener).onConnectionEstablished(true);
        assertTrue(this.transport.isConnected());
        assertBirth(0, 1);
    }

    @Test
    void shouldDisconnectCleanWhenPrimaryHostOffline() throws Exception {
        connect("h1");
        state("h1", true, 1000);
        state("h1", false, 2000);
        verify(this.listener).onDisconnecting();
        verify(this.listener).onDisconnected();
        assertFalse(this.transport.isConnected());
        assertDeath(0, 1);
    }

    @Test
    void shouldIgnoreStateMessagesWithOutdatedTimestamp() throws Exception {
        connect("h1");
        state("h1", true, 2000);
        state("h1", false, 1000);
        assertStillConnected();
    }

    @Test
    void shouldIgnoreStateMessagesWithoutPrimaryHost() throws Exception {
        connect("");
        // Explicit subscription ensures the message reaches the production dispatcher.
        this.transport.subscribe(SparkplugTopics.getStateTopic("h1"), 1);
        state("h1", false, 1000);
        assertStillConnected();
    }

    @Test
    void shouldIgnoreStateMessagesFromOtherPrimaryHost() throws Exception {
        connect("h1");
        state("h1", true, 1000);
        this.transport.subscribe(SparkplugTopics.getStateTopic("h2"), 1);
        state("h2", false, 2000);
        assertStillConnected();
    }

    @Test
    void shouldDisconnectCleanWithDeathCertificate() throws Exception {
        connect("");
        this.transport.disconnect(0);
        verify(this.listener).onDisconnecting();
        verify(this.listener).onDisconnected();
        assertFalse(this.transport.isConnected());
        assertDeath(0, 1);
    }

    @Test
    void shouldReestablishSessionWhenRebirthRequestArrives() throws Exception {
        connect("");
        rebirth("g1", "n1", true);
        verify(this.listener).onDisconnecting();
        verify(this.listener).onDisconnected();
        verify(this.listener, times(2)).onConnectionEstablished(true);
        assertDeath(0, 1);
        assertBirth(0, 2);
    }

    @Test
    void shouldIgnoreRebirthWhenDifferentNode() throws Exception {
        connect("");
        this.transport.subscribe(SparkplugTopics.getNodeCommandTopic("g1", "n2"), 0);
        rebirth("g1", "n2", true);
        assertStillConnected();
    }

    @Test
    void shouldIgnoreRebirthWhenDifferentGroup() throws Exception {
        connect("");
        this.transport.subscribe(SparkplugTopics.getNodeCommandTopic("g2", "n1"), 0);
        rebirth("g2", "n1", true);
        assertStillConnected();
    }

    @Test
    void shouldIgnoreRebirthWhenMetricIsFalse() throws Exception {
        connect("");
        rebirth("g1", "n1", false);
        assertStillConnected();
    }

    @Test
    void shouldForwardStateMessagesToListeners() throws Exception {
        connect("h1");
        state("h1", false, 1000);
        verify(this.listener).onMessageArrived(eq(SparkplugTopics.getStateTopic("h1")),
                any(byte[].class), eq(1), eq(false));
    }

    @Test
    void shouldForwardNodeCommandMessagesToListeners() throws Exception {
        connect("");
        rebirth("g1", "n1", false);
        verify(this.listener).onMessageArrived(eq(SparkplugTopics.getNodeCommandTopic("g1", "n1")),
                any(byte[].class), eq(0), eq(false));
    }

    @Test
    void shouldIncrementBdSeqOnSuccessfulReconnection() throws Exception {
        connect("");
        this.transport.disconnect(0);
        connectTransport();
        assertBirth(0, 1);
        assertDeath(0, 1);
        assertBirth(1, 1);
    }

    @Test
    void shouldNotIncrementBdSeqOnUnsuccessfulConnection() throws Exception {
        configureTransport("", "tcp://127.0.0.1:0");
        assertThrows(KuraConnectException.class, this::connectTransport);
        connect("");
        assertBirth(0, 1);
    }

    private void connect(String primaryHost) throws KuraConnectException {
        configureTransport(primaryHost);
        connectTransport();
    }

    private void state(String host, boolean online, long timestamp) throws Exception {
        String topic = SparkplugTopics.getStateTopic(host);
        byte[] payload = ("{\"online\":" + online + ",\"timestamp\":" + timestamp + "}")
                .getBytes(StandardCharsets.UTF_8);
        this.peer.publish(topic, payload, 1, false);
        awaitProcessing(topic);
    }

    private void rebirth(String group, String node, boolean value) throws Exception {
        String topic = SparkplugTopics.getNodeCommandTopic(group, node);
        byte[] payload = new SparkplugBProtobufPayloadBuilder()
                .withMetric(SparkplugPayloads.NODE_CONTROL_REBIRTH_METRIC_NAME, value, 1000).build();
        this.peer.publish(topic, payload, 0, false);
        awaitProcessing(topic);
    }

    private void assertStillConnected() {
        assertTrue(this.transport.isConnected());
        verify(this.listener, never()).onDisconnecting();
        verify(this.listener, never()).onDisconnected();
        verify(this.listener).onConnectionEstablished(true);
    }

    private void assertBirth(long bdSeq, int count) throws Exception {
        assertMessage("spBv1.0/g1/NBIRTH/n1", bdSeq, count);
    }

    private void assertDeath(long bdSeq, int count) throws Exception {
        assertMessage("spBv1.0/g1/NDEATH/n1", bdSeq, count);
    }

    private void assertMessage(String topic, long bdSeq, int count) throws Exception {
        verify(this.callback, timeout(5000).times(count)).messageArrived(eq(topic), argThat(message -> {
            try {
                Payload payload = Payload.parseFrom(message.getPayload());
                return message.getQos() == 0 && !message.isRetained()
                        && payload.getMetricsList().stream().anyMatch(metric -> metric.getName().equals("bdSeq")
                                && metric.getLongValue() == bdSeq);
            } catch (Exception error) {
                return false;
            }
        }));
    }
}
