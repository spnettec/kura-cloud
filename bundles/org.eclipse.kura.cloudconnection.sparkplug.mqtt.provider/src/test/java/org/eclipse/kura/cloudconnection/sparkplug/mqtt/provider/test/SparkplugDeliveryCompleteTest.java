/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloudconnection.sparkplug.mqtt.provider.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;
import org.eclipse.kura.cloudconnection.sparkplug.mqtt.transport.SparkplugDataTransport;
import org.eclipse.kura.data.DataTransportToken;
import org.eclipse.kura.data.transport.listener.DataTransportListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Test;

class SparkplugDeliveryCompleteTest {
    @Test void confirmsDeliveredTokenAfterPahoClearsMessage() throws Exception {
        SparkplugDataTransport transport = new SparkplugDataTransport();
        DataTransportListener listener = mock(DataTransportListener.class);
        transport.addDataTransportListener(listener);
        IMqttDeliveryToken token = mock(IMqttDeliveryToken.class);
        when(token.getMessage()).thenReturn(null);
        when(token.getMessageId()).thenReturn(42);
        assertDoesNotThrow(() -> transport.deliveryComplete(token));
        verify(listener).onMessageConfirmed(new DataTransportToken(42, null));
    }

    @Test void ignoresQosZeroCallback() throws Exception {
        SparkplugDataTransport transport = new SparkplugDataTransport();
        DataTransportListener listener = mock(DataTransportListener.class);
        transport.addDataTransportListener(listener);
        IMqttDeliveryToken token = mock(IMqttDeliveryToken.class);
        MqttMessage message = new MqttMessage();
        message.setQos(0);
        when(token.getMessage()).thenReturn(message);
        transport.deliveryComplete(token);
        verifyNoInteractions(listener);
    }
}
