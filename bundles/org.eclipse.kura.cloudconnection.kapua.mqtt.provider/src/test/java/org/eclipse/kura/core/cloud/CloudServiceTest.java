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
 ******************************************************************************/
package org.eclipse.kura.core.cloud;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.kura.KuraConnectException;
import org.eclipse.kura.KuraException;
import org.eclipse.kura.KuraErrorCode;
import org.eclipse.kura.configuration.ConfigurationService;
import org.eclipse.kura.core.data.DataServiceImpl;
import org.eclipse.kura.internal.json.marshaller.unmarshaller.message.CloudPayloadJsonEncoder;
import org.eclipse.kura.marshalling.Marshaller;
import org.eclipse.kura.message.KuraBirthPayload;
import org.eclipse.kura.message.KuraDeviceProfile;
import org.eclipse.kura.message.KuraPayload;
import org.eclipse.kura.net.status.NetworkStatusService;
import org.eclipse.kura.net.status.modem.ModemConnectionStatus;
import org.eclipse.kura.net.status.modem.ModemInterfaceStatus;
import org.eclipse.kura.net.status.modem.Sim;
import org.eclipse.kura.security.tamper.detection.TamperDetectionService;
import org.eclipse.kura.security.tamper.detection.TamperEvent;
import org.eclipse.kura.security.tamper.detection.TamperStatus;
import org.eclipse.kura.system.ExtendedProperties;
import org.eclipse.kura.system.ExtendedPropertyGroup;
import org.eclipse.kura.system.SystemAdminService;
import org.eclipse.kura.system.SystemService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.event.EventAdmin;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;

class CloudServiceTest {
    private final DataServiceImpl data = mock(DataServiceImpl.class);
    private final ComponentContext context = mock(ComponentContext.class);
    private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    private final ExecutorService acknowledgements = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
    private final AtomicReference<byte[]> lastPayload = new AtomicReference<>();
    private CloudServiceImpl cloud;
    private Runnable delayedPublish;

    @BeforeEach
    void activate() throws Exception {
        try (MockedStatic<Executors> factories = mockStatic(Executors.class, CALLS_REAL_METHODS)) {
            factories.when(Executors::newSingleThreadScheduledExecutor).thenReturn(scheduler);
            cloud = new CloudServiceImpl();
        }
        when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(call -> {
            assertEquals(30L, call.getArgument(1, Long.class));
            assertEquals(TimeUnit.SECONDS, call.getArgument(2));
            delayedPublish = call.getArgument(0);
            return mock(ScheduledFuture.class);
        });
        when(data.publish(anyString(), any(), anyInt(), anyBoolean(), anyInt())).thenAnswer(call -> {
            assertEquals("EDC/#account-name/#client-id/MQTT/BIRTH", call.getArgument(0));
            assertEquals(1, call.getArgument(2, Integer.class));
            assertEquals(false, call.getArgument(3));
            assertEquals(0, call.getArgument(4, Integer.class));
            lastPayload.set(call.getArgument(1));
            String topic = call.getArgument(0);
            acknowledgements.execute(() -> cloud.onMessagePublished(12, topic));
            return 12;
        });
        BundleContext bundle = mock(BundleContext.class);
        when(bundle.registerService(anyString(), any(), any(Dictionary.class))).thenReturn(mock(ServiceRegistration.class));
        when(context.getBundleContext()).thenReturn(bundle);
        Dictionary<String, Object> properties = new Hashtable<>();
        properties.put(ConfigurationService.KURA_SERVICE_PID, "test.cloud");
        when(context.getProperties()).thenReturn(properties);
        cloud.setDataService(data);
        cloud.setSystemService(system(Optional.empty()));
        SystemAdminService admin = mock(SystemAdminService.class);
        when(admin.getUptime()).thenReturn("1 day");
        cloud.setSystemAdminService(admin);
        cloud.setEventAdmin(mock(EventAdmin.class));
        cloud.setJsonMarshaller(new Marshaller() {
            @Override
            public String marshal(Object value) throws KuraException {
                StringWriter writer = new StringWriter();
                try {
                    CloudPayloadJsonEncoder.marshal(writer, (KuraPayload) value);
                    return writer.toString();
                } catch (IOException e) {
                    throw new KuraException(KuraErrorCode.ENCODE_ERROR, e);
                }
            }
            @Override
            public void marshal(OutputStream out, Object value) throws KuraException {
                try {
                    out.write(marshal(value).getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    throw new KuraException(KuraErrorCode.ENCODE_ERROR, e);
                }
            }
        });
        cloud.activate(context, Map.of(ConfigurationService.KURA_SERVICE_PID, "test.cloud",
                "topic.control-prefix", "EDC", "payload.encoding", "simple-json"));
    }

    @AfterEach
    void deactivate() throws Exception {
        try {
            if (cloud != null) {
                when(data.isConnected()).thenReturn(false);
                cloud.deactivate(context);
            }
        } finally {
            acknowledgements.shutdown();
            try {
                assertTrue(acknowledgements.awaitTermination(2, TimeUnit.SECONDS));
            } finally {
                acknowledgements.shutdownNow();
            }
        }
    }

    @Test
    void testConnectCannotConnect() throws Exception {
        KuraConnectException failure = new KuraConnectException("unavailable broker");
        doThrow(failure).when(data).connect();
        assertSame(failure, assertThrows(KuraConnectException.class, () -> cloud.connect()));
    }

    @Test
    void testDisconnect() {
        cloud.disconnect();
        verify(data).disconnect(10);
    }

    @Test
    void testGetConnectionInfo() {
        Map<String, String> info = Map.of("Broker URL", "mqtt://localhost:1883", "Account", "account",
                "Username", "user", "Client ID", "client");
        when(data.getConnectionInfo()).thenReturn(info);
        assertSame(info, cloud.getInfo());
    }

    @Test
    void testGetNotificationPublisherPid() {
        assertEquals("org.eclipse.kura.cloud.publisher.CloudNotificationPublisher", cloud.getNotificationPublisherPid());
    }

    @Test
    void shouldSupportAdditionalBirthProperties() {
        assertEquals("getCpuVersion", publishBirth().get(KuraDeviceProfile.CPU_VERSION_KEY).asString());
    }

    @Test
    void shouldSupportEmptyExtendedProperties() {
        assertNull(publishBirth().get("extended_properties"));
    }

    @Test
    void shouldSupportExtendedPropertiesSerialization() {
        ExtendedProperties properties = new ExtendedProperties("1.5", List.of(
                new ExtendedPropertyGroup("first", Map.of("string", "str", "foo", "bar")),
                new ExtendedPropertyGroup("empty", Map.of())));
        cloud.setSystemService(system(Optional.of(properties)));
        JsonObject parsed = Json.parse(publishBirth().get("extended_properties").asString()).asObject();
        assertEquals("1.5", parsed.get("version").asString());
        JsonObject groups = parsed.get("properties").asObject();
        JsonObject first = groups.get("first").asObject();
        assertEquals(2, first.size());
        assertEquals("str", first.get("string").asString());
        assertEquals("bar", first.get("foo").asString());
        assertTrue(groups.get("empty").asObject().isEmpty());
    }

    @Test
    void shouldNotPublishTamperStatusIfTamperDetectionIsNotAvailable() {
        assertNull(publishBirth().get("tamper_status"));
    }

    @Test
    void shouldPublishTamperStatusIfTamperDetectionIsAvailable() throws Exception {
        TamperDetectionService tamper = mock(TamperDetectionService.class);
        cloud.setTamperDetectionService(tamper);
        when(tamper.getTamperStatus()).thenReturn(new TamperStatus(true, Map.of()));
        assertEquals(KuraBirthPayload.TamperStatus.TAMPERED.name(), publishBirth().get("tamper_status").asString());
        when(tamper.getTamperStatus()).thenReturn(new TamperStatus(false, Map.of()));
        assertEquals(KuraBirthPayload.TamperStatus.NOT_TAMPERED.name(), publishBirth().get("tamper_status").asString());
        cloud.unsetTamperDetectionService(tamper);
    }

    @Test
    void shouldRepublishBirthOnTamperEvent() throws Exception {
        TamperDetectionService tamper = mock(TamperDetectionService.class);
        cloud.setTamperDetectionService(tamper);
        when(tamper.getTamperStatus()).thenReturn(new TamperStatus(true, Map.of()));
        assertEquals("TAMPERED", publishBirth().get("tamper_status").asString());
        TamperStatus status = new TamperStatus(false, Map.of());
        when(tamper.getTamperStatus()).thenReturn(status);
        lastPayload.set(null);
        cloud.handleEvent(new TamperEvent("sensor", status));
        assertNull(lastPayload.get());
        assertNotNull(delayedPublish);
        delayedPublish.run();
        assertEquals("NOT_TAMPERED", metrics().get("tamper_status").asString());
        cloud.unsetTamperDetectionService(tamper);
    }

    @Test
    void shouldPublishBirthMessageWithModemInfoWhenNetworkStatusServiceIsPresent() throws Exception {
        modemStatuses(modem(1, ModemConnectionStatus.CONNECTED));
        assertModem(publishBirth(), 1);
    }

    @Test
    void shouldPublishBirthMessageWithConnectedModemInfoWhenMultipleModems() throws Exception {
        modemStatuses(modem(1, ModemConnectionStatus.FAILED), modem(2, ModemConnectionStatus.CONNECTED));
        assertModem(publishBirth(), 2);
    }

    private JsonObject publishBirth() {
        lastPayload.set(null);
        when(data.isConnected()).thenReturn(true);
        cloud.onConnectionEstablished();
        return metrics();
    }

    private JsonObject metrics() {
        assertNotNull(lastPayload.get());
        return Json.parse(new String(lastPayload.get(), StandardCharsets.UTF_8)).asObject().get("metrics").asObject();
    }

    private void modemStatuses(ModemInterfaceStatus... statuses) throws Exception {
        NetworkStatusService network = mock(NetworkStatusService.class);
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < statuses.length; i++) {
            String id = "modem-" + i;
            ids.add(id);
            when(network.getNetworkStatus(id)).thenReturn(Optional.of(statuses[i]));
        }
        when(network.getInterfaceIds()).thenReturn(ids);
        cloud.setNetworkStatusService(network);
    }

    private static ModemInterfaceStatus modem(int id, ModemConnectionStatus connection) {
        return ModemInterfaceStatus.builder().withConnectionStatus(connection).withSerialNumber("fooImei" + id)
                .withFirmwareVersion("fooFwVer" + id).withSignalStrength(id)
                .withAvailableSims(List.of(Sim.builder().withIccid("fooIccid" + id).withImsi("fooImsi" + id)
                        .withActive(true).withPrimary(true).build())).build();
    }

    private static void assertModem(JsonObject metrics, int id) {
        assertEquals("fooImei" + id, metrics.get("modem_imei").asString());
        assertEquals("fooImsi" + id, metrics.get("modem_imsi").asString());
        assertEquals("fooIccid" + id, metrics.get("modem_iccid").asString());
        assertEquals(Integer.toString(id), metrics.get("modem_rssi").asString());
        assertEquals("fooFwVer" + id, metrics.get("modem_firmware_version").asString());
    }

    private static SystemService system(Optional<ExtendedProperties> properties) {
        return (SystemService) Proxy.newProxyInstance(CloudServiceTest.class.getClassLoader(),
                new Class<?>[] {SystemService.class}, (object, method, args) -> {
                    if ("getExtendedProperties".equals(method.getName())) {
                        return properties;
                    } else if (method.getReturnType() == String.class) {
                        return method.getName();
                    } else if (method.getReturnType() == int.class) {
                        return 0;
                    } else if (method.getReturnType() == long.class) {
                        return 0L;
                    }
                    return null;
                });
    }
}
