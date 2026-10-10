/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.eclipse.kura.testing.osgi.EquinoxRuntime;
import org.eclipse.kura.testing.osgi.EquinoxRuntime.Service;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.osgi.framework.Bundle;

/** A real broker connects SCR-bound core handlers to the Kapua CloudService. */
final class CoreProtocolMqttScenario {
    private static final String ACCOUNT = "factory-account";
    private static final String OBSERVER = "core-protocol-observer";
    private static final String JSON_PID = "org.eclipse.kura.json.marshaller.unmarshaller.provider";
    private static final String PACKAGE_NAME = "org.eclipse.kura.cloud.testing.mqtt.inventory";
    private static final String BUNDLE_NAME = PACKAGE_NAME + ".bundle";
    private static final String BUNDLE_VERSION = "1.0.0.test";

    private CoreProtocolMqttScenario() { }

    static void run(EquinoxRuntime runtime, Service cloud, String brokerUri, String user, String password,
            String clientId, Path data) throws Exception {
        assertEquals(Bundle.ACTIVE, runtime.bundle("org.eclipse.kura.core.configuration").getState());
        try (var inventory = runtime.service("org.eclipse.kura.core.inventory.InventoryHandlerV1", null,
                     Duration.ofSeconds(10));
             var restConfiguration = runtime.service(
                     "org.eclipse.kura.internal.rest.configuration.ConfigurationRestService", null,
                     Duration.ofSeconds(10));
             var crypto = runtime.service("org.eclipse.kura.crypto.CryptoService", null,
                     Duration.ofSeconds(10));
             var marshaller = runtime.service("org.eclipse.kura.marshalling.Marshaller",
                     "(kura.service.pid=" + JSON_PID + ")", Duration.ofSeconds(5));
             var unmarshaller = runtime.service("org.eclipse.kura.marshalling.Unmarshaller",
                     "(kura.service.pid=" + JSON_PID + ")", Duration.ofSeconds(5));
             var deploymentAdmin = runtime.service("org.osgi.service.deploymentadmin.DeploymentAdmin", null,
                     Duration.ofSeconds(5));
             MqttClient observer = new MqttClient(brokerUri, OBSERVER, new MemoryPersistence())) {
            assertEquals("org.eclipse.kura.core.inventory", inventory.provider().getSymbolicName());
            assertEquals("org.eclipse.kura.rest.configuration.provider",
                    restConfiguration.provider().getSymbolicName());
            assertEquals("org.eclipse.kura.core.crypto", crypto.provider().getSymbolicName());
            LinkedBlockingQueue<MqttMessage> replies = new LinkedBlockingQueue<>();
            observer.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) { }
                public void deliveryComplete(IMqttDeliveryToken token) { }
                public void messageArrived(String topic, MqttMessage message) { replies.add(message); }
            });
            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName(user);
            options.setPassword(password.toCharArray());
            options.setConnectionTimeout(3);
            Object deploymentPackage = deploymentAdmin.call("installDeploymentPackage",
                    new ByteArrayInputStream(deploymentPackageBytes()));
            Bundle installedBundle = null;
            try {
                installedBundle = runtime.bundle(BUNDLE_NAME);
                assertEquals(Bundle.ACTIVE, installedBundle.getState());
                observer.connect(options);
                assertTrue((Boolean) cloud.call("isConnected"));
                Class<?> payloadType = cloud.provider().loadClass("org.eclipse.kura.message.KuraPayload");
                String configuration = request(observer, replies, marshaller, unmarshaller, payloadType,
                        clientId, "CONF-V1", "configurations/fixture.custom.connection");
                assertTrue(configuration.contains("fixture.custom.connection"),
                        "The real configuration handler must return its SCR factory configuration");
                String packages = request(observer, replies, marshaller, unmarshaller, payloadType,
                        clientId, "INVENTORY-V1", "systemPackages");
                assertTrue(packages.contains("\"systemPackages\""),
                        "The real inventory handler must return its system package response");
                String bundles = request(observer, replies, marshaller, unmarshaller, payloadType,
                        clientId, "INVENTORY-V1", "bundles");
                assertTrue(bundles.contains("org.eclipse.kura.core.inventory"),
                        "The real inventory handler must enumerate its installed Equinox bundle");
                JsonObject deployment = findNamed(JsonParser.parseString(request(observer, replies, marshaller,
                        unmarshaller, payloadType, clientId, "INVENTORY-V1", "deploymentPackages"))
                        .getAsJsonObject().getAsJsonArray("deploymentPackages"), PACKAGE_NAME);
                assertEquals("1.0.0", deployment.get("version").getAsString());
                assertFalse(deployment.get("signed").getAsBoolean());
                JsonObject packageBundle = findNamed(deployment.getAsJsonArray("bundles"), BUNDLE_NAME);
                assertEquals(BUNDLE_VERSION, packageBundle.get("version").getAsString());
                assertEquals("ACTIVE", packageBundle.get("state").getAsString());
                JsonArray inventoryItems = JsonParser.parseString(request(observer, replies, marshaller, unmarshaller,
                        payloadType, clientId, "INVENTORY-V1", "inventory"))
                        .getAsJsonObject().getAsJsonArray("inventory");
                assertEquals("DP", findNamed(inventoryItems, PACKAGE_NAME).get("type").getAsString());
                assertEquals(BUNDLE_VERSION, findNamed(inventoryItems, BUNDLE_NAME).get("version").getAsString());
                String written = request(observer, replies, marshaller, unmarshaller, payloadType,
                        clientId, "CONF-V2", "EXEC", "snapshots/_write", null);
                long snapshotId = JsonParser.parseString(written).getAsJsonObject().get("id").getAsLong();
                Path snapshot = data.resolve("snapshots/snapshot_" + snapshotId + ".xml");
                assertTrue(Files.isRegularFile(snapshot), "CONF-V2 must write an actual snapshot file");
                assertFalse(new String(Files.readAllBytes(snapshot), StandardCharsets.UTF_8).startsWith("<?xml"),
                        "The real master-key CryptoService must encrypt the snapshot");
                String restored = request(observer, replies, marshaller, unmarshaller, payloadType,
                        clientId, "CONF-V2", "POST", "snapshots/byId", "{\"id\":" + snapshotId + "}");
                assertTrue(restored.contains("fixture.custom.connection"),
                        "CONF-V2 must read the encrypted snapshot through the real ConfigurationService");
            } finally {
                try {
                    if (observer.isConnected()) {
                        observer.disconnect();
                    }
                } finally {
                    Class<?> packageType = deploymentAdmin.provider()
                            .loadClass("org.osgi.service.deploymentadmin.DeploymentPackage");
                    EquinoxRuntime.invoke(packageType, deploymentPackage, "uninstall");
                    if (installedBundle != null) {
                        assertEquals(Bundle.UNINSTALLED, installedBundle.getState());
                    }
                }
            }
        }
    }

    private static JsonObject findNamed(JsonArray items, String name) {
        assertNotNull(items);
        for (var element : items) {
            JsonObject item = element.getAsJsonObject();
            if (name.equals(item.get("name").getAsString())) {
                return item;
            }
        }
        return fail("Missing inventory resource " + name);
    }

    private static byte[] deploymentPackageBytes() throws Exception {
        Manifest bundle = new Manifest();
        Attributes bundleHeaders = bundle.getMainAttributes();
        bundleHeaders.putValue("Manifest-Version", "1.0");
        bundleHeaders.putValue("Bundle-ManifestVersion", "2");
        bundleHeaders.putValue("Bundle-SymbolicName", BUNDLE_NAME);
        bundleHeaders.putValue("Bundle-Version", BUNDLE_VERSION);
        ByteArrayOutputStream bundleBytes = new ByteArrayOutputStream();
        try (JarOutputStream jar = new JarOutputStream(bundleBytes, bundle)) {
            // A manifest-only bundle exercises DeploymentAdmin without a host service.
        }
        String path = "bundles/fixture.jar";
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        manifest.getMainAttributes().putValue("DeploymentPackage-SymbolicName", PACKAGE_NAME);
        manifest.getMainAttributes().putValue("DeploymentPackage-Version", "1.0.0");
        Attributes entry = new Attributes();
        entry.putValue("Bundle-SymbolicName", BUNDLE_NAME);
        entry.putValue("Bundle-Version", BUNDLE_VERSION);
        manifest.getEntries().put(path, entry);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream jar = new JarOutputStream(bytes, manifest)) {
            jar.putNextEntry(new JarEntry(path));
            jar.write(bundleBytes.toByteArray());
            jar.closeEntry();
        }
        return bytes.toByteArray();
    }

    private static String request(MqttClient observer, LinkedBlockingQueue<MqttMessage> replies,
            Service marshaller, Service unmarshaller, Class<?> payloadType, String clientId,
            String appId, String resource) throws Exception {
        return request(observer, replies, marshaller, unmarshaller, payloadType, clientId,
                appId, "GET", resource, null);
    }

    private static String request(MqttClient observer, LinkedBlockingQueue<MqttMessage> replies,
            Service marshaller, Service unmarshaller, Class<?> payloadType, String clientId,
            String appId, String method, String resource, String body) throws Exception {
        String id = UUID.randomUUID().toString();
        String replyTopic = "EDC/" + ACCOUNT + "/" + OBSERVER + "/" + appId + "/REPLY/" + id;
        observer.subscribe(replyTopic, 1);
        try {
            Object payload = payloadType.getConstructor().newInstance();
            EquinoxRuntime.invoke(payloadType, payload, "addMetric", "request.id", id);
            EquinoxRuntime.invoke(payloadType, payload, "addMetric", "requester.client.id", OBSERVER);
            if (body != null) {
                EquinoxRuntime.invoke(payloadType, payload, "setBody", body.getBytes(StandardCharsets.UTF_8));
            }
            String json = (String) marshaller.call("marshal", payload);
            String topic = "EDC/" + ACCOUNT + "/" + clientId + "/" + appId + "/" + method + "/" + resource;
            observer.publish(topic, json.getBytes(StandardCharsets.UTF_8), 1, false);
            MqttMessage reply = replies.poll(10, TimeUnit.SECONDS);
            assertNotNull(reply, "No MQTT reply from SCR handler " + appId);
            Object decoded = unmarshaller.call("unmarshal",
                    new String(reply.getPayload(), StandardCharsets.UTF_8), payloadType);
            assertEquals(200, ((Number) EquinoxRuntime.invoke(payloadType, decoded,
                    "getMetric", "response.code")).intValue(), appId + " rejected the MQTT request");
            byte[] replyBody = (byte[]) EquinoxRuntime.invoke(payloadType, decoded, "getBody");
            assertNotNull(replyBody, appId + " returned no response body");
            return new String(replyBody, StandardCharsets.UTF_8);
        } finally {
            observer.unsubscribe(replyTopic);
        }
    }
}
