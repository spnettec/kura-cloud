/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing.fullruntime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.eclipse.kura.cloud.CloudService;
import org.eclipse.kura.KuraProcessExecutionErrorException;
import org.eclipse.kura.cloudconnection.CloudConnectionManager;
import org.eclipse.kura.cloudconnection.factory.CloudConnectionFactory;
import org.eclipse.kura.configuration.ConfigurationService;
import org.eclipse.kura.configuration.Password;
import org.eclipse.kura.data.DataTransportService;
import org.eclipse.kura.marshalling.Marshaller;
import org.eclipse.kura.marshalling.Unmarshaller;
import org.eclipse.kura.message.KuraPayload;
import org.eclipse.kura.system.SystemService;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.deploymentadmin.DeploymentAdmin;
import org.osgi.service.deploymentadmin.DeploymentPackage;
import org.w3c.dom.Document;

/** Test-only protocol acceptance against a complete, isolated application. */
public final class ProtocolProbe implements BundleActivator {
    private static final String LEGACY_PID = "org.eclipse.kura.core.test.IConfigurationServiceTest";
    private static final String JSON_PID = "org.eclipse.kura.json.marshaller.unmarshaller.provider";
    private static final String PACKAGE = "org.eclipse.kura.cloud.testing.complete.inventory";
    private static final String PACKAGE_BUNDLE = PACKAGE + ".bundle";
    private static final String ACCOUNT = "complete-mac-account";
    private static final String USER = "complete-mac-user";
    private static final String PASSWORD = "isolated-test-password";
    private final List<ServiceReference<?>> references = new ArrayList<>();
    private final List<Map<String, Object>> requests = new ArrayList<>();
    private final List<String> legacy = new ArrayList<>();

    @Override
    public void start(BundleContext context) throws Exception {
        Path home = Path.of(System.getProperty("kura.home")).toAbsolutePath().normalize();
        Path allowed = Path.of(System.getProperty("kura.acceptance.root")).toAbsolutePath().normalize();
        require(home.startsWith(allowed) && !home.equals(allowed)
                && Files.isRegularFile(home.resolve(".protocol-acceptance-owned")), "Owned isolated profile required");
        String uri = System.getProperty("kura.acceptance.broker");
        require(uri.startsWith("tcp://127.0.0.1:"), "Loopback broker required");
        String pid = "acceptance.complete." + UUID.randomUUID();
        String client = "complete-cloud-" + UUID.randomUUID();
        String observerId = "complete-observer-" + UUID.randomUUID();
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("legacyAssertions", legacy);
        evidence.put("mqttRequests", requests);
        ConfigurationService configuration = null;
        CloudConnectionFactory factory = null;
        CloudConnectionManager manager = null;
        boolean created = false;
        boolean databaseCreated = false;
        Throwable failure = null;
        try {
            require(context.getBundles().length >= 284, "Complete application and upstream fixtures required");
            SystemService system = service(context, SystemService.class, null);
            require(system.getClass().getName().equals("org.eclipse.kura.core.system.SystemServiceImpl"), "Actual host SystemService required");
            require(Path.of(system.getKuraHome()).toAbsolutePath().normalize().equals(home), "Actual host home");
            Path snapshots = Path.of(system.getKuraSnapshotsDirectory()).toAbsolutePath().normalize();
            require(snapshots.startsWith(home), "Actual snapshot path must be owned");
            configuration = service(context, ConfigurationService.class, null);
            ConfigurationService config = configuration;
            await(() -> config.getConfigurableComponentPids().contains(LEGACY_PID), "Legacy SCR fixture arrival");
            runLegacy(context, "LegacyCoreScenarios", List.of("testServiceExists", "testLocalConfiguration",
                    "testRemoteConfiguration", "testSnapshotsMaxCount"));
            runLegacy(context, "LegacyInventoryScenarios", List.of("testGetPackages", "testGetBundles"));
            factory = service(context, CloudConnectionFactory.class,
                    "(service.pid=org.eclipse.kura.core.cloud.factory.DefaultCloudServiceFactory)");
            require("org.eclipse.kura.cloud.CloudService".equals(factory.getFactoryPid()), "Preserved cloud factory PID");
            if (!configuration.getConfigurableComponentPids().contains("org.eclipse.kura.db.H2DbService")) {
                configuration.createFactoryConfiguration("org.eclipse.kura.core.db.H2DbService", "org.eclipse.kura.db.H2DbService",
                        Map.of("db.connector.url", "jdbc:h2:file:" + home.resolve("data/protocol-messages")), true);
                databaseCreated = true;
            }
            factory.createConfiguration(pid, "测试云连接", "保留本地名称与描述");
            created = true;
            List<String> stack = factory.getStackComponentsPids(pid);
            require(stack.size() == 3 && pid.equals(stack.getFirst()), "Actual three-service cloud stack");
            CloudService cloud = service(context, CloudService.class, "(kura.service.pid=" + pid + ")");
            ServiceReference<CloudService> cloudRef = context.getServiceReferences(CloudService.class,
                    "(kura.service.pid=" + pid + ")").iterator().next();
            require("org.eclipse.kura.cloudconnection.kapua.mqtt.provider".equals(cloudRef.getBundle().getSymbolicName()), "Production Kapua provider");
            require(pid.equals(cloudRef.getProperty("kura.service.pid")), "Preserved cloud service PID");
            require("测试云连接".equals(cloudRef.getProperty("kura.cloud.factory.name"))
                    && "保留本地名称与描述".equals(cloudRef.getProperty("kura.cloud.factory.desc")), "Preserved name/description");
            configuration.updateConfiguration(stack.get(2), new HashMap<>(Map.of("broker-url", uri, "username", USER,
                    "password", new Password(PASSWORD.toCharArray()), "client-id", client,
                    "topic.context.account-name", ACCOUNT, "timeout", 3)), false);
            configuration.updateConfiguration(pid, Map.of("topic.control-prefix", "EDC", "payload.encoding", "simple-json",
                    "encode.gzip", false), false);
            DataTransportService transport = service(context, DataTransportService.class,
                    "(kura.service.pid=" + stack.get(2) + ")");
            await(() -> uri.equals(transport.getBrokerUrl()) && client.equals(transport.getClientId())
                    && ACCOUNT.equals(transport.getAccountName()) && "simple-json".equals(cloudRef.getProperty("payload.encoding")),
                    "Actual transport/cloud configuration updates");
            manager = service(context, CloudConnectionManager.class, "(kura.service.pid=" + pid + ")");
            manager.connect();
            await(cloud::isConnected, "Actual cloud MQTT connection");
            Marshaller marshaller = service(context, Marshaller.class, "(kura.service.pid=" + JSON_PID + ")");
            Unmarshaller unmarshaller = service(context, Unmarshaller.class, "(kura.service.pid=" + JSON_PID + ")");
            DeploymentAdmin deployment = service(context, DeploymentAdmin.class, null);
            try (MqttClient observer = new MqttClient(uri, observerId, new MemoryPersistence())) {
                var replies = new LinkedBlockingQueue<Reply>();
                observer.setCallback(new MqttCallback() {
                    public void connectionLost(Throwable cause) { }
                    public void deliveryComplete(IMqttDeliveryToken token) { }
                    public void messageArrived(String topic, MqttMessage message) { replies.add(new Reply(topic, message)); }
                });
                MqttConnectOptions options = new MqttConnectOptions();
                options.setUserName(USER); options.setPassword(PASSWORD.toCharArray()); options.setConnectionTimeout(3);
                observer.connect(options);
                try {
                    ProtocolClient protocol = new ProtocolClient(observer, replies, marshaller, unmarshaller, client, observerId);
                    String original = protocol.request("CONF-V1", "GET", "configurations/" + LEGACY_PID, null, 200);
                    require(original.contains("prop.string.value"), "Actual CONF-V1 fixture default over MQTT");
                    String ids = protocol.request("CONF-V1", "EXEC", "snapshot", null, 200);
                    long before = Long.parseLong(xml(ids).getElementsByTagNameNS("http://eurotech.com/esf/2.0", "snapshotIds")
                            .item(0).getTextContent());
                    protocol.request("CONF-V1", "PUT", "configurations/" + LEGACY_PID, changedXml(original), 200);
                    await(() -> "modified_value".equals(config.getComponentConfiguration(LEGACY_PID)
                            .getConfigurationProperties().get("prop.string")), "Actual SCR MQTT PUT callback");
                    require(protocol.request("CONF-V1", "GET", "configurations/" + LEGACY_PID, null, 200)
                            .contains("modified_value"), "MQTT GET after PUT");
                    protocol.request("CONF-V1", "EXEC", "rollback/" + before, null, 200);
                    await(() -> "prop.string.value".equals(config.getComponentConfiguration(LEGACY_PID)
                            .getConfigurationProperties().get("prop.string")), "Actual SCR MQTT rollback callback");
                    require(protocol.request("CONF-V1", "GET", "configurations/" + LEGACY_PID, null, 200)
                            .contains("prop.string.value"), "MQTT GET after rollback");
                    DeploymentPackage installed = deployment.installDeploymentPackage(new ByteArrayInputStream(packageBytes()));
                    Bundle installedBundle = installed.getBundle(PACKAGE_BUNDLE);
                    try {
                        require(installedBundle != null && installedBundle.getState() == Bundle.ACTIVE, "Actual deployment lifecycle");
                        JsonObject packages = JsonParser.parseString(protocol.request("INVENTORY-V1", "GET", "deploymentPackages", null, 200)).getAsJsonObject();
                        require("1.0.0".equals(find(packages.getAsJsonArray("deploymentPackages"), PACKAGE).get("version").getAsString()), "MQTT deployment inventory");
                        JsonArray bundles = JsonParser.parseString(protocol.request("INVENTORY-V1", "GET", "bundles", null, 200)).getAsJsonObject().getAsJsonArray("bundles");
                        require("ACTIVE".equals(find(bundles, PACKAGE_BUNDLE).get("state").getAsString()), "MQTT bundle inventory");
                        require(find(bundles, "org.eclipse.kura.core.inventory") != null, "Complete application core inventory");
                        JsonArray merged = JsonParser.parseString(protocol.request("INVENTORY-V1", "GET", "inventory", null, 200))
                                .getAsJsonObject().getAsJsonArray("inventory");
                        require("DP".equals(find(merged, PACKAGE).get("type").getAsString()), "Merged MQTT deployment inventory");
                        require("1.0.0.test".equals(find(merged, PACKAGE_BUNDLE).get("version").getAsString()), "Merged MQTT bundle inventory");
                    } finally {
                        installed.uninstall();
                        require(installedBundle.getState() == Bundle.UNINSTALLED, "Owned deployment package cleanup");
                    }
                    boolean packageApiSucceeded;
                    int hostPackageCount = -1;
                    String hostPackageError = "";
                    try { hostPackageCount = system.getSystemPackages().size(); packageApiSucceeded = true; }
                    catch (KuraProcessExecutionErrorException unsupportedOnHost) { packageApiSucceeded = false; hostPackageError = unsupportedOnHost.toString(); }
                    String packageReply = protocol.request("INVENTORY-V1", "GET", "systemPackages", null, packageApiSucceeded ? 200 : 500);
                    if (packageApiSucceeded) {
                        require(JsonParser.parseString(packageReply).getAsJsonObject().getAsJsonArray("systemPackages").size()
                                == hostPackageCount, "MQTT system package count equals actual host API");
                    }
                    evidence.put("systemPackageApiSucceeded", packageApiSucceeded);
                    evidence.put("hostSystemPackageCount", hostPackageCount);
                    evidence.put("hostPackageError", hostPackageError);
                    String written = protocol.request("CONF-V2", "EXEC", "snapshots/_write", null, 200);
                    long snapshot = JsonParser.parseString(written).getAsJsonObject().get("id").getAsLong();
                    Path file = snapshots.resolve("snapshot_" + snapshot + ".xml");
                    require(Files.isRegularFile(file) && !Files.readString(file, StandardCharsets.ISO_8859_1).startsWith("<?xml"), "Actual encrypted host snapshot");
                    require(protocol.request("CONF-V2", "POST", "snapshots/byId", "{\"id\":" + snapshot + "}", 200)
                            .contains(pid), "Actual encrypted snapshot read over MQTT");
                    evidence.put("snapshotId", snapshot);
                } finally { if (observer.isConnected()) { observer.disconnect(); } }
            }
            evidence.put("bundleCount", context.getBundles().length);
            evidence.put("systemImplementation", system.getClass().getName());
            evidence.put("cloudPid", pid); evidence.put("clientId", client); evidence.put("observerId", observerId);
            evidence.put("legacyAssertions", legacy); evidence.put("mqttRequests", requests);
        } catch (InvocationTargetException e) { failure = e.getCause(); }
        catch (Throwable e) { failure = e; }
        finally {
            try {
                if (manager != null && manager.isConnected()) { manager.disconnect(); }
                if (created) {
                    factory.deleteConfiguration(pid);
                    require(!factory.getManagedCloudConnectionPids().contains(pid), "Owned cloud stack cleanup");
                    evidence.put("cloudStackDeleted", true);
                }
                if (databaseCreated) { configuration.deleteFactoryConfiguration("org.eclipse.kura.db.H2DbService", true); }
            } catch (Throwable cleanup) { if (failure == null) { failure = cleanup; } else { failure.addSuppressed(cleanup); } }
            for (ServiceReference<?> reference : references) { context.ungetService(reference); }
        }
        evidence.put("passed", failure == null);
        if (failure != null) { evidence.put("error", failure.toString()); failure.printStackTrace(); }
        Files.writeString(home.resolve("protocol-probe-result.json"), new Gson().toJson(evidence) + "\n");
        if (failure != null) { throw new IllegalStateException("Complete runtime protocol acceptance failed", failure); }
        System.out.println("COMPLETE_PROTOCOL_PROBE_PASS");
    }

    private void runLegacy(BundleContext context, String simpleName, List<String> methods) throws Exception {
        Bundle fixture = java.util.Arrays.stream(context.getBundles())
                .filter(b -> "org.eclipse.kura.testing.configuration.fixtures".equals(b.getSymbolicName())).findFirst().orElseThrow();
        Class<?> type = fixture.loadClass("org.eclipse.kura.testing.configuration.fixture." + simpleName);
        for (String method : methods) {
            try (AutoCloseable instance = (AutoCloseable) type.getConstructor(BundleContext.class).newInstance(fixture.getBundleContext())) {
                type.getMethod(method).invoke(instance); legacy.add(simpleName + "." + method);
            }
        }
    }

    private <T> T service(BundleContext context, Class<T> type, String filter) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        do {
            for (ServiceReference<T> reference : context.getServiceReferences(type, filter)) {
                T service = context.getService(reference);
                if (service != null) { references.add(reference); return service; }
            }
            Thread.sleep(25);
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Missing actual service " + type.getName() + " " + filter);
    }

    private static void await(CheckedCondition condition, String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!condition.get() && System.nanoTime() < deadline) { Thread.sleep(25); }
        require(condition.get(), message);
    }
    private interface CheckedCondition { boolean get() throws Exception; }
    private record Reply(String topic, MqttMessage message) { }

    private final class ProtocolClient {
        private final MqttClient observer;
        private final LinkedBlockingQueue<Reply> replies;
        private final Marshaller marshaller;
        private final Unmarshaller unmarshaller;
        private final String client;
        private final String observerId;
        ProtocolClient(MqttClient observer, LinkedBlockingQueue<Reply> replies, Marshaller marshaller,
                Unmarshaller unmarshaller, String client, String observerId) {
            this.observer = observer; this.replies = replies; this.marshaller = marshaller;
            this.unmarshaller = unmarshaller; this.client = client; this.observerId = observerId;
        }
        String request(String app, String method, String resource, String body, int expectedCode) throws Exception {
            String id = UUID.randomUUID().toString();
            String replyTopic = "EDC/" + ACCOUNT + "/" + observerId + "/" + app + "/REPLY/" + id;
            observer.subscribe(replyTopic, 1);
            try {
                KuraPayload payload = new KuraPayload();
                payload.addMetric("request.id", id); payload.addMetric("requester.client.id", observerId);
                if (body != null) { payload.setBody(body.getBytes(StandardCharsets.UTF_8)); }
                observer.publish("EDC/" + ACCOUNT + "/" + client + "/" + app + "/" + method + "/" + resource,
                        marshaller.marshal(payload).getBytes(StandardCharsets.UTF_8), 1, false);
                Reply reply = replies.poll(15, TimeUnit.SECONDS);
                require(reply != null && replyTopic.equals(reply.topic()), "UUID-correlated MQTT reply for " + app + "/" + resource);
                KuraPayload decoded = unmarshaller.unmarshal(new String(reply.message().getPayload(), StandardCharsets.UTF_8), KuraPayload.class);
                int code = ((Number) decoded.getMetric("response.code")).intValue();
                requests.add(Map.of("app", app, "method", method, "resource", resource, "responseCode", code,
                        "requestId", id, "replyTopicMatched", true));
                require(code == expectedCode, "Unexpected response " + code + " for " + app + "/" + resource);
                byte[] bytes = decoded.getBody();
                return bytes == null ? "" : new String(bytes, StandardCharsets.UTF_8);
            } finally { observer.unsubscribe(replyTopic); }
        }
    }

    private static Document xml(String text) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance(); factory.setNamespaceAware(true);
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, ""); factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        return factory.newDocumentBuilder().parse(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)));
    }
    private static String changedXml(String original) throws Exception {
        Document document = xml(original);
        var properties = document.getElementsByTagNameNS("http://eurotech.com/esf/2.0", "property");
        boolean changed = false;
        for (int i = 0; i < properties.getLength(); i++) {
            var property = (org.w3c.dom.Element) properties.item(i);
            if ("prop.string".equals(property.getAttribute("name"))) {
                property.getElementsByTagNameNS("http://eurotech.com/esf/2.0", "value").item(0).setTextContent("modified_value");
                changed = true;
            }
        }
        require(changed, "Actual configuration XML property");
        TransformerFactory factory = TransformerFactory.newInstance();
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, ""); factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
        StringWriter writer = new StringWriter(); factory.newTransformer().transform(new DOMSource(document), new StreamResult(writer));
        return writer.toString();
    }
    private static JsonObject find(JsonArray items, String name) {
        for (var item : items) { if (name.equals(item.getAsJsonObject().get("name").getAsString())) { return item.getAsJsonObject(); } }
        throw new IllegalStateException("Missing actual inventory item " + name);
    }
    private static byte[] packageBytes() throws Exception {
        Manifest bundle = new Manifest(); Attributes headers = bundle.getMainAttributes();
        headers.putValue("Manifest-Version", "1.0"); headers.putValue("Bundle-ManifestVersion", "2");
        headers.putValue("Bundle-SymbolicName", PACKAGE_BUNDLE); headers.putValue("Bundle-Version", "1.0.0.test");
        ByteArrayOutputStream bundleBytes = new ByteArrayOutputStream();
        try (JarOutputStream ignored = new JarOutputStream(bundleBytes, bundle)) { }
        Manifest manifest = new Manifest(); headers = manifest.getMainAttributes();
        headers.putValue("Manifest-Version", "1.0"); headers.putValue("DeploymentPackage-SymbolicName", PACKAGE);
        headers.putValue("DeploymentPackage-Version", "1.0.0");
        Attributes entry = new Attributes(); entry.putValue("Bundle-SymbolicName", PACKAGE_BUNDLE); entry.putValue("Bundle-Version", "1.0.0.test");
        manifest.getEntries().put("bundles/fixture.jar", entry);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream jar = new JarOutputStream(bytes, manifest)) {
            jar.putNextEntry(new JarEntry("bundles/fixture.jar")); jar.write(bundleBytes.toByteArray()); jar.closeEntry();
        }
        return bytes.toByteArray();
    }
    private static void require(boolean condition, String message) { if (!condition) { throw new IllegalStateException(message); } }
    @Override public void stop(BundleContext context) { }
}
