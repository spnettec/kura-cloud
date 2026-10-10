/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.bouncycastle.asn1.x500.X500Name;
import org.eclipse.kura.core.testutil.pki.TestCA;
import org.eclipse.kura.core.testutil.pki.TestCA.CertificateCreationOptions;
import org.eclipse.kura.testing.osgi.EquinoxRuntime;
import org.eclipse.kura.testing.osgi.EquinoxRuntime.Service;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

/** Real filesystem key material, SCR SSL manager and MQTT client; no keystore mock. */
final class CloudTlsRuntimeScenario {
    private static final String PASSWORD = "isolated-key-password";
    private static final String KEYSTORE_FACTORY = "org.eclipse.kura.core.keystore.FilesystemKeystoreServiceImpl";
    private static final String SSL_FACTORY = "org.eclipse.kura.ssl.SslManagerService";
    private static final String TRANSPORT_FACTORY = "org.eclipse.kura.core.data.transport.mqtt.MqttDataTransport";
    private static final String TOPIC = "fixture/tls/roundtrip";
    enum Case { MISSING_KEY, WRONG_TRUST, WRONG_HOSTNAME, MUTUAL_TLS, REVOCATION }

    static void run(EquinoxRuntime runtime, Service configuration, Path directory, boolean sparkplug,
            boolean websocket) throws Exception {
        TestCA brokerCA = new TestCA(CertificateCreationOptions.builder(new X500Name("CN=runtime broker CA")).build());
        KeyPair serverKey = TestCA.generateKeyPair();
        X509Certificate serverCertificate = brokerCA.createAndSignCertificate(
                CertificateCreationOptions.builder(new X500Name("CN=localhost")).build(), serverKey);
        TestCA clientCA = new TestCA(CertificateCreationOptions.builder(new X500Name("CN=runtime client CA")).build());
        KeyPair clientKey = TestCA.generateKeyPair();
        X509Certificate clientCertificate = clientCA.createAndSignCertificate(
                CertificateCreationOptions.builder(new X500Name("CN=runtime client")).build(), clientKey);
        var tls = SslContextBuilder.forServer(serverKey.getPrivate(), serverCertificate, brokerCA.getCertificate())
                .sslProvider(SslProvider.JDK).trustManager(clientCA.getCertificate()).build();
        Server broker = new Server();
        Properties options = new Properties();
        options.setProperty("host", "127.0.0.1");
        options.setProperty("port", "0");
        options.setProperty("ssl_port", "0");
        if (websocket) { options.setProperty("secure_websocket_port", "0"); }
        options.setProperty("allow_anonymous", "true");
        options.setProperty("need_client_auth", "true");
        options.setProperty("persistence_enabled", "false");
        options.setProperty("telemetry_enabled", "false");
        try {
            broker.startServer(new MemoryConfig(options), List.of(), () -> tls, null, null);
            await(() -> {
                try { return broker.getPort() > 0 && broker.getSslPort() > 0; }
                catch (ConcurrentModificationException bindingInProgress) { return false; }
            }, "TLS broker did not bind");
            int secureWebsocketPort = websocket ? awaitSecureWebsocketPort(broker) : -1;
            LinkedBlockingQueue<MqttMessage> messages = new LinkedBlockingQueue<>();
            LinkedBlockingQueue<MqttMessage> deaths = new LinkedBlockingQueue<>();
            try (MqttClient observer = new MqttClient("tcp://127.0.0.1:" + broker.getPort(), "tls-observer", new MemoryPersistence())) {
                observer.setCallback(new MqttCallback() {
                    public void connectionLost(Throwable cause) { }
                    public void deliveryComplete(IMqttDeliveryToken token) { }
                    public void messageArrived(String topic, MqttMessage message) {
                        (topic.equals(TOPIC) ? messages : deaths).add(message);
                    }
                });
                observer.connect();
                observer.subscribe(TOPIC, 1);
                if (sparkplug) { observer.subscribe("spBv1.0/group/NDEATH/node", 1); }
                try {
                    for (Case scenario : Case.values()) {
                        Path file = directory.resolve(scenario.name() + ".jks");
                        KeyStore keys = KeyStore.getInstance("JKS");
                        keys.load(null, null);
                        if (scenario != Case.MISSING_KEY) {
                            keys.setKeyEntry("client", clientKey.getPrivate(), PASSWORD.toCharArray(),
                                    new Certificate[] { clientCertificate, clientCA.getCertificate() });
                        }
                        keys.setCertificateEntry("trust", scenario == Case.WRONG_TRUST
                                ? clientCA.getCertificate() : brokerCA.getCertificate());
                        try (var output = Files.newOutputStream(file)) { keys.store(output, PASSWORD.toCharArray()); }
                        try (var crl = scenario == Case.REVOCATION
                                ? new RuntimeCrlFeed(runtime, brokerCA, serverCertificate, "fixture.tls.keys." + scenario) : null) {
                            verify(runtime, configuration, file, scenario,
                                    websocket ? secureWebsocketPort : broker.getSslPort(), messages, deaths,
                                    sparkplug, websocket, crl);
                        }
                    }
                } finally { observer.disconnect(); }
            }
        } finally { broker.stopServer(); }
    }

    private static void verify(EquinoxRuntime runtime, Service configuration, Path file, Case scenario, int port,
            LinkedBlockingQueue<MqttMessage> messages, LinkedBlockingQueue<MqttMessage> deaths,
            boolean sparkplug, boolean websocket, RuntimeCrlFeed crl) throws Exception {
        String keyPid = "fixture.tls.keys." + scenario;
        String sslPid = "fixture.tls.ssl." + scenario;
        String transportPid = "fixture.tls.transport." + scenario;
        Class<?> passwordType = runtime.bundle("org.eclipse.kura.api").loadClass("org.eclipse.kura.configuration.Password");
        Object password = passwordType.getConstructor(char[].class).newInstance((Object) PASSWORD.toCharArray());
        Map<String, Object> keyOptions = new java.util.HashMap<>(Map.of("keystore.path", file.toString(),
                "keystore.password", password, "randomize.password", false, "crl.management.enabled", false));
        if (crl != null) { keyOptions.putAll(crl.options(file.resolveSibling("crl-cache.json"))); }
        configuration.call("createFactoryConfiguration", KEYSTORE_FACTORY, keyPid, keyOptions, true);
        try (var keys = runtime.service("org.eclipse.kura.security.keystore.KeystoreService", filter(keyPid), Duration.ofSeconds(10))) {
            assertEquals("org.eclipse.kura.core.keystore", keys.provider().getSymbolicName());
            KeyStore actual = (KeyStore) keys.call("getKeyStore");
            assertEquals(scenario != Case.MISSING_KEY, actual.isKeyEntry("client"));
            assertTrue(actual.isCertificateEntry("trust"));
            if (crl != null) { crl.awaitInitial(keys); }
            configuration.call("createFactoryConfiguration", SSL_FACTORY, sslPid,
                    Map.of("KeystoreService.target", filter(keyPid), "TruststoreKeystoreService.target", filter(keyPid),
                            "ssl.default.protocol", "TLSv1.2", "ssl.hostname.verification", true,
                            "ssl.revocation.check.enabled", crl != null, "ssl.revocation.mode", "CRL_ONLY"), true);
            try (var ssl = runtime.service(SSL_FACTORY, filter(sslPid), Duration.ofSeconds(10))) {
                assertEquals("org.eclipse.kura.core", ssl.provider().getSymbolicName());
                String uri = (websocket ? "wss://" : sparkplug ? "ssl://" : "mqtts://")
                        + (scenario == Case.WRONG_HOSTNAME ? "127.0.0.1" : "localhost") + ":" + port
                        + (websocket ? "/mqtt" : "");
                Map<String, Object> transportOptions = sparkplug
                        ? Map.of("server.uris", uri, "client.id", "tls-client-" + scenario, "connection.timeout", 2,
                                "SslManagerService.target", filter(sslPid))
                        : Map.of("broker-url", uri, "client-id", "tls-client-" + scenario, "timeout", 2,
                                "SslManagerService.target", filter(sslPid), "in-flight.persistence", "memory");
                configuration.call("createFactoryConfiguration", sparkplug
                                ? "org.eclipse.kura.cloudconnection.sparkplug.mqtt.transport.SparkplugDataTransport" : TRANSPORT_FACTORY,
                        transportPid, transportOptions, true);
                try (var transport = runtime.service("org.eclipse.kura.data.DataTransportService", filter(transportPid), Duration.ofSeconds(10))) {
                    assertEquals(sparkplug ? "org.eclipse.kura.cloudconnection.sparkplug.mqtt.provider"
                            : "org.eclipse.kura.cloud.base.provider", transport.provider().getSymbolicName());
                    assertEquals(filter(sslPid), transport.property("SslManagerService.target"));
                    LinkedBlockingQueue<Object> confirmations = new LinkedBlockingQueue<>();
                    java.util.concurrent.atomic.AtomicReference<Throwable> connectionLost = new java.util.concurrent.atomic.AtomicReference<>();
                    Class<?> listenerType = runtime.bundle("org.eclipse.kura.api")
                            .loadClass("org.eclipse.kura.data.transport.listener.DataTransportListener");
                    Object observer = java.lang.reflect.Proxy.newProxyInstance(listenerType.getClassLoader(),
                            new Class<?>[] { listenerType }, (proxy, method, args) -> {
                                if (method.getName().equals("onConnectionLost")) { connectionLost.set((Throwable) args[0]); }
                                if (method.getName().equals("onMessageConfirmed")) { confirmations.add(args[0]); }
                                if (method.getName().equals("hashCode")) { return System.identityHashCode(proxy); }
                                if (method.getName().equals("equals")) { return proxy == args[0]; }
                                if (method.getName().equals("toString")) { return "TLS connection observer"; }
                                return null;
                            });
                    transport.call("addDataTransportListener", observer);
                    if (scenario != Case.MUTUAL_TLS && scenario != Case.REVOCATION) {
                        Exception failure = assertThrows(Exception.class, () -> transport.call("connect"));
                        assertEquals("org.eclipse.kura.KuraConnectException", failure.getClass().getName());
                        assertFalse((Boolean) transport.call("isConnected"));
                    } else {
                        transport.call("connect");
                        assertTrue((Boolean) transport.call("isConnected"));
                        try {
                            byte[] body = ("真实文件密钥库双向 TLS:" + sparkplug + ":" + scenario)
                                    .getBytes(StandardCharsets.UTF_8);
                            Object sent = transport.call("publish", TOPIC, body, 1, false);
                            assertNotNull(sent);
                            Object confirmed = confirmations.poll(10, TimeUnit.SECONDS);
                            assertNotNull(confirmed, () -> "Missing delivery confirmation; connectionLost=" + connectionLost.get());
                            assertEquals(sent, confirmed);
                            MqttMessage delivered = messages.poll(10, TimeUnit.SECONDS);
                            assertNotNull(delivered);
                            assertArrayEquals(body, delivered.getPayload());
                            assertEquals(1, delivered.getQos());
                            assertFalse(delivered.isRetained());
                            assertNull(connectionLost.get(), "Unexpected connection loss");
                            assertTrue((Boolean) transport.call("isConnected"), "Transport disconnected during publication");
                        } finally { transport.call("disconnect", 0L); }
                        if (sparkplug) {
                            MqttMessage death = deaths.poll(10, TimeUnit.SECONDS);
                            assertNotNull(death, "Missing graceful NDEATH");
                            assertEquals(0, death.getQos(), "Graceful NDEATH, not QoS 1 last will");
                            assertFalse(death.isRetained());
                        }
                        if (crl != null) {
                            crl.revokeAndAwait(keys);
                            Exception revoked = assertThrows(Exception.class, () -> transport.call("connect"));
                            assertEquals("org.eclipse.kura.KuraConnectException", revoked.getClass().getName());
                            assertFalse((Boolean) transport.call("isConnected"));
                        }
                    }
                }
                configuration.call("deleteFactoryConfiguration", transportPid, true);
                await(() -> !runtime.hasService("org.eclipse.kura.data.DataTransportService", filter(transportPid)), "Transport remained registered");
            }
            configuration.call("deleteFactoryConfiguration", sslPid, true);
            await(() -> !runtime.hasService(SSL_FACTORY, filter(sslPid)), "SSL manager remained registered");
        }
        configuration.call("deleteFactoryConfiguration", keyPid, true);
        await(() -> !runtime.hasService("org.eclipse.kura.security.keystore.KeystoreService", filter(keyPid)), "Keystore remained registered");
    }

    private static String filter(String pid) { return "(kura.service.pid=" + pid + ")"; }

    @SuppressWarnings("unchecked")
    private static int awaitSecureWebsocketPort(Server broker) throws Exception {
        var acceptorField = Server.class.getDeclaredField("acceptor");
        acceptorField.setAccessible(true);
        Object acceptor = acceptorField.get(broker);
        var portsField = acceptor.getClass().getDeclaredField("ports");
        portsField.setAccessible(true);
        Map<String, Integer> ports = (Map<String, Integer>) portsField.get(acceptor);
        await(() -> {
            try { return ports.getOrDefault("Secure websocket", 0) > 0; }
            catch (ConcurrentModificationException bindingInProgress) { return false; }
        }, "WSS broker did not bind");
        return ports.get("Secure websocket");
    }
    private static void await(Callable<Boolean> condition, String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.call() && System.nanoTime() < deadline) { Thread.sleep(20); }
        assertTrue(condition.call(), message);
    }
}
