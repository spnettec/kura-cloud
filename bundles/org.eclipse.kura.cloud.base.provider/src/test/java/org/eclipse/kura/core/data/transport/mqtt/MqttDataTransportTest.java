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
package org.eclipse.kura.core.data.transport.mqtt;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.CertStore;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.bouncycastle.asn1.x500.X500Name;
import org.eclipse.kura.KuraConnectException;
import org.eclipse.kura.status.CloudConnectionStatusService;
import org.eclipse.kura.core.ssl.SslManagerServiceImpl;
import org.eclipse.kura.core.testutil.TestUtil;
import org.eclipse.kura.core.testutil.pki.TestCA;
import org.eclipse.kura.core.testutil.pki.TestCA.CertificateCreationOptions;
import org.eclipse.kura.core.testutil.pki.TestCA.CRLCreationOptions;
import org.eclipse.kura.security.keystore.KeystoreService;
import org.eclipse.kura.security.keystore.KeystoreChangedEvent;
import org.eclipse.kura.ssl.SslManagerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;

import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

/** Real Paho/Moquette/TLS and SSL-manager behavior; SCR and CRL HTTP refresh are separate acceptance work. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MqttDataTransportTest {

    private enum Protocol { MQTTS, WSS }
    private final List<Server> brokers = new ArrayList<>();
    private final ComponentContext componentContext = mock(ComponentContext.class);
    private TestCA brokerCA;
    private X509Certificate brokerCertificate;
    private TestCA clientCA;
    private KeyPair clientKey;
    private X509Certificate clientCertificate;
    private Server anonymousBroker;
    private Server mutualBroker;
    private MqttDataTransport transport;
    private TestSslManager ssl;

    @BeforeAll
    void startBrokers() throws Exception {
        when(this.componentContext.getBundleContext()).thenReturn(mock(BundleContext.class));
        this.brokerCA = new TestCA(CertificateCreationOptions.builder(new X500Name("CN=broker CA")).build());
        KeyPair brokerKey = TestCA.generateKeyPair();
        this.brokerCertificate = this.brokerCA.createAndSignCertificate(
                CertificateCreationOptions.builder(new X500Name("CN=broker.invalid")).build(), brokerKey);
        this.clientCA = new TestCA(CertificateCreationOptions.builder(new X500Name("CN=client CA")).build());
        this.clientKey = TestCA.generateKeyPair();
        this.clientCertificate = this.clientCA.createAndSignCertificate(
                CertificateCreationOptions.builder(new X500Name("CN=client")).build(), this.clientKey);
        SslContext serverTls = SslContextBuilder.forServer(brokerKey.getPrivate(), this.brokerCertificate,
                this.brokerCA.getCertificate()).sslProvider(SslProvider.JDK)
                .trustManager(this.clientCA.getCertificate()).build();
        this.anonymousBroker = startBroker(serverTls, false);
        this.mutualBroker = startBroker(serverTls, true);
    }

    private Server startBroker(SslContext tls, boolean requireClientCertificate) throws Exception {
        Properties options = new Properties();
        options.setProperty("host", "127.0.0.1");
        options.setProperty("port", "0");
        options.setProperty("ssl_port", "0");
        options.setProperty("secure_websocket_port", "0");
        options.setProperty("allow_anonymous", "true");
        options.setProperty("persistence_enabled", "false");
        options.setProperty("telemetry_enabled", "false");
        options.setProperty("need_client_auth", Boolean.toString(requireClientCertificate));
        Server broker = new Server();
        this.brokers.add(broker);
        broker.startServer(new MemoryConfig(options), List.of(), () -> tls, null, null);
        awaitPort(broker, "TCP MQTT");
        awaitPort(broker, "SSL MQTT");
        awaitPort(broker, "Secure websocket");
        return broker;
    }

    @SuppressWarnings("unchecked")
    private int awaitPort(Server broker, String protocol) throws Exception {
        // Moquette has no public WSS-port accessor. Read its bound-port map, never reserve-and-release a port.
        Object acceptor = TestUtil.getFieldValue(broker, "acceptor");
        Map<String, Integer> ports = (Map<String, Integer>) TestUtil.getFieldValue(acceptor, "ports");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            Integer port = ports.get(protocol);
            if (port != null && port > 0) {
                return port;
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }
        throw new IllegalStateException("Broker did not bind " + protocol);
    }

    private String uri(Server broker, Protocol protocol) throws Exception {
        return protocol == Protocol.MQTTS ? "mqtts://127.0.0.1:" + awaitPort(broker, "SSL MQTT")
                : "wss://127.0.0.1:" + awaitPort(broker, "Secure websocket") + "/mqtt";
    }

    @AfterEach
    void closeClient() throws Throwable {
        try {
            if (this.transport != null) {
                this.transport.deactivate(this.componentContext);
                // The current lifecycle only disconnects. Close retained Paho resources explicitly in this fixture.
                TestUtil.invokePrivate(this.transport, "closeMqttClient");
            }
        } finally {
            if (this.ssl != null) {
                this.ssl.stop(this.componentContext);
            }
            this.transport = null;
            this.ssl = null;
        }
    }

    @AfterAll
    void stopBrokers() {
        this.brokers.forEach(Server::stopServer);
    }

    private KeyStore store(boolean includeKey, X509Certificate trust) throws Exception {
        KeyStore result = KeyStore.getInstance("JKS");
        result.load(null, null);
        if (includeKey) {
            result.setKeyEntry("client", this.clientKey.getPrivate(), "changeit".toCharArray(),
                    new Certificate[] { this.clientCertificate, this.clientCA.getCertificate() });
        }
        if (trust != null) {
            result.setCertificateEntry("trust", trust);
        }
        return result;
    }

    private KeystoreService keystore(KeyStore store) throws Exception {
        KeystoreService service = mock(KeystoreService.class);
        when(service.getKeyStore()).thenReturn(store);
        KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(store, "changeit".toCharArray());
        when(service.getKeyManagers(anyString())).thenReturn(Arrays.asList(factory.getKeyManagers()));
        return service;
    }

    private KeystoreService configureSsl(boolean key, boolean hostnameVerification, boolean separateTrust,
            X509Certificate trust, boolean revocation) throws Exception {
        this.ssl = new TestSslManager();
        this.ssl.start(this.componentContext, Map.of("ssl.default.protocol", "TLS",
                "ssl.hostname.verification", hostnameVerification, "ssl.revocation.check.enabled", revocation,
                "ssl.revocation.mode", "CRL_ONLY"));
        KeystoreService primary = keystore(store(key, separateTrust ? null : trust));
        this.ssl.setKeystoreService(primary, Map.of("kura.service.pid", "key"));
        if (separateTrust) {
            this.ssl.setTruststoreKeystoreService(keystore(store(false, trust)), Map.of("kura.service.pid", "trust"));
        }
        return primary;
    }

    private void configureTransport(String brokerUri) {
        this.transport = new MqttDataTransport();
        this.transport.setCloudConnectionStatusService(mock(CloudConnectionStatusService.class));
        if (this.ssl != null) {
            this.transport.setSslManagerService(this.ssl);
        }
        Map<String, Object> properties = new HashMap<>();
        properties.put("kura.service.pid", "test.transport");
        properties.put("broker-url", brokerUri);
        properties.put("client-id", "mqtt-test-client");
        properties.put("keep-alive", 30);
        properties.put("timeout", 2);
        properties.put("clean-session", true);
        properties.put("protocol-version", 4);
        properties.put("in-flight.persistence", "memory");
        this.transport.activate(this.componentContext, properties);
    }

    private void connects() throws Exception {
        this.transport.connect();
        assertTrue(this.transport.isConnected());
    }

    private void rejectsConnection() {
        assertThrows(KuraConnectException.class, this.transport::connect);
        assertFalse(this.transport.isConnected());
    }

    @Test
    void shouldConnectOverPlainMqtt() throws Exception {
        configureTransport("mqtt://127.0.0.1:" + awaitPort(this.anonymousBroker, "TCP MQTT"));
        connects();
    }

    @Test
    void shouldNotConnectOverMqttsWithoutKeystore() throws Exception {
        configureTransport(uri(this.anonymousBroker, Protocol.MQTTS));
        rejectsConnection();
    }

    @Test
    void customSocketFactoryRetainsPahoHostnameVerification() throws Exception {
        TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(store(false, this.brokerCA.getCertificate()));
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trust.getTrustManagers(), null);
        SslManagerService custom = mock(SslManagerService.class);
        when(custom.getSSLSocketFactory()).thenReturn(context.getSocketFactory());
        configureTransport(uri(this.anonymousBroker, Protocol.MQTTS));
        this.transport.setSslManagerService(custom);
        rejectsConnection();
    }

    @ParameterizedTest
    @EnumSource(Protocol.class)
    void rejectsHostnameMismatch(Protocol protocol) throws Exception {
        configureSsl(false, true, false, this.brokerCA.getCertificate(), false);
        configureTransport(uri(this.anonymousBroker, protocol));
        rejectsConnection();
    }

    @ParameterizedTest
    @EnumSource(Protocol.class)
    void connectsWithHostnameIdentificationDisabled(Protocol protocol) throws Exception {
        configureSsl(false, false, false, this.brokerCA.getCertificate(), false);
        configureTransport(uri(this.anonymousBroker, protocol));
        connects();
    }

    @ParameterizedTest
    @EnumSource(Protocol.class)
    void rejectsMutualTlsWithoutClientKey(Protocol protocol) throws Exception {
        configureSsl(false, false, false, this.brokerCA.getCertificate(), false);
        configureTransport(uri(this.mutualBroker, protocol));
        rejectsConnection();
    }

    @ParameterizedTest
    @EnumSource(Protocol.class)
    void connectsWithMutualTls(Protocol protocol) throws Exception {
        configureSsl(true, false, false, this.brokerCA.getCertificate(), false);
        configureTransport(uri(this.mutualBroker, protocol));
        connects();
    }

    @ParameterizedTest
    @EnumSource(Protocol.class)
    void connectsWithSeparateTruststore(Protocol protocol) throws Exception {
        configureSsl(true, false, true, this.brokerCA.getCertificate(), false);
        configureTransport(uri(this.mutualBroker, protocol));
        connects();
    }

    @ParameterizedTest
    @EnumSource(Protocol.class)
    void rejectsWrongTruststore(Protocol protocol) throws Exception {
        configureSsl(true, false, true, this.clientCA.getCertificate(), false);
        configureTransport(uri(this.mutualBroker, protocol));
        rejectsConnection();
    }

    @Test
    void connectionShouldFailIfSeparateTruststoreIsUnset() throws Exception {
        configureSsl(true, false, false, null, false);
        KeystoreService trust = keystore(store(false, this.brokerCA.getCertificate()));
        this.ssl.setTruststoreKeystoreService(trust, Map.of("kura.service.pid", "trust"));
        configureTransport(uri(this.mutualBroker, Protocol.MQTTS));
        connects();
        this.transport.disconnect(0);
        this.ssl.unsetTruststoreKeystoreService(trust);
        this.transport.onConfigurationUpdated();
        rejectsConnection();
    }

    @Test
    void shouldSupportRevocation() throws Exception {
        KeystoreService keys = configureSsl(false, false, false, this.brokerCA.getCertificate(), true);
        when(keys.getCRLStore()).thenReturn(CertStore.getInstance("Collection", new CollectionCertStoreParameters(
                List.of(this.brokerCA.generateCRL(CRLCreationOptions.builder().build())))));
        configureTransport(uri(this.anonymousBroker, Protocol.MQTTS));
        connects();
        this.transport.disconnect(0);
        this.brokerCA.revokeCertificate(this.brokerCertificate);
        when(keys.getCRLStore()).thenReturn(CertStore.getInstance("Collection", new CollectionCertStoreParameters(
                List.of(this.brokerCA.generateCRL(CRLCreationOptions.builder().build())))));
        this.ssl.handleEvent(new KeystoreChangedEvent("key"));
        this.transport.onConfigurationUpdated();
        rejectsConnection();
    }

    @Test
    void rejectsUnknownRevocationStatus() throws Exception {
        KeystoreService keys = configureSsl(true, false, false, this.brokerCA.getCertificate(), true);
        when(keys.getCRLStore()).thenReturn(CertStore.getInstance("Collection",
                new CollectionCertStoreParameters(List.of())));
        configureTransport(uri(this.mutualBroker, Protocol.WSS));
        rejectsConnection();
    }

    private static class TestSslManager extends SslManagerServiceImpl {
        void start(ComponentContext context, Map<String, Object> properties) {
            super.activate(context, properties);
        }

        void stop(ComponentContext context) {
            super.deactivate(context);
        }
    }
}
