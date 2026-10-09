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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.eclipse.kura.core.data.transport.mqtt.MqttDataTransport;
import org.eclipse.kura.core.testutil.TestUtil;
import org.eclipse.kura.crypto.CryptoService;
import org.eclipse.kura.internal.db.h2db.provider.H2DbServiceImpl;
import org.eclipse.kura.status.CloudConnectionStatusService;
import org.eclipse.kura.watchdog.WatchdogService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.component.ComponentContext;

import io.moquette.broker.Server;

/** Real SQL storage, publisher executor and MQTT; registry/status/watchdog are controlled service boundaries. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseCloudTests {

    protected static final String DATA_PID = "test.data";
    protected DataServiceImpl dataService;
    protected H2DbServiceImpl database;
    protected String brokerUri;
    private Server broker;
    private TestTransport transport;
    private ComponentContext context;
    private BundleContext registry;
    private ServiceReference<Object> databaseReference;
    private List<ExecutorService> executors = List.of();
    private boolean active;

    @BeforeAll
    protected void startBroker() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("host", "127.0.0.1");
        properties.setProperty("port", "0");
        properties.setProperty("allow_anonymous", "true");
        properties.setProperty("persistence_enabled", "false");
        properties.setProperty("telemetry_enabled", "false");
        this.broker = new Server();
        this.broker.startServer(properties);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            try {
                int port = this.broker.getPort();
                if (port > 0) {
                    this.brokerUri = "mqtt://127.0.0.1:" + port;
                    return;
                }
            } catch (ConcurrentModificationException bindingInProgress) {
                // Only Moquette's asynchronous bound-port bookkeeping is retried.
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }
        throw new IllegalStateException("Broker did not report its bound port");
    }

    @SuppressWarnings("unchecked")
    @BeforeEach
    protected void startPipeline() throws Exception {
        this.active = false;
        this.executors = List.of();
        this.database = new H2DbServiceImpl();
        CryptoService crypto = mock(CryptoService.class);
        when(crypto.decryptAes(any(char[].class))).thenReturn(new char[0]);
        this.database.setCryptoService(crypto);
        this.database.activate(Map.of("db.connector.url", "jdbc:h2:mem:" + UUID.randomUUID(),
                "db.user", "sa", "db.password", "encrypted", "db.connection.pool.max.size", 4));
        this.context = mock(ComponentContext.class);
        this.registry = mock(BundleContext.class);
        when(this.context.getBundleContext()).thenReturn(this.registry);
        this.databaseReference = mock(ServiceReference.class);
        when(this.databaseReference.getBundle()).thenReturn(mock(Bundle.class));
        when(this.databaseReference.getProperty("kura.service.pid")).thenReturn("test.db");
        when(this.registry.getServiceReferences(isNull(String.class), anyString()))
                .thenReturn(new ServiceReference<?>[] { this.databaseReference });
        when(this.registry.getService(this.databaseReference)).thenReturn(this.database);
        this.transport = new TestTransport();
        this.transport.setCloudConnectionStatusService(mock(CloudConnectionStatusService.class));
        this.transport.start(this.context, Map.of("broker-url", this.brokerUri, "client-id", "test-client",
                "topic.context.account-name", "test-account", "keep-alive", 30, "timeout", 3,
                "clean-session", true, "protocol-version", 4, "in-flight.persistence", "memory"));
        this.dataService = new DataServiceImpl();
        this.dataService.setDataTransportService(this.transport);
        this.dataService.setCloudConnectionStatusService(mock(CloudConnectionStatusService.class));
        this.dataService.setWatchdogService(mock(WatchdogService.class));
        this.dataService.activate(this.context, Map.of("kura.service.pid", DATA_PID, "store.db.service.pid", "test.db",
                "connect.auto-on-startup", false, "enable.rate.limit", false, "disconnect.quiesce-timeout", 0));
        this.active = true;
        this.executors = List.of((ExecutorService) TestUtil.getFieldValue(this.dataService, "publisherExecutor"),
                (ExecutorService) TestUtil.getFieldValue(this.dataService, "connectionMonitorExecutor"),
                (ExecutorService) TestUtil.getFieldValue(this.dataService, "congestionExecutor"));
    }

    @AfterEach
    protected void stopPipeline() throws Exception {
        try {
            if (this.active) {
                this.dataService.deactivate(this.context);
                for (ExecutorService executor : this.executors) {
                    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Owned executor did not stop");
                }
                verify(this.registry).ungetService(this.databaseReference);
            }
        } finally {
            try {
                if (this.transport != null) {
                    this.transport.stop(this.context);
                }
            } finally {
                if (this.database != null) {
                    this.database.deactivate();
                }
            }
        }
    }

    @AfterAll
    protected void stopBroker() {
        if (this.broker != null) {
            this.broker.stopServer();
        }
    }

    private static class TestTransport extends MqttDataTransport {
        void start(ComponentContext context, Map<String, Object> properties) {
            super.activate(context, properties);
        }

        void stop(ComponentContext context) {
            super.deactivate(context);
        }
    }
}
