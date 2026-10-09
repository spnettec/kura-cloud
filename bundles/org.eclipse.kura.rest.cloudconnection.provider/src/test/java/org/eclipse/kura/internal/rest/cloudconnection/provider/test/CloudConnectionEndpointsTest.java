/*******************************************************************************
 * Copyright (c) 2023, 2026 Eurotech and/or its affiliates and others
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
package org.eclipse.kura.internal.rest.cloudconnection.provider.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.kura.cloudconnection.CloudConnectionConstants;
import org.eclipse.kura.cloudconnection.CloudConnectionManager;
import org.eclipse.kura.cloudconnection.factory.CloudConnectionFactory;
import org.eclipse.kura.cloudconnection.message.KuraMessage;
import org.eclipse.kura.cloudconnection.publisher.CloudPublisher;
import org.eclipse.kura.cloudconnection.request.RequestHandlerMessageConstants;
import org.eclipse.kura.cloudconnection.subscriber.CloudSubscriber;
import org.eclipse.kura.configuration.ComponentConfiguration;
import org.eclipse.kura.configuration.ConfigurationService;
import org.eclipse.kura.core.configuration.ComponentConfigurationImpl;
import org.eclipse.kura.crypto.CryptoService;
import org.eclipse.kura.internal.rest.cloudconnection.provider.CloudConnectionRestService;
import org.eclipse.kura.message.KuraPayload;
import org.eclipse.kura.message.KuraResponsePayload;
import org.eclipse.kura.request.handler.jaxrs.JaxRsRequestHandlerProxy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.runtime.ServiceComponentRuntime;
import org.osgi.service.component.runtime.dto.ComponentDescriptionDTO;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/** Actual request/JSON/service logic; authenticated HTTP/MQTT and SCR assembly remain runtime work. */
class CloudConnectionEndpointsTest {

    private static final String ENDPOINT = "custom.cloud";
    private static final String FACTORY = "test.cloud.factory";
    private static final String PUBLISHER_FACTORY = "test.publisher.factory";
    private static final String SUBSCRIBER_FACTORY = "test.subscriber.factory";
    private final ConfigurationService configurations = mock(ConfigurationService.class);
    private final CloudConnectionFactory factory = mock(CloudConnectionFactory.class);
    private final CloudConnectionManager manager = mock(CloudConnectionManager.class);
    private final ServiceComponentRuntime scr = mock(ServiceComponentRuntime.class);
    private final BundleContext context = mock(BundleContext.class);
    private final List<Registration> registrations = new ArrayList<>();
    private final Gson gson = new Gson();
    private MockedStatic<FrameworkUtil> framework;
    private JaxRsRequestHandlerProxy proxy;

    private record Registration(ServiceReference<?> reference, Object service, Map<String, Object> properties) {
    }

    @BeforeEach
    void setUp() throws Exception {
        Bundle bundle = mock(Bundle.class);
        when(bundle.getBundleContext()).thenReturn(this.context);
        this.framework = mockStatic(FrameworkUtil.class, CALLS_REAL_METHODS);
        this.framework.when(() -> FrameworkUtil.getBundle(any(Class.class))).thenReturn(bundle);
        when(this.context.getAllServiceReferences(nullable(String.class), nullable(String.class)))
                .thenAnswer(invocation -> matching(invocation.getArgument(0), invocation.getArgument(1)).toArray(ServiceReference<?>[]::new));
        when(this.context.getServiceReferences(any(Class.class), nullable(String.class)))
                .thenAnswer(invocation -> matching(((Class<?>) invocation.getArgument(0)).getName(), invocation.getArgument(1)));
        when(this.context.getServiceReference(any(Class.class)))
                .thenAnswer(invocation -> matching(((Class<?>) invocation.getArgument(0)).getName(), null).stream().findFirst().orElse(null));
        when(this.context.getService(any())).thenAnswer(invocation -> this.registrations.stream()
                .filter(registration -> registration.reference() == invocation.getArgument(0))
                .map(Registration::service).findFirst().orElse(null));
        register(CloudConnectionFactory.class, "factory.service", this.factory,
                Map.of("kura.ui.csf.pid.default", "custom.cloud", "kura.ui.csf.pid.regex", ".*"));
        register(CloudConnectionManager.class, ENDPOINT, this.manager, Map.of());
        register(ConfigurationService.class, "configuration.service", this.configurations, Map.of());
        register(ServiceComponentRuntime.class, "scr", this.scr, Map.of());
        register(CloudPublisher.class, "test.publisher", mock(CloudPublisher.class), pubsubProperties(PUBLISHER_FACTORY));
        register(CloudSubscriber.class, "test.subscriber", mock(CloudSubscriber.class), pubsubProperties(SUBSCRIBER_FACTORY));
        when(this.factory.getFactoryPid()).thenReturn(FACTORY);
        when(this.factory.getManagedCloudConnectionPids()).thenReturn(Set.of(ENDPOINT));
        when(this.factory.getStackComponentsPids(ENDPOINT)).thenReturn(List.of(ENDPOINT, "data.123", "transport.123"));
        when(this.scr.getComponentDescriptionDTOs()).thenReturn(List.of(description(PUBLISHER_FACTORY, CloudPublisher.class),
                description(SUBSCRIBER_FACTORY, CloudSubscriber.class)));
        CloudConnectionRestService endpoint = new CloudConnectionRestService();
        endpoint.bindConfigurationService(this.configurations);
        endpoint.bindCryptoService(mock(CryptoService.class));
        endpoint.activate();
        this.proxy = new JaxRsRequestHandlerProxy(endpoint);
    }

    @AfterEach
    void closeFrameworkScope() {
        this.framework.close();
    }

    @Test
    void shouldGetCloudComponentInstances() throws Exception {
        JsonObject response = request("GET", "/instances", null);
        assertEquals(1, response.getAsJsonArray("cloudEndpointInstances").size());
        JsonObject endpoint = response.getAsJsonArray("cloudEndpointInstances").get(0).getAsJsonObject();
        assertEquals(ENDPOINT, endpoint.get("cloudEndpointPid").getAsString());
        assertEquals(FACTORY, endpoint.get("cloudConnectionFactoryPid").getAsString());
        assertEquals("DISCONNECTED", endpoint.get("state").getAsString());
        Set<String> pubsubPids = new HashSet<>();
        response.getAsJsonArray("pubsubInstances").forEach(value -> pubsubPids.add(value.getAsJsonObject().get("pid").getAsString()));
        assertEquals(Set.of("test.publisher", "test.subscriber"), pubsubPids);
    }

    @Test
    void shouldGetStackComponentPids() throws Exception {
        JsonObject response = request("POST", "/cloudEndpoint/stackComponentPids", endpointRequest(ENDPOINT));
        Set<String> pids = new HashSet<>();
        response.getAsJsonArray("pids").forEach(value -> pids.add(value.getAsString()));
        assertEquals(Set.of(ENDPOINT, "data.123", "transport.123"), pids);
    }

    @Test
    void shouldCreateCloudEndpoint() throws Exception {
        request("POST", "/cloudEndpoint", endpointRequest("custom.created"));
        verify(this.factory).createConfiguration("custom.created");
    }

    @Test
    void shouldDeleteCloudEndpoint() throws Exception {
        request("DELETE", "/cloudEndpoint", endpointRequest(ENDPOINT));
        verify(this.factory).deleteConfiguration(ENDPOINT);
    }

    @Test
    void shouldGetCloudComponentFactories() throws Exception {
        JsonObject response = request("GET", "/factories", null);
        assertEquals(1, response.getAsJsonArray("cloudConnectionFactories").size());
        assertEquals(FACTORY, response.getAsJsonArray("cloudConnectionFactories").get(0).getAsJsonObject()
                .get("cloudConnectionFactoryPid").getAsString());
        Set<String> pids = new HashSet<>();
        response.getAsJsonArray("pubSubFactories").forEach(value -> pids.add(value.getAsJsonObject().get("factoryPid").getAsString()));
        assertEquals(Set.of(PUBLISHER_FACTORY, SUBSCRIBER_FACTORY), pids);
    }

    @Test
    void shouldCreatePublisherInstance() throws Exception {
        createPubSub("new.publisher", PUBLISHER_FACTORY);
    }

    @Test
    void shouldCreateSubscriberInstance() throws Exception {
        createPubSub("new.subscriber", SUBSCRIBER_FACTORY);
    }

    @Test
    void shouldDeletePublisherInstance() throws Exception {
        request("DELETE", "/pubSub", Map.of("pid", "test.publisher"));
        verify(this.configurations).deleteFactoryConfiguration("test.publisher", true);
    }

    @Test
    void shouldDeleteSubscriberInstance() throws Exception {
        request("DELETE", "/pubSub", Map.of("pid", "test.subscriber"));
        verify(this.configurations).deleteFactoryConfiguration("test.subscriber", true);
    }

    @Test
    void shouldGetConfigurations() throws Exception {
        when(this.configurations.getComponentConfiguration("transport.123"))
                .thenReturn(new ComponentConfigurationImpl("transport.123", null, Map.of("username", "local.user")));
        when(this.configurations.getComponentConfiguration("test.publisher"))
                .thenReturn(new ComponentConfigurationImpl("test.publisher", null, Map.of("app.id", "设备")));
        JsonObject response = request("POST", "/configurations", Map.of("pids", List.of("transport.123", "test.publisher")));
        Map<String, JsonObject> configs = new HashMap<>();
        response.getAsJsonArray("configs").forEach(value -> configs.put(value.getAsJsonObject().get("pid").getAsString(), value.getAsJsonObject()));
        assertEquals(Set.of("transport.123", "test.publisher"), configs.keySet());
        assertEquals("local.user", configs.get("transport.123").getAsJsonObject("properties").getAsJsonObject("username").get("value").getAsString());
        assertEquals("设备", configs.get("test.publisher").getAsJsonObject("properties").getAsJsonObject("app.id").get("value").getAsString());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldUpdateStackComponentConfigurations() throws Exception {
        request("PUT", "/configurations", Map.of("configs", List.of(Map.of("pid", "transport.123", "properties",
                Map.of("username", Map.of("type", "STRING", "value", "updated.user"),
                        "keep-alive", Map.of("type", "INTEGER", "value", 30)))), "takeSnapshot", true));
        ArgumentCaptor<List<ComponentConfiguration>> configs = ArgumentCaptor.forClass(List.class);
        verify(this.configurations).updateConfigurations(configs.capture(), eq(true));
        assertEquals(1, configs.getValue().size());
        assertEquals("transport.123", configs.getValue().getFirst().getPid());
        assertEquals(Map.of("username", "updated.user", "keep-alive", 30), configs.getValue().getFirst().getConfigurationProperties());
    }

    @Test
    void shouldConnectEndpoint() throws Exception {
        request("POST", "/cloudEndpoint/connect", Map.of("cloudEndpointPid", ENDPOINT));
        verify(this.manager).connect();
    }

    @Test
    void shouldDisconnectEndpoint() throws Exception {
        request("POST", "/cloudEndpoint/disconnect", Map.of("cloudEndpointPid", ENDPOINT));
        verify(this.manager).disconnect();
    }

    @Test
    void shouldCheckEndpointStatus() throws Exception {
        assertEquals(false, request("POST", "/cloudEndpoint/isConnected", Map.of("cloudEndpointPid", ENDPOINT)).get("connected").getAsBoolean());
        when(this.manager.isConnected()).thenReturn(true);
        assertEquals(true, request("POST", "/cloudEndpoint/isConnected", Map.of("cloudEndpointPid", ENDPOINT)).get("connected").getAsBoolean());
    }

    private void createPubSub(String pid, String factoryPid) throws Exception {
        request("POST", "/pubSub", Map.of("pid", pid, "factoryPid", factoryPid, "cloudEndpointPid", ENDPOINT));
        verify(this.configurations).createFactoryConfiguration(factoryPid, pid,
                Map.of(CloudConnectionConstants.CLOUD_ENDPOINT_SERVICE_PID_PROP_NAME.value(), ENDPOINT), true);
    }

    private Map<String, String> endpointRequest(String pid) {
        return Map.of("cloudConnectionFactoryPid", FACTORY, "cloudEndpointPid", pid);
    }

    private Map<String, Object> pubsubProperties(String factoryPid) {
        return Map.of(ConfigurationAdmin.SERVICE_FACTORYPID, factoryPid,
                CloudConnectionConstants.CLOUD_ENDPOINT_SERVICE_PID_PROP_NAME.value(), ENDPOINT);
    }

    private ComponentDescriptionDTO description(String factoryPid, Class<?> serviceInterface) {
        ComponentDescriptionDTO description = new ComponentDescriptionDTO();
        description.name = factoryPid;
        description.serviceInterfaces = new String[] { serviceInterface.getName() };
        description.properties = Map.of("service.pid", factoryPid,
                CloudConnectionConstants.CLOUD_CONNECTION_FACTORY_PID_PROP_NAME.value(), FACTORY);
        return description;
    }

    private void register(Class<?> serviceInterface, String pid, Object service, Map<String, Object> additional) {
        Map<String, Object> properties = new HashMap<>(additional);
        properties.put(Constants.OBJECTCLASS, new String[] { serviceInterface.getName() });
        properties.put(ConfigurationService.KURA_SERVICE_PID, pid);
        ServiceReference<?> reference = mock(ServiceReference.class);
        when(reference.getProperty(anyString())).thenAnswer(invocation -> properties.get(invocation.getArgument(0)));
        this.registrations.add(new Registration(reference, service, properties));
    }

    private List<ServiceReference<?>> matching(String className, String filter) throws Exception {
        List<ServiceReference<?>> matches = new ArrayList<>();
        for (Registration registration : this.registrations) {
            boolean classMatches = className == null || Arrays.asList((String[]) registration.properties().get(Constants.OBJECTCLASS)).contains(className);
            if (classMatches && (filter == null || FrameworkUtil.createFilter(filter).matches(registration.properties()))) {
                matches.add(registration.reference());
            }
        }
        return matches;
    }

    private JsonObject request(String method, String path, Object body) throws Exception {
        KuraPayload payload = new KuraPayload();
        if (body != null) {
            payload.setBody(this.gson.toJson(body).getBytes(StandardCharsets.UTF_8));
        }
        KuraMessage request = new KuraMessage(payload);
        request.getProperties().put(RequestHandlerMessageConstants.ARGS_KEY.value(), Arrays.asList(path.substring(1).split("/")));
        KuraMessage response = switch (method) {
        case "GET" -> this.proxy.doGet(null, request);
        case "POST" -> this.proxy.doPost(null, request);
        case "PUT" -> this.proxy.doPut(null, request);
        case "DELETE" -> this.proxy.doDel(null, request);
        default -> throw new AssertionError(method);
        };
        KuraResponsePayload status = new KuraResponsePayload(response.getPayload());
        assertEquals(200, status.getResponseCode(), status::getExceptionStack);
        byte[] content = response.getPayload().getBody();
        return content == null || content.length == 0 ? new JsonObject()
                : JsonParser.parseString(new String(content, StandardCharsets.UTF_8)).getAsJsonObject();
    }
}
