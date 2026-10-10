/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.eclipse.kura.testing.osgi.EquinoxRuntime;

/** Exercises the request handler registered by the real SCR resource against real factories. */
final class CloudRestRuntimeScenario {
    private static final String REST = "org.eclipse.kura.internal.rest.cloudconnection.provider.CloudConnectionRestService";
    private static final String FACTORY = "org.eclipse.kura.cloud.CloudService";
    private static final String PID = "fixture.rest.custom.cloud";
    private static final String PUB = "fixture.rest.publisher";
    private static final String PUB_FACTORY = "org.eclipse.kura.cloud.publisher.CloudPublisher";
    private static final String SUB = "fixture.rest.subscriber";
    private static final String SUB_FACTORY = "org.eclipse.kura.cloud.subscriber.CloudSubscriber";
    private static final Gson JSON = new Gson();

    static void run(EquinoxRuntime runtime) throws Exception {
        try (var store = runtime.service("org.apache.felix.useradmin.RoleRepositoryStore", null, Duration.ofSeconds(10))) {
            assertEquals("org.eclipse.kura.useradmin.store", store.provider().getSymbolicName());
        }
        var restBundle = runtime.install(java.nio.file.Path.of("target/it-bundles/org.eclipse.kura.rest.cloudconnection.provider.jar"));
        runtime.resolve(java.util.List.of(restBundle));
        runtime.start(java.util.List.of(restBundle));
        // The request ingress runs inside the container's Jersey class space.
        ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(runtime.bundle("org.glassfish.jersey.core.jersey-common")
                .adapt(org.osgi.framework.wiring.BundleWiring.class).getClassLoader());
        var api = runtime.bundle("org.eclipse.kura.api");
        AtomicReference<Object> handler = new AtomicReference<>();
        try (var registry = runtime.register("org.eclipse.kura.api",
                "org.eclipse.kura.cloudconnection.request.RequestHandlerRegistry", (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "registerRequestHandler" -> { if ("CLD-V1".equals(args[0])) { handler.set(args[1]); } }
                        case "unregister" -> { if ("CLD-V1".equals(args[0])) { handler.set(null); } }
                        case "hashCode" -> { return System.identityHashCode(proxy); }
                        case "equals" -> { return proxy == args[0]; }
                        case "toString" -> { return "isolated request ingress"; }
                        default -> throw new AssertionError(method);
                    }
                    return null;
                }, Map.of());
             var resource = runtime.service(REST, null, Duration.ofSeconds(10));
             var userAdmin = runtime.service("org.osgi.service.useradmin.UserAdmin", null, Duration.ofSeconds(10))) {
            assertEquals("org.eclipse.kura.rest.cloudconnection.provider", resource.provider().getSymbolicName());
            await(() -> userAdmin.call("getRole", "kura.permission.rest.cloudconnection") != null,
                    "REST permission role was not created in " + userAdmin.provider().getSymbolicName());
            await(() -> handler.get() != null, "SCR did not register CLD-V1");
            JsonObject factories = request(api, handler.get(), "GET", "/factories", null);
            Set<String> factoryIds = new HashSet<>();
            factories.getAsJsonArray("cloudConnectionFactories").forEach(v ->
                    factoryIds.add(v.getAsJsonObject().get("cloudConnectionFactoryPid").getAsString()));
            assertTrue(factoryIds.contains(FACTORY));
            assertTrue(factoryIds.contains("org.eclipse.kura.cloudconnection.sparkplug.mqtt.endpoint.SparkplugCloudEndpoint"));
            Set<String> pubFactories = new HashSet<>();
            factories.getAsJsonArray("pubSubFactories").forEach(v ->
                    pubFactories.add(v.getAsJsonObject().get("factoryPid").getAsString()));
            assertTrue(pubFactories.contains(PUB_FACTORY), "Real SCR publisher description not discovered");
            assertTrue(pubFactories.contains(SUB_FACTORY), "Real SCR subscriber description not discovered");
            Map<String, Object> endpoint = Map.of("cloudConnectionFactoryPid", FACTORY, "cloudEndpointPid", PID);
            request(api, handler.get(), "POST", "/cloudEndpoint", endpoint);
            try (var cloud = runtime.service("org.eclipse.kura.cloudconnection.CloudConnectionManager",
                    "(kura.service.pid=" + PID + ")", Duration.ofSeconds(10))) {
                assertEquals(PID, cloud.property("kura.service.pid"));
                JsonObject instances = request(api, handler.get(), "GET", "/instances", null);
                assertEquals(1, instances.getAsJsonArray("cloudEndpointInstances").size());
                JsonObject instance = instances.getAsJsonArray("cloudEndpointInstances").get(0).getAsJsonObject();
                assertEquals(PID, instance.get("cloudEndpointPid").getAsString());
                assertEquals("DISCONNECTED", instance.get("state").getAsString());
                assertFalse(request(api, handler.get(), "POST", "/cloudEndpoint/isConnected",
                        Map.of("cloudEndpointPid", PID)).get("connected").getAsBoolean());
                Set<String> stack = new HashSet<>();
                request(api, handler.get(), "POST", "/cloudEndpoint/stackComponentPids", endpoint)
                        .getAsJsonArray("pids").forEach(v -> stack.add(v.getAsString()));
                assertEquals(3, stack.size());
                assertTrue(stack.contains(PID));
                request(api, handler.get(), "POST", "/pubSub",
                        Map.of("pid", PUB, "factoryPid", PUB_FACTORY, "cloudEndpointPid", PID));
                request(api, handler.get(), "POST", "/pubSub",
                        Map.of("pid", SUB, "factoryPid", SUB_FACTORY, "cloudEndpointPid", PID));
                try (var publisher = runtime.service("org.eclipse.kura.cloudconnection.publisher.CloudPublisher",
                        "(kura.service.pid=" + PUB + ")", Duration.ofSeconds(10));
                     var subscriber = runtime.service("org.eclipse.kura.cloudconnection.subscriber.CloudSubscriber",
                        "(kura.service.pid=" + SUB + ")", Duration.ofSeconds(10))) {
                    assertEquals(PID, publisher.property("cloud.endpoint.service.pid"));
                    assertEquals(PID, subscriber.property("cloud.endpoint.service.pid"));
                    assertEquals(2, request(api, handler.get(), "GET", "/instances", null)
                            .getAsJsonArray("pubsubInstances").size());
                    stack.add(PUB);
                    stack.add(SUB);
                    JsonObject configurations = request(api, handler.get(), "POST", "/configurations", Map.of("pids", stack));
                    Set<String> actual = new HashSet<>();
                    configurations.getAsJsonArray("configs").forEach(v -> actual.add(v.getAsJsonObject().get("pid").getAsString()));
                    assertEquals(stack, actual);
                    request(api, handler.get(), "PUT", "/configurations",
                            Map.of("configs", java.util.List.of(Map.of("pid", PID, "properties",
                                    Map.of("kura.cloud.factory.name", Map.of("type", "STRING", "value", "REST 中文连接"),
                                            "kura.cloud.factory.desc", Map.of("type", "STRING", "value", "保留独立服务标识")))), "takeSnapshot", true));
                    await(() -> "REST 中文连接".equals(cloud.property("kura.cloud.factory.name")),
                            "REST update did not reach actual cloud configuration");
                    assertEquals("保留独立服务标识", cloud.property("kura.cloud.factory.desc"));
                    assertEquals(PID, cloud.property("kura.service.pid"));
                }
                request(api, handler.get(), "DELETE", "/pubSub", Map.of("pid", PUB));
                request(api, handler.get(), "DELETE", "/pubSub", Map.of("pid", SUB));
                await(() -> !runtime.hasService("org.eclipse.kura.cloudconnection.publisher.CloudPublisher",
                        "(kura.service.pid=" + PUB + ")"), "REST publisher deletion did not reach SCR");
                request(api, handler.get(), "DELETE", "/cloudEndpoint", endpoint);
                for (String pid : stack) {
                    await(() -> !runtime.hasService("org.eclipse.kura.configuration.ConfigurableComponent",
                            "(kura.service.pid=" + pid + ")"), "REST factory deletion left " + pid);
                }
            }
            JsonObject empty = request(api, handler.get(), "GET", "/instances", null);
            assertTrue(empty.getAsJsonArray("cloudEndpointInstances").isEmpty());
            assertTrue(empty.getAsJsonArray("pubsubInstances").isEmpty());
        } finally {
            Thread.currentThread().setContextClassLoader(previousLoader);
        }
    }

    @SuppressWarnings("unchecked")
    private static JsonObject request(org.osgi.framework.Bundle api, Object handler, String method, String path, Object body) throws Exception {
        Class<?> payloadType = api.loadClass("org.eclipse.kura.message.KuraPayload");
        Object payload = payloadType.getConstructor().newInstance();
        if (body != null) { EquinoxRuntime.invoke(payloadType, payload, "setBody", JSON.toJson(body).getBytes(StandardCharsets.UTF_8)); }
        Class<?> messageType = api.loadClass("org.eclipse.kura.cloudconnection.message.KuraMessage");
        Object message = messageType.getConstructor(payloadType).newInstance(payload);
        Class<?> constants = api.loadClass("org.eclipse.kura.cloudconnection.request.RequestHandlerMessageConstants");
        Object argsKey = EquinoxRuntime.invoke(constants, constants.getField("ARGS_KEY").get(null), "value");
        ((Map<Object, Object>) EquinoxRuntime.invoke(messageType, message, "getProperties"))
                .put(argsKey, Arrays.asList(path.substring(1).split("/")));
        Class<?> handlerType = api.loadClass("org.eclipse.kura.cloudconnection.request.RequestHandler");
        Object response = EquinoxRuntime.invoke(handlerType, handler,
                switch (method) { case "GET" -> "doGet"; case "POST" -> "doPost"; case "DELETE" -> "doDel"; case "PUT" -> "doPut";
                    default -> throw new AssertionError(method); }, null, message);
        Object resultPayload = EquinoxRuntime.invoke(messageType, response, "getPayload");
        Class<?> responseType = api.loadClass("org.eclipse.kura.message.KuraResponsePayload");
        Object status = responseType.getConstructor(payloadType).newInstance(resultPayload);
        assertEquals(200, EquinoxRuntime.invoke(responseType, status, "getResponseCode"),
                method + " " + path + " " + EquinoxRuntime.invoke(responseType, status, "getExceptionStack")
                        + " body=" + Arrays.toString((byte[]) EquinoxRuntime.invoke(payloadType, resultPayload, "getBody")));
        byte[] content = (byte[]) EquinoxRuntime.invoke(payloadType, resultPayload, "getBody");
        return content == null || content.length == 0 ? new JsonObject()
                : JsonParser.parseString(new String(content, StandardCharsets.UTF_8)).getAsJsonObject();
    }

    private static void await(Callable<Boolean> condition, String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.call() && System.nanoTime() < deadline) { Thread.sleep(20); }
        assertTrue(condition.call(), message);
    }
}
