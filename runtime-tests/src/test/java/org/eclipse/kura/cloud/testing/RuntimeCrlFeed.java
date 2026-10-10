/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.cert.CertStore;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;
import org.eclipse.kura.core.testutil.pki.TestCA;
import org.eclipse.kura.core.testutil.pki.TestCA.CRLCreationOptions;
import org.eclipse.kura.testing.osgi.EquinoxRuntime;
import org.eclipse.kura.testing.osgi.EquinoxRuntime.Service;

/** HTTP CRL source and observers; actual Kura download/cache/EventAdmin/SSL code remains untouched. */
final class RuntimeCrlFeed implements AutoCloseable {
    private final TestCA ca;
    private final X509Certificate certificate;
    private final HttpServer server;
    private final AtomicReference<byte[]> content = new AtomicReference<>();
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicInteger events = new AtomicInteger();
    private final AtomicInteger sslUpdates = new AtomicInteger();
    private final AutoCloseable eventObserver;
    private final AutoCloseable sslObserver;
    private Path cacheFile;

    RuntimeCrlFeed(EquinoxRuntime runtime, TestCA ca, X509Certificate certificate, String keyPid) throws Exception {
        this.ca = ca;
        this.certificate = certificate;
        content.set(ca.generateCRL(CRLCreationOptions.builder().build()).getEncoded());
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/runtime.crl", exchange -> {
            requests.incrementAndGet();
            byte[] bytes = content.get();
            exchange.getResponseHeaders().set("Content-Type", "application/pkix-crl");
            exchange.sendResponseHeaders(200, bytes.length);
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
        });
        eventObserver = runtime.register("org.osgi.service.event", "org.osgi.service.event.EventHandler",
                (proxy, method, args) -> {
                    if (method.getName().equals("handleEvent")) {
                        Object sender = EquinoxRuntime.invoke(runtime.bundle("org.osgi.service.event")
                                .loadClass("org.osgi.service.event.Event"), args[0], "getProperty", "sender.pid");
                        if (keyPid.equals(sender)) { events.incrementAndGet(); }
                        return null;
                    }
                    return identity(proxy, method.getName(), args);
                }, Map.of("event.topics", new String[] { "org/eclipse/kura/security/keystore/KeystoreChangedEvent/KEYSTORE_CHANGED" }));
        sslObserver = runtime.register("org.eclipse.kura.api", "org.eclipse.kura.ssl.SslServiceListener",
                (proxy, method, args) -> {
                    if (method.getName().equals("onConfigurationUpdated")) { sslUpdates.incrementAndGet(); return null; }
                    return identity(proxy, method.getName(), args);
                }, Map.of());
        server.start();
    }

    Map<String, Object> options(Path file) {
        cacheFile = file;
        return Map.of("crl.management.enabled", true, "crl.urls", new String[] {
                "http://127.0.0.1:" + server.getAddress().getPort() + "/runtime.crl" },
                "crl.update.interval", 1L, "crl.update.interval.time.unit", "SECONDS",
                "crl.check.interval", 1L, "crl.check.interval.time.unit", "SECONDS",
                "crl.store.path", file.toString(), "verify.crl", true);
    }

    void awaitInitial(Service keys) throws Exception {
        await(() -> requests.get() > 0 && hasCrl(keys, false), "Initial HTTP CRL was not loaded");
    }

    void revokeAndAwait(Service keys) throws Exception {
        int previousRequests = requests.get();
        int previousEvents = events.get();
        int previousUpdates = sslUpdates.get();
        ca.revokeCertificate(certificate);
        content.set(ca.generateCRL(CRLCreationOptions.builder().build()).getEncoded());
        await(() -> requests.get() > previousRequests && hasCrl(keys, true)
                && events.get() > previousEvents && sslUpdates.get() > previousUpdates,
                "Revoked CRL did not traverse HTTP cache, EventAdmin and SSL listeners");
        await(() -> java.nio.file.Files.isRegularFile(cacheFile) && java.nio.file.Files.size(cacheFile) > 0,
                "CRL cache metadata was not persisted");
    }

    private boolean hasCrl(Service keys, boolean revoked) throws Exception {
        CertStore store = (CertStore) keys.call("getCRLStore");
        return store.getCRLs(null).stream().filter(X509CRL.class::isInstance).map(X509CRL.class::cast)
                .anyMatch(crl -> crl.getIssuerX500Principal().equals(certificate.getIssuerX500Principal())
                        && crl.isRevoked(certificate) == revoked);
    }

    private static Object identity(Object proxy, String method, Object[] args) {
        return switch (method) {
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            case "toString" -> "CRL runtime observer";
            default -> throw new AssertionError(method);
        };
    }

    private static void await(Callable<Boolean> condition, String message) throws Exception {
        // Local CRLManager retains a minimum 30-second reschedule interval.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (!condition.call() && System.nanoTime() < deadline) { Thread.sleep(25); }
        assertTrue(condition.call(), message);
    }

    @Override public void close() throws Exception {
        server.stop(0);
        try { eventObserver.close(); } finally { sslObserver.close(); }
    }
}
