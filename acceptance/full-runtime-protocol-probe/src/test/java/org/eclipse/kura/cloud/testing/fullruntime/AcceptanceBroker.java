/* SPDX-License-Identifier: EPL-2.0 */
package org.eclipse.kura.cloud.testing.fullruntime;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.gson.Gson;
import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;

/** Loopback broker in a separate owned JVM, using the existing runtime-test configuration. */
public final class AcceptanceBroker {
    public static void main(String[] args) throws Exception {
        Path output = Path.of(args[0]);
        var clients = new ConcurrentSkipListSet<String>();
        Server broker = new Server();
        Properties options = new Properties();
        options.setProperty("host", "127.0.0.1");
        options.setProperty("port", "0");
        options.setProperty("allow_anonymous", "false");
        options.setProperty("persistence_enabled", "false");
        options.setProperty("telemetry_enabled", "false");
        options.setProperty("netty.mqtt.message_size", "1048576");
        try {
            broker.startServer(new MemoryConfig(options), List.of(), null, (id, user, password) -> {
                boolean accepted = "complete-mac-user".equals(user)
                        && Arrays.equals("isolated-test-password".getBytes(StandardCharsets.UTF_8), password);
                if (accepted) { clients.add(id); }
                return accepted;
            }, null);
            int port = 0;
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
            while (port == 0 && System.nanoTime() < deadline) {
                try { port = broker.getPort(); } catch (ConcurrentModificationException binding) { /* bounded startup */ }
                if (port == 0) { Thread.sleep(25); }
            }
            if (port == 0) { throw new IllegalStateException("Broker did not bind"); }
            Files.writeString(output, new Gson().toJson(java.util.Map.of("uri", "tcp://127.0.0.1:" + port)));
            while (System.in.read() != -1) { /* controller closes stdin after application cleanup */ }
        } finally {
            broker.stopServer();
            Files.writeString(output.resolveSibling("broker-final.json"),
                    new Gson().toJson(java.util.Map.of("authenticatedClients", clients, "stopped", true)));
        }
    }
}
