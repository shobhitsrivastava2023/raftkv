package dev.raftkv.kv;

import dev.raftkv.client.RaftKvClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcClusterTest {
    @TempDir
    Path dir;

    private final Map<String, String> peers = new LinkedHashMap<>();
    private final Map<String, RaftKvServer> servers = new LinkedHashMap<>();

    private void start(String id) {
        int port = Integer.parseInt(peers.get(id).split(":")[1]);
        servers.put(id, new RaftKvServer(id, port, dir.resolve(id), peers, Map.of(), 50, false));
    }

    @AfterEach
    void stopAll() {
        servers.values().forEach(RaftKvServer::close);
    }

    @Test
    void putGetDeleteFailoverAndRestart() throws Exception {
        for (String id : List.of("n1", "n2", "n3")) {
            peers.put(id, "localhost:" + freePort());
        }
        peers.keySet().forEach(this::start);

        try (RaftKvClient client = new RaftKvClient(new ArrayList<>(peers.values()))) {
            for (int i = 0; i < 100; i++) {
                client.put("k" + i, "v" + i);
            }
            assertThat(client.get("k42")).contains("v42");
            client.delete("k42");
            assertThat(client.get("k42")).isEmpty();

            String leaderId = servers.entrySet().stream()
                    .filter(e -> e.getValue().node().isLeader()).findFirst().orElseThrow().getKey();
            servers.remove(leaderId).close();
            client.put("after-failover", "yes");
            assertThat(client.get("k7")).contains("v7");

            start(leaderId);
            client.put("after-restart", "yes");
            RaftKvServer restarted = servers.get(leaderId);
            long deadline = System.currentTimeMillis() + 10_000;
            while (restarted.node().lastApplied() < restarted.node().commitIndex()
                    || restarted.node().commitIndex() == 0) {
                assertThat(System.currentTimeMillis()).as("restarted node caught up").isLessThan(deadline);
                Thread.sleep(20);
            }
            assertThat(client.get("after-restart")).contains("yes");
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
