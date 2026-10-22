package dev.raftkv.sim;

import dev.raftkv.storage.FileStorage;
import dev.raftkv.storage.MemoryStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RestartTest {
    @TempDir
    Path dir;

    @Test
    void noAcknowledgedWriteIsLostAcrossRestarts() {
        for (long seed = 1; seed <= 3; seed++) {
            Path seedDir = dir.resolve("seed" + seed);
            SimCluster cluster = new SimCluster(seed, 3, 50, (id, old) -> {
                if (old instanceof FileStorage f) {
                    f.close();
                }
                return new FileStorage(seedDir.resolve(id));
            });
            runScenario(cluster, seed);
        }
    }

    private static void runScenario(SimCluster cluster, long seed) {
        Map<String, String> acked = new HashMap<>();
        List<SimClient> clients = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            SimClient c = new SimClient(cluster, "c" + i, acked);
            clients.add(c);
            c.start();
        }

        for (int round = 0; round < 12; round++) {
            cluster.runFor(250);

            SimCluster.SimNode leader = cluster.leader();
            String victim = leader != null && round % 2 == 0
                    ? leader.id
                    : cluster.nodes().get(cluster.random.nextInt(3)).id;
            cluster.crash(victim);
            cluster.runFor(50 + cluster.random.nextInt(200));
            cluster.restart(victim);
        }

        clients.forEach(SimClient::stop);
        boolean done = cluster.runUntil(() -> clients.stream().allMatch(SimClient::idle)
                && cluster.leader() != null
                && cluster.nodes().stream().allMatch(n -> n.raft.lastApplied() == cluster.leader().raft.commitIndex()), 20_000);
        assertThat(done).as("seed %d converged", seed).isTrue();
        assertThat(acked).as("seed %d made progress", seed).hasSizeGreaterThan(20);

        for (SimCluster.SimNode n : cluster.nodes()) {
            for (Map.Entry<String, String> w : acked.entrySet()) {
                assertThat(n.kv.get(w.getKey())).as("seed %d: %s has acked write %s", seed, n.id, w.getKey())
                        .isEqualTo(w.getValue());
            }
        }
        cluster.nodes().forEach(n -> {
            MemoryStorage s = n.storage;
            if (s instanceof FileStorage f) {
                f.close();
            }
        });
    }
}
