package dev.raftkv.sim;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class RandomizedSimulationTest {
    private static final int SEEDS = 1000;
    private static final int NODES = 5;
    private static final int CLIENTS = 3;
    private static final long CHAOS_MS = 5_000;

    @Test
    void safetyHoldsUnderRandomFaults() {
        String single = System.getProperty("seed");
        if (single != null) {
            runSeed(Long.parseLong(single));
            return;
        }
        int seeds = Integer.getInteger("seeds", SEEDS);
        for (long seed = 1; seed <= seeds; seed++) {
            runSeed(seed);
        }
    }

    private void runSeed(long seed) {
        try {
            simulate(seed);
        } catch (Throwable t) {
            System.err.println("Simulation FAILED for seed " + seed + " (rerun with -Dseed=" + seed + ")");
            throw new AssertionError("seed " + seed + ": " + t.getMessage(), t);
        }
    }

    private void simulate(long seed) {
        SimCluster cluster = new SimCluster(seed, NODES, 40);
        Map<String, String> acked = new HashMap<>();
        List<SimClient> clients = new ArrayList<>();
        for (int i = 0; i < CLIENTS; i++) {
            SimClient c = new SimClient(cluster, "c" + i, acked);
            clients.add(c);
            c.start();
        }

        long end = cluster.now() + CHAOS_MS;
        while (cluster.now() < end) {
            cluster.runFor(50 + cluster.random.nextInt(250));
            injectFault(cluster);
            cluster.checkCommittedEntriesUnchanged();
        }

        long appliedBeforeHeal = cluster.highestAppliedIndex();
        clients.forEach(SimClient::stop);
        cluster.heal();
        cluster.nodes().forEach(n -> cluster.restart(n.id));
        boolean converged = cluster.runUntil(() -> clients.stream().allMatch(SimClient::idle) && converged(cluster), 30_000);
        if (!converged) {
            fail("cluster did not converge after healing: " + cluster.nodes().stream().map(n -> n.raft.toString()).toList());
        }
        cluster.checkCommittedEntriesUnchanged();

        for (SimCluster.SimNode n : cluster.nodes()) {
            assertThat(n.raft.lastApplied()).as("%s lost committed entries", n.id).isGreaterThanOrEqualTo(appliedBeforeHeal);
        }

        Map<String, String> reference = cluster.nodes().get(0).kv.asMap();
        for (SimCluster.SimNode n : cluster.nodes()) {
            assertThat(n.kv.asMap()).as("state of %s", n.id).isEqualTo(reference);
        }

        SimCluster.SimNode leader = cluster.leader();
        CompletableFuture<Long> read = leader.raft.readIndex();
        cluster.runUntil(read::isDone, 5_000);
        assertThat(read).as("ReadIndex on the leader").isCompleted();
        for (Map.Entry<String, String> w : acked.entrySet()) {
            assertThat(leader.kv.get(w.getKey())).as("acknowledged write %s", w.getKey()).isEqualTo(w.getValue());
        }
        if (Boolean.getBoolean("verbose")) {
            System.out.printf("seed %d: %d acked writes, leader %s, %d InstallSnapshot RPCs%n",
                    seed, acked.size(), leader.raft, cluster.installSnapshotsDelivered);
        }
    }

    private static boolean converged(SimCluster cluster) {
        SimCluster.SimNode leader = cluster.leader();
        if (leader == null) {
            return false;
        }
        long commit = leader.raft.commitIndex();
        return cluster.nodes().stream().allMatch(n -> n.raft.lastApplied() == commit
                && n.raft.currentTerm() == leader.raft.currentTerm());
    }

    private static void injectFault(SimCluster cluster) {
        List<SimCluster.SimNode> nodes = cluster.nodes();
        SimCluster.SimNode victim = nodes.get(cluster.random.nextInt(nodes.size()));
        int r = cluster.random.nextInt(100);
        if (r < 15) {
            cluster.crash(victim.id);
        } else if (r < 35) {
            cluster.restart(victim.id);
        } else if (r < 50) {
            Map<String, Integer> groups = new HashMap<>();
            nodes.forEach(n -> groups.put(n.id, cluster.random.nextInt(2)));
            cluster.partition(groups);
        } else if (r < 65) {
            cluster.heal();
        } else if (r < 80) {
            cluster.setDropRate(cluster.random.nextDouble() * 0.3);
        }
    }
}
