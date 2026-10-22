package dev.raftkv.sim;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotTest {
    @Test
    void longDeadFollowerCatchesUpThroughInstallSnapshot() {
        SimCluster cluster = new SimCluster(3, 3, 20);
        cluster.runUntil(() -> cluster.leader() != null, 2_000);
        SimCluster.SimNode leader = cluster.leader();
        SimCluster.SimNode follower = cluster.nodes().stream().filter(n -> n != leader).findFirst().orElseThrow();
        cluster.crash(follower.id);
        long followerLastIndex = follower.storage.lastIndex();

        Map<String, String> acked = new HashMap<>();
        SimClient client = new SimClient(cluster, "c", acked);
        client.start();
        cluster.runFor(3_000);
        client.stop();
        cluster.runUntil(client::idle, 2_000);

        assertThat(acked).hasSizeGreaterThan(100);
        assertThat(leader.storage.snapshotIndex()).isGreaterThan(followerLastIndex + 1);
        assertThat(cluster.installSnapshotsDelivered).isZero();

        cluster.restart(follower.id);
        boolean caughtUp = cluster.runUntil(
                () -> follower.raft.lastApplied() == leader.raft.commitIndex(), 5_000);

        assertThat(caughtUp).isTrue();
        assertThat(cluster.installSnapshotsDelivered).as("caught up via InstallSnapshot").isPositive();
        assertThat(follower.storage.snapshotIndex()).isPositive();
        assertThat(follower.kv.asMap()).isEqualTo(leader.kv.asMap());
        acked.forEach((k, v) -> assertThat(follower.kv.get(k)).isEqualTo(v));
    }
}
