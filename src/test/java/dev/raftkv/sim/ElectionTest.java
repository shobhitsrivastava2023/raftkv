package dev.raftkv.sim;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ElectionTest {
    @Test
    void electsExactlyOneLeaderAndKeepsIt() {
        SimCluster cluster = new SimCluster(42, 3, 1000);
        assertThat(cluster.runUntil(() -> cluster.leader() != null, 2_000)).as("a leader is elected").isTrue();
        SimCluster.SimNode leader = cluster.leader();
        long term = leader.raft.currentTerm();

        cluster.runFor(3_000);

        long leaders = cluster.nodes().stream().filter(n -> n.raft.isLeader()).count();
        assertThat(leaders).isEqualTo(1);
        assertThat(cluster.leader().id).isEqualTo(leader.id);
        assertThat(cluster.leader().raft.currentTerm()).isEqualTo(term);
        cluster.nodes().forEach(n -> assertThat(n.raft.leaderId()).isEqualTo(leader.id));
    }

    @Test
    void electsNewLeaderAfterLeaderDies() {
        SimCluster cluster = new SimCluster(7, 3, 1000);
        cluster.runUntil(() -> cluster.leader() != null, 2_000);
        SimCluster.SimNode old = cluster.leader();
        long oldTerm = old.raft.currentTerm();

        cluster.crash(old.id);

        assertThat(cluster.runUntil(() -> cluster.leader() != null, 2_000)).as("a new leader is elected").isTrue();
        SimCluster.SimNode fresh = cluster.leader();
        assertThat(fresh.id).isNotEqualTo(old.id);
        assertThat(fresh.raft.currentTerm()).isGreaterThan(oldTerm);
    }

    @Test
    void oldLeaderStepsDownWhenItRejoins() {
        SimCluster cluster = new SimCluster(11, 3, 1000);
        cluster.runUntil(() -> cluster.leader() != null, 2_000);
        SimCluster.SimNode old = cluster.leader();

        cluster.partition(java.util.Map.of(old.id, 1));
        cluster.runUntil(() -> cluster.nodes().stream().anyMatch(n -> n != old && n.raft.isLeader()), 2_000);
        assertThat(old.raft.isLeader()).as("isolated leader does not know yet").isTrue();

        cluster.heal();
        cluster.runFor(500);
        assertThat(old.raft.isLeader()).as("stale leader stepped down on seeing a higher term").isFalse();
        assertThat(cluster.nodes().stream().filter(n -> n.raft.isLeader()).count()).isEqualTo(1);
    }
}
