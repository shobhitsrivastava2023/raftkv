package dev.raftkv.raft;

import dev.raftkv.storage.LogEntry;
import dev.raftkv.storage.MemoryStorage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CommitRuleTest {
    private static final List<String> ALL = List.of("s1", "s2", "s3", "s4", "s5");

    @Test
    void leaderDoesNotCommitPreviousTermEntryByCountingReplicas() {
        ManualNetwork net = new ManualNetwork();

        net.add("s1", ALL, storage(3, entry(1, 1), entry(2, 2)), 1);
        net.add("s2", ALL, storage(2, entry(1, 1), entry(2, 2)), 1);
        net.add("s3", ALL, storage(3, entry(1, 1)), 1);
        net.add("s4", ALL, storage(3, entry(1, 1)), 1);
        net.add("s5", ALL, storage(3, entry(1, 1), entry(3, 2)), 1);
        RaftNode s1 = net.nodes.get("s1");

        net.now = 1_000;
        s1.tick();
        net.deliverOne("s1", "s2");
        net.deliverOne("s1", "s3");
        assertThat(s1.isLeader()).isTrue();
        assertThat(s1.currentTerm()).isEqualTo(4);
        net.drop("s1", "s2");
        net.drop("s1", "s5");

        net.deliverOne("s1", "s3");
        net.deliverOne("s1", "s3");
        net.drop("s1", "s3");

        net.deliverOne("s1", "s4");
        net.deliverOne("s1", "s4");
        net.deliverOne("s1", "s4");
        net.drop("s1", "s4");

        assertThat(s1.commitIndex())
                .as("§5.4.2: an entry from a previous term must not be committed by counting replicas")
                .isLessThan(2);

        net.now += 100;
        s1.tick();
        net.deliver("s1", "s3");
        net.deliver("s1", "s4");
        assertThat(s1.commitIndex()).isEqualTo(3);
    }

    private static MemoryStorage storage(long term, LogEntry... entries) {
        MemoryStorage s = new MemoryStorage();
        s.setTermAndVote(term, null);
        s.append(List.of(entries));
        return s;
    }

    private static LogEntry entry(long term, long index) {
        return new LogEntry(term, index, new byte[]{1});
    }
}
