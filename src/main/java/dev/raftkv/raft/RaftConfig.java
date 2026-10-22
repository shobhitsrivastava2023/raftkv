package dev.raftkv.raft;

import java.util.List;

public record RaftConfig(String id, List<String> peers, long electionTimeoutMinMs, long electionTimeoutMaxMs,
                         long heartbeatMs, long snapshotThreshold, int maxBatch) {
    public static RaftConfig defaults(String id, List<String> peers) {
        return new RaftConfig(id, List.copyOf(peers), 300, 600, 50, 10_000, 512);
    }

    public int clusterSize() {
        return peers.size() + 1;
    }

    public int majority() {
        return clusterSize() / 2 + 1;
    }
}
