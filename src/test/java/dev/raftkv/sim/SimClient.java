package dev.raftkv.sim;

import dev.raftkv.kv.Command;

import java.util.Map;

public final class SimClient {
    private static final long TIMEOUT_MS = 500;
    private static final long RETRY_DELAY_MS = 20;

    private final SimCluster cluster;
    private final String clientId;
    private final Map<String, String> acked;
    private long sequence;
    private Command inFlight;
    private int attempt;
    private boolean stopping;

    public SimClient(SimCluster cluster, String clientId, Map<String, String> acked) {
        this.cluster = cluster;
        this.clientId = clientId;
        this.acked = acked;
    }

    public void start() {
        cluster.schedule(1 + cluster.random.nextInt(20), this::nextWrite);
    }

    public void stop() {
        stopping = true;
    }

    public boolean idle() {
        return inFlight == null;
    }

    private void nextWrite() {
        if (stopping) {
            return;
        }
        sequence++;
        String key = clientId + "-" + sequence;
        inFlight = Command.put(key, "v" + sequence + "-" + cluster.random.nextInt(1000), clientId, sequence);
        send();
    }

    private void send() {
        int myAttempt = ++attempt;
        Command cmd = inFlight;
        SimCluster.SimNode leader = cluster.leader();
        if (leader == null) {
            cluster.schedule(RETRY_DELAY_MS, this::send);
            return;
        }
        leader.raft.propose(cmd.encode()).whenComplete((result, error) -> {
            if (myAttempt != attempt || inFlight != cmd) {
                return;
            }
            if (error == null) {
                acked.put(cmd.key(), cmd.value());
                inFlight = null;
                attempt++;
                cluster.schedule(1 + cluster.random.nextInt(10), this::nextWrite);
            } else {
                attempt++;
                cluster.schedule(RETRY_DELAY_MS, this::send);
            }
        });
        cluster.schedule(TIMEOUT_MS, () -> {
            if (myAttempt == attempt && inFlight == cmd) {
                attempt++;
                send();
            }
        });
    }
}
