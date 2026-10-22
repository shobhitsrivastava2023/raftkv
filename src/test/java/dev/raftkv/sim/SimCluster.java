package dev.raftkv.sim;

import dev.raftkv.kv.KvStateMachine;
import dev.raftkv.proto.AppendEntriesRequest;
import dev.raftkv.proto.AppendEntriesResponse;
import dev.raftkv.proto.InstallSnapshotRequest;
import dev.raftkv.proto.InstallSnapshotResponse;
import dev.raftkv.proto.RequestVoteRequest;
import dev.raftkv.proto.RequestVoteResponse;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.raft.StateMachine;
import dev.raftkv.storage.LogEntry;
import dev.raftkv.storage.MemoryStorage;
import dev.raftkv.transport.Transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

public final class SimCluster {
    public static final int TICK_MS = 5;

    private record Event(long time, long seq, Runnable action) { }

    public final Random random;
    private long now;
    private long eventSeq;
    private final PriorityQueue<Event> events = new PriorityQueue<>(
            (a, b) -> a.time != b.time ? Long.compare(a.time, b.time) : Long.compare(a.seq, b.seq));

    private final Map<String, SimNode> nodes = new LinkedHashMap<>();
    private final Map<String, Integer> partitionGroup = new HashMap<>();
    private double dropRate;
    private final long snapshotThreshold;
    private final BiFunction<String, MemoryStorage, MemoryStorage> reopenStorage;

    private final Map<Long, String> leaderOfTerm = new HashMap<>();
    private final Map<Long, byte[]> appliedCommands = new HashMap<>();
    public int installSnapshotsDelivered;

    public final class SimNode {
        public final String id;
        public MemoryStorage storage;
        public RaftNode raft;
        public KvStateMachine kv;
        public boolean up;
        int incarnation;
        long lastAppliedIndex;

        SimNode(String id) {
            this.id = id;
        }
    }

    public SimCluster(long seed, int size, long snapshotThreshold) {
        this(seed, size, snapshotThreshold, (id, old) -> old == null ? new MemoryStorage() : old);
    }

    public SimCluster(long seed, int size, long snapshotThreshold,
                      BiFunction<String, MemoryStorage, MemoryStorage> reopenStorage) {
        this.random = new Random(seed);
        this.snapshotThreshold = snapshotThreshold;
        this.reopenStorage = reopenStorage;
        for (int i = 1; i <= size; i++) {
            String id = "n" + i;
            nodes.put(id, new SimNode(id));
            partitionGroup.put(id, 0);
        }
        nodes.values().forEach(this::boot);
        schedule(TICK_MS, this::tickAll);
    }

    public long now() {
        return now;
    }

    public void schedule(long delayMs, Runnable action) {
        events.add(new Event(now + delayMs, eventSeq++, action));
    }

    public void runFor(long ms) {
        long end = now + ms;
        while (!events.isEmpty() && events.peek().time <= end) {
            Event e = events.poll();
            now = e.time;
            e.action.run();
            checkElectionSafety();
        }
        now = end;
    }

    public boolean runUntil(BooleanSupplier condition, long maxMs) {
        long end = now + maxMs;
        while (now < end) {
            if (condition.getAsBoolean()) {
                return true;
            }
            runFor(TICK_MS);
        }
        return condition.getAsBoolean();
    }

    private void tickAll() {
        for (SimNode n : nodes.values()) {
            if (n.up) {
                n.raft.tick();
            }
        }
        schedule(TICK_MS, this::tickAll);
    }

    private void boot(SimNode n) {
        List<String> peers = new ArrayList<>(nodes.keySet());
        peers.remove(n.id);
        RaftConfig config = new RaftConfig(n.id, peers, 150, 300, 50, snapshotThreshold, 64);
        n.storage = reopenStorage.apply(n.id, n.storage);
        n.kv = new KvStateMachine();
        n.lastAppliedIndex = n.storage.snapshotIndex();
        n.raft = new RaftNode(config, n.storage, new SimTransport(n.id), () -> now,
                new RecordingStateMachine(n), new Random(random.nextLong()));
        n.up = true;
    }

    public void crash(String id) {
        SimNode n = nodes.get(id);
        if (!n.up) {
            return;
        }
        n.raft.stop();
        n.up = false;
        n.incarnation++;
    }

    public void restart(String id) {
        SimNode n = nodes.get(id);
        if (n.up) {
            return;
        }
        boot(n);
    }

    public SimNode node(String id) {
        return nodes.get(id);
    }

    public List<SimNode> nodes() {
        return List.copyOf(nodes.values());
    }

    public SimNode leader() {
        SimNode best = null;
        for (SimNode n : nodes.values()) {
            if (n.up && n.raft.isLeader() && (best == null || n.raft.currentTerm() > best.raft.currentTerm())) {
                best = n;
            }
        }
        return best;
    }

    public void setDropRate(double dropRate) {
        this.dropRate = dropRate;
    }

    public void partition(Map<String, Integer> groups) {
        partitionGroup.putAll(groups);
    }

    public void heal() {
        partitionGroup.replaceAll((k, v) -> 0);
        dropRate = 0;
    }

    private boolean deliverable(String from, String to) {
        if (!partitionGroup.get(from).equals(partitionGroup.get(to))) {
            return false;
        }
        return dropRate == 0 || random.nextDouble() >= dropRate;
    }

    private long latency() {
        return 1 + random.nextInt(10);
    }

    private <Resp> void rpc(String from, String to, Function<RaftNode, Resp> handler, Consumer<Resp> onReply) {
        int fromIncarnation = nodes.get(from).incarnation;
        if (!deliverable(from, to)) {
            return;
        }
        schedule(latency(), () -> {
            SimNode target = nodes.get(to);
            if (!target.up) {
                return;
            }
            Resp resp = handler.apply(target.raft);
            if (!deliverable(to, from)) {
                return;
            }
            schedule(latency(), () -> {
                SimNode sender = nodes.get(from);
                if (sender.up && sender.incarnation == fromIncarnation) {
                    onReply.accept(resp);
                }
            });
        });
    }

    private final class SimTransport implements Transport {
        private final String from;

        SimTransport(String from) {
            this.from = from;
        }

        @Override
        public void requestVote(String peer, RequestVoteRequest req, Consumer<RequestVoteResponse> onReply) {
            rpc(from, peer, n -> n.handleRequestVote(req), onReply);
        }

        @Override
        public void appendEntries(String peer, AppendEntriesRequest req, Consumer<AppendEntriesResponse> onReply) {
            rpc(from, peer, n -> n.handleAppendEntries(req), onReply);
        }

        @Override
        public void installSnapshot(String peer, InstallSnapshotRequest req,
                                    Consumer<InstallSnapshotResponse> onReply) {
            rpc(from, peer, n -> {
                installSnapshotsDelivered++;
                return n.handleInstallSnapshot(req);
            }, onReply);
        }
    }

    private void checkElectionSafety() {
        for (SimNode n : nodes.values()) {
            if (n.up && n.raft.isLeader()) {
                String previous = leaderOfTerm.putIfAbsent(n.raft.currentTerm(), n.id);
                if (previous != null && !previous.equals(n.id)) {
                    throw new AssertionError("two leaders in term " + n.raft.currentTerm() + ": " + previous + " and " + n.id);
                }
            }
        }
    }

    private final class RecordingStateMachine implements StateMachine {
        private final SimNode node;

        RecordingStateMachine(SimNode node) {
            this.node = node;
        }

        @Override
        public Object apply(long index, byte[] command) {
            if (index <= node.lastAppliedIndex) {
                throw new AssertionError(node.id + " applied index " + index + " after " + node.lastAppliedIndex);
            }
            node.lastAppliedIndex = index;
            byte[] previous = appliedCommands.putIfAbsent(index, command);
            if (previous != null && !Arrays.equals(previous, command)) {
                throw new AssertionError(node.id + " applied a different command at index " + index);
            }
            return node.kv.apply(index, command);
        }

        @Override
        public byte[] snapshot() {
            return node.kv.snapshot();
        }

        @Override
        public void restore(byte[] snapshot) {
            node.kv.restore(snapshot);
            node.lastAppliedIndex = node.storage.snapshotIndex();
        }
    }

    public void checkCommittedEntriesUnchanged() {
        for (SimNode n : nodes.values()) {
            if (!n.up) {
                continue;
            }
            long from = n.storage.snapshotIndex() + 1;
            long to = Math.min(n.raft.commitIndex(), n.storage.lastIndex());
            for (LogEntry e : n.storage.entries(from, to)) {
                byte[] applied = appliedCommands.get(e.index());
                if (applied != null && !Arrays.equals(applied, e.command())) {
                    throw new AssertionError(n.id + " has a changed committed entry at index " + e.index());
                }
            }
        }
    }

    public long highestAppliedIndex() {
        return appliedCommands.keySet().stream().mapToLong(Long::longValue).max().orElse(0);
    }
}
