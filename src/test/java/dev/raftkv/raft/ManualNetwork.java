package dev.raftkv.raft;

import dev.raftkv.proto.AppendEntriesRequest;
import dev.raftkv.proto.AppendEntriesResponse;
import dev.raftkv.proto.InstallSnapshotRequest;
import dev.raftkv.proto.InstallSnapshotResponse;
import dev.raftkv.proto.RequestVoteRequest;
import dev.raftkv.proto.RequestVoteResponse;
import dev.raftkv.storage.MemoryStorage;
import dev.raftkv.transport.Transport;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;

final class ManualNetwork {
    private record Message(String from, String to, Runnable roundTrip) { }

    long now;
    final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    private final List<Message> queue = new ArrayList<>();

    RaftNode add(String id, List<String> all, MemoryStorage storage, int maxBatch) {
        List<String> peers = new ArrayList<>(all);
        peers.remove(id);
        RaftConfig config = new RaftConfig(id, peers, 150, 300, 50, 1_000_000, maxBatch);
        RaftNode node = new RaftNode(config, storage, new ManualTransport(id), () -> now,
                new NoopStateMachine(), new Random(id.hashCode()));
        nodes.put(id, node);
        return node;
    }

    void deliver(String from, String to) {
        boolean progress = true;
        while (progress) {
            progress = false;
            Iterator<Message> it = queue.iterator();
            while (it.hasNext()) {
                Message m = it.next();
                if (m.from.equals(from) && m.to.equals(to)) {
                    it.remove();
                    m.roundTrip.run();
                    progress = true;
                    break;
                }
            }
        }
    }

    void deliverOne(String from, String to) {
        for (Message m : queue) {
            if (m.from.equals(from) && m.to.equals(to)) {
                queue.remove(m);
                m.roundTrip.run();
                return;
            }
        }
        throw new AssertionError("no queued message " + from + " -> " + to);
    }

    void drop(String from, String to) {
        queue.removeIf(m -> m.from.equals(from) && m.to.equals(to));
    }

    private <Resp> void enqueue(String from, String to, Function<RaftNode, Resp> handler, Consumer<Resp> onReply) {
        queue.add(new Message(from, to, () -> onReply.accept(handler.apply(nodes.get(to)))));
    }

    private final class ManualTransport implements Transport {
        private final String from;

        ManualTransport(String from) {
            this.from = from;
        }

        @Override
        public void requestVote(String peer, RequestVoteRequest req, Consumer<RequestVoteResponse> onReply) {
            enqueue(from, peer, n -> n.handleRequestVote(req), onReply);
        }

        @Override
        public void appendEntries(String peer, AppendEntriesRequest req, Consumer<AppendEntriesResponse> onReply) {
            enqueue(from, peer, n -> n.handleAppendEntries(req), onReply);
        }

        @Override
        public void installSnapshot(String peer, InstallSnapshotRequest req, Consumer<InstallSnapshotResponse> onReply) {
            enqueue(from, peer, n -> n.handleInstallSnapshot(req), onReply);
        }
    }

    private static final class NoopStateMachine implements StateMachine {
        @Override
        public Object apply(long index, byte[] command) {
            return null;
        }

        @Override
        public byte[] snapshot() {
            return new byte[0];
        }

        @Override
        public void restore(byte[] snapshot) {
        }
    }
}
