package dev.raftkv.kv;

import dev.raftkv.raft.Clock;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.storage.FileStorage;
import dev.raftkv.transport.GrpcTransport;
import dev.raftkv.transport.RaftGrpcService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class RaftKvServer implements AutoCloseable {
    private final RaftNode node;
    private final FileStorage storage;
    private final GrpcTransport transport;
    private final Server server;
    private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();

    public RaftKvServer(String id, int port, Path dataDir, Map<String, String> peers,
                        Map<String, String> clientAddrs, long snapshotEvery, boolean verbose) {
        Map<String, String> others = new LinkedHashMap<>(peers);
        others.remove(id);
        RaftConfig d = RaftConfig.defaults(id, List.copyOf(others.keySet()));
        RaftConfig config = new RaftConfig(id, d.peers(), d.electionTimeoutMinMs(), d.electionTimeoutMaxMs(),
                d.heartbeatMs(), snapshotEvery, d.maxBatch());

        storage = new FileStorage(dataDir);
        transport = new GrpcTransport(others);
        KvStateMachine kv = new KvStateMachine();
        node = new RaftNode(config, storage, transport, Clock.SYSTEM, kv, new Random());
        if (verbose) {
            node.setEventLog(msg -> System.out.println(LocalTime.now() + " [" + id + "] " + msg));
        }
        Map<String, String> redirects = new LinkedHashMap<>(peers);
        redirects.putAll(clientAddrs);
        try {
            server = ServerBuilder.forPort(port)
                    .addService(new RaftGrpcService(node))
                    .addService(new KvGrpcService(node, kv, redirects))
                    .build()
                    .start();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot listen on port " + port, e);
        }
        ticker.scheduleAtFixedRate(node::tick, 10, 10, TimeUnit.MILLISECONDS);
    }

    public RaftNode node() {
        return node;
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @Override
    public void close() {
        ticker.shutdownNow();
        node.stop();
        server.shutdownNow();
        transport.close();
        try {
            server.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        storage.close();
    }
}
