package dev.raftkv.client;

import dev.raftkv.proto.DeleteRequest;
import dev.raftkv.proto.GetRequest;
import dev.raftkv.proto.KvResponse;
import dev.raftkv.proto.KvServiceGrpc;
import dev.raftkv.proto.PutRequest;
import dev.raftkv.proto.Status;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class RaftKvClient implements AutoCloseable {
    private static final long RPC_TIMEOUT_MS = 4000;

    private final List<String> addresses;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final String clientId = UUID.randomUUID().toString();
    private final long retryForMs;
    private long sequence;
    private String current;

    public RaftKvClient(List<String> addresses) {
        this(addresses, 15_000);
    }

    public RaftKvClient(List<String> addresses, long retryForMs) {
        this.addresses = new ArrayList<>(addresses);
        this.current = addresses.get(0);
        this.retryForMs = retryForMs;
    }

    public void put(String key, String value) {
        long seq = ++sequence;
        call(stub -> stub.put(PutRequest.newBuilder()
                .setKey(key).setValue(value).setClientId(clientId).setSequence(seq).build()));
    }

    public void delete(String key) {
        long seq = ++sequence;
        call(stub -> stub.delete(DeleteRequest.newBuilder()
                .setKey(key).setClientId(clientId).setSequence(seq).build()));
    }

    public Optional<String> get(String key) {
        KvResponse r = call(stub -> stub.get(GetRequest.newBuilder().setKey(key).build()));
        return r.getStatus() == Status.NOT_FOUND ? Optional.empty() : Optional.of(r.getValue());
    }

    public String currentLeader() {
        return current;
    }

    private KvResponse call(Function<KvServiceGrpc.KvServiceBlockingStub, KvResponse> rpc) {
        long deadline = System.currentTimeMillis() + retryForMs;
        int attempt = 0;
        while (true) {
            try {
                KvServiceGrpc.KvServiceBlockingStub stub = KvServiceGrpc.newBlockingStub(channel(current))
                        .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                KvResponse r = rpc.apply(stub);
                if (r.getStatus() == Status.OK || r.getStatus() == Status.NOT_FOUND) {
                    return r;
                }
                if (r.getStatus() == Status.NOT_LEADER && !r.getLeaderHint().isEmpty()
                        && !r.getLeaderHint().equals(current)) {
                    current = r.getLeaderHint();
                    continue;
                }
            } catch (StatusRuntimeException e) {
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("cluster unavailable: no leader answered within " + retryForMs + " ms");
            }
            current = addresses.get(++attempt % addresses.size());
            sleep(Math.min(100, 10L * attempt));
        }
    }

    private ManagedChannel channel(String address) {
        return channels.computeIfAbsent(address,
                a -> ManagedChannelBuilder.forTarget(a).usePlaintext().build());
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
    }
}
