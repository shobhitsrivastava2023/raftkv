package dev.raftkv.transport;

import dev.raftkv.proto.AppendEntriesRequest;
import dev.raftkv.proto.AppendEntriesResponse;
import dev.raftkv.proto.InstallSnapshotRequest;
import dev.raftkv.proto.InstallSnapshotResponse;
import dev.raftkv.proto.RaftServiceGrpc;
import dev.raftkv.proto.RequestVoteRequest;
import dev.raftkv.proto.RequestVoteResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class GrpcTransport implements Transport, AutoCloseable {
    private static final long RPC_TIMEOUT_MS = 1000;
    private static final long SNAPSHOT_TIMEOUT_MS = 10_000;

    private final Map<String, ManagedChannel> channels = new HashMap<>();
    private final Map<String, RaftServiceGrpc.RaftServiceStub> stubs = new HashMap<>();

    public GrpcTransport(Map<String, String> peerAddresses) {
        peerAddresses.forEach((id, address) -> {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
            channels.put(id, channel);
            stubs.put(id, RaftServiceGrpc.newStub(channel));
        });
    }

    @Override
    public void requestVote(String peer, RequestVoteRequest request, Consumer<RequestVoteResponse> onReply) {
        stubs.get(peer).withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .requestVote(request, observer(onReply));
    }

    @Override
    public void appendEntries(String peer, AppendEntriesRequest request, Consumer<AppendEntriesResponse> onReply) {
        stubs.get(peer).withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .appendEntries(request, observer(onReply));
    }

    @Override
    public void installSnapshot(String peer, InstallSnapshotRequest request,
                                Consumer<InstallSnapshotResponse> onReply) {
        stubs.get(peer).withDeadlineAfter(SNAPSHOT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .installSnapshot(request, observer(onReply));
    }

    private static <T> StreamObserver<T> observer(Consumer<T> onReply) {
        return new StreamObserver<>() {
            @Override
            public void onNext(T value) {
                onReply.accept(value);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        };
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
    }
}
