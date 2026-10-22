package dev.raftkv.transport;

import dev.raftkv.proto.AppendEntriesRequest;
import dev.raftkv.proto.AppendEntriesResponse;
import dev.raftkv.proto.InstallSnapshotRequest;
import dev.raftkv.proto.InstallSnapshotResponse;
import dev.raftkv.proto.RaftServiceGrpc;
import dev.raftkv.proto.RequestVoteRequest;
import dev.raftkv.proto.RequestVoteResponse;
import dev.raftkv.raft.RaftNode;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.function.Supplier;

public final class RaftGrpcService extends RaftServiceGrpc.RaftServiceImplBase {
    private final RaftNode node;

    public RaftGrpcService(RaftNode node) {
        this.node = node;
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> observer) {
        respond(observer, () -> node.handleRequestVote(request));
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> observer) {
        respond(observer, () -> node.handleAppendEntries(request));
    }

    @Override
    public void installSnapshot(InstallSnapshotRequest request, StreamObserver<InstallSnapshotResponse> observer) {
        respond(observer, () -> node.handleInstallSnapshot(request));
    }

    private static <T> void respond(StreamObserver<T> observer, Supplier<T> handler) {
        try {
            observer.onNext(handler.get());
            observer.onCompleted();
        } catch (RuntimeException e) {
            observer.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
