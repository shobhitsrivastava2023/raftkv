package dev.raftkv.kv;

import dev.raftkv.proto.DeleteRequest;
import dev.raftkv.proto.GetRequest;
import dev.raftkv.proto.KvResponse;
import dev.raftkv.proto.KvServiceGrpc;
import dev.raftkv.proto.PutRequest;
import dev.raftkv.proto.Status;
import dev.raftkv.raft.NotLeaderException;
import dev.raftkv.raft.RaftNode;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

public final class KvGrpcService extends KvServiceGrpc.KvServiceImplBase {
    private static final long REQUEST_TIMEOUT_MS = 3000;

    private final RaftNode node;
    private final KvStateMachine kv;
    private final Map<String, String> clientAddresses;

    public KvGrpcService(RaftNode node, KvStateMachine kv, Map<String, String> clientAddresses) {
        this.node = node;
        this.kv = kv;
        this.clientAddresses = clientAddresses;
    }

    @Override
    public void put(PutRequest req, StreamObserver<KvResponse> observer) {
        write(Command.put(req.getKey(), req.getValue(), req.getClientId(), req.getSequence()), observer);
    }

    @Override
    public void delete(DeleteRequest req, StreamObserver<KvResponse> observer) {
        write(Command.delete(req.getKey(), req.getClientId(), req.getSequence()), observer);
    }

    private void write(Command command, StreamObserver<KvResponse> observer) {
        CompletableFuture<Object> applied = node.propose(command.encode());
        reply(applied.thenApply(r -> KvResponse.newBuilder().setStatus(Status.OK).build()), observer);
    }

    @Override
    public void get(GetRequest req, StreamObserver<KvResponse> observer) {
        CompletableFuture<KvResponse> result = node.readIndex().thenApply(index -> {
            String value = kv.get(req.getKey());
            return value == null
                    ? KvResponse.newBuilder().setStatus(Status.NOT_FOUND).build()
                    : KvResponse.newBuilder().setStatus(Status.OK).setValue(value).build();
        });
        reply(result, observer);
    }

    private void reply(CompletableFuture<KvResponse> future, StreamObserver<KvResponse> observer) {
        future.orTimeout(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).whenComplete((response, error) -> {
            observer.onNext(error == null ? response : failure(unwrap(error)));
            observer.onCompleted();
        });
    }

    private KvResponse failure(Throwable error) {
        if (error instanceof NotLeaderException nle) {
            String hint = nle.leaderId() == null ? "" : clientAddresses.getOrDefault(nle.leaderId(), "");
            return KvResponse.newBuilder().setStatus(Status.NOT_LEADER).setLeaderHint(hint).build();
        }
        return KvResponse.newBuilder().setStatus(Status.TIMEOUT).build();
    }

    private static Throwable unwrap(Throwable t) {
        return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
    }
}
