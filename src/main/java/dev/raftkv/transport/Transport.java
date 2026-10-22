package dev.raftkv.transport;

import dev.raftkv.proto.AppendEntriesRequest;
import dev.raftkv.proto.AppendEntriesResponse;
import dev.raftkv.proto.InstallSnapshotRequest;
import dev.raftkv.proto.InstallSnapshotResponse;
import dev.raftkv.proto.RequestVoteRequest;
import dev.raftkv.proto.RequestVoteResponse;

import java.util.function.Consumer;

public interface Transport {
    void requestVote(String peer, RequestVoteRequest request, Consumer<RequestVoteResponse> onReply);

    void appendEntries(String peer, AppendEntriesRequest request, Consumer<AppendEntriesResponse> onReply);

    void installSnapshot(String peer, InstallSnapshotRequest request, Consumer<InstallSnapshotResponse> onReply);
}
