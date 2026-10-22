package dev.raftkv.raft;

import com.google.protobuf.ByteString;
import dev.raftkv.proto.AppendEntriesRequest;
import dev.raftkv.proto.AppendEntriesResponse;
import dev.raftkv.proto.InstallSnapshotRequest;
import dev.raftkv.proto.InstallSnapshotResponse;
import dev.raftkv.proto.LogEntryProto;
import dev.raftkv.proto.RequestVoteRequest;
import dev.raftkv.proto.RequestVoteResponse;
import dev.raftkv.storage.LogEntry;
import dev.raftkv.storage.Snapshot;
import dev.raftkv.storage.Storage;
import dev.raftkv.transport.Transport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class RaftNode {
    private final RaftConfig config;
    private final Storage storage;
    private final Transport transport;
    private final Clock clock;
    private final StateMachine stateMachine;
    private final Random random;

    private Role role = Role.FOLLOWER;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;
    private long electionDeadline;

    private final Set<String> votes = new HashSet<>();

    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    private final Map<String, Long> inflightUntil = new HashMap<>();
    private long nextHeartbeatAt;

    private long heartbeatRound;

    private final Map<String, Long> ackedRound = new HashMap<>();
    private final List<PendingRead> pendingReads = new ArrayList<>();

    private final Map<Long, PendingWrite> pendingWrites = new HashMap<>();

    private boolean stopped;
    private Consumer<String> eventLog = s -> { };

    private record PendingWrite(long term, CompletableFuture<Object> future) { }

    private static final class PendingRead {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        long readIndex = -1;
        long round = -1;
    }

    public RaftNode(RaftConfig config, Storage storage, Transport transport, Clock clock,
                    StateMachine stateMachine, Random random) {
        this.config = config;
        this.storage = storage;
        this.transport = transport;
        this.clock = clock;
        this.stateMachine = stateMachine;
        this.random = random;

        Snapshot snapshot = storage.snapshot();
        if (snapshot != null) {
            stateMachine.restore(snapshot.data());
            commitIndex = snapshot.lastIncludedIndex();
            lastApplied = snapshot.lastIncludedIndex();
        }
        resetElectionTimer();
    }

    public synchronized void tick() {
        if (stopped) {
            return;
        }
        long now = clock.nowMillis();
        if (role == Role.LEADER) {
            if (now >= nextHeartbeatAt) {
                broadcastAppend(true);
            }
        } else if (now >= electionDeadline) {
            startElection();
        }
    }

    private void resetElectionTimer() {
        long spread = config.electionTimeoutMaxMs() - config.electionTimeoutMinMs();

        electionDeadline = clock.nowMillis() + config.electionTimeoutMinMs() + (long) (random.nextDouble() * spread);
    }

    private void startElection() {
        role = Role.CANDIDATE;
        leaderId = null;
        long term = storage.currentTerm() + 1;
        storage.setTermAndVote(term, config.id());
        votes.clear();
        votes.add(config.id());
        resetElectionTimer();
        eventLog.accept("starting election for term " + term);
        if (votes.size() >= config.majority()) {
            becomeLeader();
            return;
        }
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(term)
                .setCandidateId(config.id())
                .setLastLogIndex(storage.lastIndex())
                .setLastLogTerm(storage.termAt(storage.lastIndex()))
                .build();
        for (String peer : config.peers()) {
            transport.requestVote(peer, request, reply -> onVoteReply(peer, term, reply));
        }
    }

    private synchronized void onVoteReply(String peer, long term, RequestVoteResponse reply) {
        if (stopped) {
            return;
        }
        if (reply.getTerm() > storage.currentTerm()) {
            becomeFollower(reply.getTerm());
            return;
        }
        if (role != Role.CANDIDATE || storage.currentTerm() != term) {
            return;
        }
        if (reply.getVoteGranted()) {
            votes.add(peer);
            if (votes.size() >= config.majority()) {
                becomeLeader();
            }
        }
    }

    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        if (req.getTerm() > storage.currentTerm()) {
            becomeFollower(req.getTerm());
        }
        long term = storage.currentTerm();
        String votedFor = storage.votedFor();
        boolean grant = !stopped
                && req.getTerm() == term
                && (votedFor == null || votedFor.equals(req.getCandidateId()))
                && candidateLogIsUpToDate(req.getLastLogTerm(), req.getLastLogIndex());
        if (grant) {
            storage.setTermAndVote(term, req.getCandidateId());
            resetElectionTimer();
        }
        return RequestVoteResponse.newBuilder().setTerm(term).setVoteGranted(grant).build();
    }

    private boolean candidateLogIsUpToDate(long candidateLastTerm, long candidateLastIndex) {
        long myLastIndex = storage.lastIndex();
        long myLastTerm = storage.termAt(myLastIndex);
        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= myLastIndex;
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = config.id();
        long last = storage.lastIndex();
        for (String peer : config.peers()) {
            nextIndex.put(peer, last + 1);
            matchIndex.put(peer, 0L);
            inflightUntil.put(peer, 0L);
            ackedRound.put(peer, 0L);
        }
        heartbeatRound = 0;
        eventLog.accept("became leader for term " + storage.currentTerm());

        storage.append(List.of(new LogEntry(storage.currentTerm(), last + 1, new byte[0])));
        broadcastAppend(true);
        advanceCommitIndex();
    }

    private void becomeFollower(long term) {
        if (term > storage.currentTerm()) {
            storage.setTermAndVote(term, null);
            leaderId = null;
        }
        if (role == Role.LEADER) {
            failPendingRequests();
            resetElectionTimer();
            eventLog.accept("stepping down in term " + storage.currentTerm());
        }
        role = Role.FOLLOWER;
    }

    public synchronized CompletableFuture<Object> propose(byte[] command) {
        if (stopped || role != Role.LEADER) {
            return CompletableFuture.failedFuture(new NotLeaderException(leaderId));
        }
        if (command.length == 0) {
            throw new IllegalArgumentException("empty commands are reserved for no-ops");
        }
        long index = storage.lastIndex() + 1;
        long term = storage.currentTerm();
        storage.append(List.of(new LogEntry(term, index, command)));
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingWrites.put(index, new PendingWrite(term, future));
        for (String peer : config.peers()) {
            sendAppend(peer, false);
        }
        advanceCommitIndex();
        return future;
    }

    private void broadcastAppend(boolean heartbeat) {
        nextHeartbeatAt = clock.nowMillis() + config.heartbeatMs();
        for (String peer : config.peers()) {
            sendAppend(peer, heartbeat);
        }
    }

    private void sendAppend(String peer, boolean heartbeat) {
        long now = clock.nowMillis();
        long next = nextIndex.get(peer);
        long term = storage.currentTerm();
        long round = heartbeatRound;

        if (next <= storage.snapshotIndex()) {
            if (now < inflightUntil.get(peer)) {
                return;
            }
            inflightUntil.put(peer, now + 4 * config.heartbeatMs());
            Snapshot snapshot = storage.snapshot();
            InstallSnapshotRequest req = InstallSnapshotRequest.newBuilder()
                    .setTerm(term)
                    .setLeaderId(config.id())
                    .setLastIncludedIndex(snapshot.lastIncludedIndex())
                    .setLastIncludedTerm(snapshot.lastIncludedTerm())
                    .setData(ByteString.copyFrom(snapshot.data()))
                    .build();
            transport.installSnapshot(peer, req,
                    reply -> onSnapshotReply(peer, term, round, snapshot.lastIncludedIndex(), reply));
            return;
        }

        if (!heartbeat && now < inflightUntil.get(peer)) {
            return;
        }
        inflightUntil.put(peer, now + 2 * config.heartbeatMs());
        long prev = next - 1;
        long last = Math.min(storage.lastIndex(), prev + config.maxBatch());
        AppendEntriesRequest.Builder req = AppendEntriesRequest.newBuilder()
                .setTerm(term)
                .setLeaderId(config.id())
                .setPrevLogIndex(prev)
                .setPrevLogTerm(storage.termAt(prev))
                .setLeaderCommit(commitIndex);
        for (LogEntry e : storage.entries(next, last)) {
            req.addEntries(toProto(e));
        }
        transport.appendEntries(peer, req.build(), reply -> onAppendReply(peer, term, round, reply));
    }

    private synchronized void onAppendReply(String peer, long term, long round, AppendEntriesResponse reply) {
        if (stopped) {
            return;
        }
        if (reply.getTerm() > storage.currentTerm()) {
            becomeFollower(reply.getTerm());
            return;
        }
        if (role != Role.LEADER || storage.currentTerm() != term) {
            return;
        }
        ackedRound.merge(peer, round, Math::max);
        inflightUntil.put(peer, 0L);
        if (reply.getSuccess()) {
            matchIndex.merge(peer, reply.getMatchIndex(), Math::max);
            nextIndex.put(peer, Math.max(nextIndex.get(peer), matchIndex.get(peer) + 1));
            advanceCommitIndex();
        } else {
            nextIndex.put(peer, Math.max(matchIndex.get(peer) + 1, reply.getConflictIndex()));
        }
        processReads();
        if (role == Role.LEADER && nextIndex.get(peer) <= storage.lastIndex()) {
            sendAppend(peer, false);
        }
    }

    private synchronized void onSnapshotReply(String peer, long term, long round, long snapshotIndex,
                                              InstallSnapshotResponse reply) {
        if (stopped) {
            return;
        }
        if (reply.getTerm() > storage.currentTerm()) {
            becomeFollower(reply.getTerm());
            return;
        }
        if (role != Role.LEADER || storage.currentTerm() != term) {
            return;
        }
        ackedRound.merge(peer, round, Math::max);
        inflightUntil.put(peer, 0L);
        matchIndex.merge(peer, snapshotIndex, Math::max);
        nextIndex.put(peer, Math.max(nextIndex.get(peer), matchIndex.get(peer) + 1));
        advanceCommitIndex();
        processReads();
        if (role == Role.LEADER && nextIndex.get(peer) <= storage.lastIndex()) {
            sendAppend(peer, false);
        }
    }

    private void advanceCommitIndex() {
        for (long n = storage.lastIndex(); n > commitIndex; n--) {
            if (storage.termAt(n) != storage.currentTerm()) {
                break;
            }
            int replicas = 1;
            for (String peer : config.peers()) {
                if (matchIndex.get(peer) >= n) {
                    replicas++;
                }
            }
            if (replicas >= config.majority()) {
                commitIndex = n;
                applyCommitted();
                break;
            }
        }
    }

    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        long term = storage.currentTerm();
        if (stopped || req.getTerm() < term) {
            return appendReply(false, 0, storage.lastIndex() + 1);
        }
        if (req.getTerm() == term && role == Role.LEADER) {
            throw new IllegalStateException("two leaders in term " + term);
        }
        becomeFollower(req.getTerm());
        leaderId = req.getLeaderId();
        resetElectionTimer();

        long prev = req.getPrevLogIndex();
        long prevTerm = req.getPrevLogTerm();
        List<LogEntryProto> entries = req.getEntriesList();
        long snapshotIndex = storage.snapshotIndex();
        if (prev < snapshotIndex) {
            int skip = (int) Math.min(entries.size(), snapshotIndex - prev);
            if (skip == entries.size()) {
                return appendReply(true, snapshotIndex, 0);
            }
            entries = entries.subList(skip, entries.size());
            prev = snapshotIndex;
            prevTerm = storage.snapshotTerm();
        }

        if (prev > storage.lastIndex()) {
            return appendReply(false, 0, storage.lastIndex() + 1);
        }
        if (storage.termAt(prev) != prevTerm) {
            long conflictTerm = storage.termAt(prev);
            long first = prev;
            while (first - 1 > snapshotIndex && storage.termAt(first - 1) == conflictTerm) {
                first--;
            }
            return appendReply(false, 0, first);
        }

        List<LogEntry> toAppend = new ArrayList<>();
        long index = prev;
        for (LogEntryProto p : entries) {
            index++;
            if (toAppend.isEmpty() && index <= storage.lastIndex()) {
                if (storage.termAt(index) == p.getTerm()) {
                    continue;
                }
                if (index <= commitIndex) {
                    throw new IllegalStateException("leader tried to overwrite committed entry " + index);
                }
                storage.truncateFrom(index);
            }
            toAppend.add(fromProto(p));
        }
        if (!toAppend.isEmpty()) {
            storage.append(toAppend);
        }

        long lastNew = prev + entries.size();
        if (req.getLeaderCommit() > commitIndex) {
            commitIndex = Math.max(commitIndex, Math.min(req.getLeaderCommit(), lastNew));
            applyCommitted();
        }
        return appendReply(true, lastNew, 0);
    }

    private AppendEntriesResponse appendReply(boolean success, long matchIndex, long conflictIndex) {
        return AppendEntriesResponse.newBuilder()
                .setTerm(storage.currentTerm())
                .setSuccess(success)
                .setMatchIndex(matchIndex)
                .setConflictIndex(conflictIndex)
                .build();
    }

    public synchronized InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest req) {
        long term = storage.currentTerm();
        if (stopped || req.getTerm() < term) {
            return InstallSnapshotResponse.newBuilder().setTerm(term).build();
        }
        becomeFollower(req.getTerm());
        leaderId = req.getLeaderId();
        resetElectionTimer();

        long index = req.getLastIncludedIndex();
        if (index > lastApplied) {
            storage.installSnapshot(new Snapshot(index, req.getLastIncludedTerm(), req.getData().toByteArray()));
            stateMachine.restore(req.getData().toByteArray());
            lastApplied = index;
            commitIndex = Math.max(index, Math.min(commitIndex, storage.lastIndex()));
            applyCommitted();
        }
        return InstallSnapshotResponse.newBuilder().setTerm(storage.currentTerm()).build();
    }

    private void maybeSnapshot() {
        if (lastApplied - storage.snapshotIndex() >= config.snapshotThreshold()) {
            storage.installSnapshot(new Snapshot(lastApplied, storage.termAt(lastApplied), stateMachine.snapshot()));
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            long index = lastApplied + 1;
            LogEntry entry = storage.entry(index);
            Object result = entry.isNoop() ? null : stateMachine.apply(index, entry.command());
            lastApplied = index;
            PendingWrite pending = pendingWrites.remove(index);
            if (pending != null) {
                if (pending.term() == entry.term()) {
                    pending.future().complete(result);
                } else {
                    pending.future().completeExceptionally(new NotLeaderException(leaderId));
                }
            }
        }
        maybeSnapshot();
        processReads();
    }

    public synchronized CompletableFuture<Long> readIndex() {
        if (stopped || role != Role.LEADER) {
            return CompletableFuture.failedFuture(new NotLeaderException(leaderId));
        }
        PendingRead read = new PendingRead();
        pendingReads.add(read);
        processReads();
        return read.future;
    }

    private void processReads() {
        if (pendingReads.isEmpty() || role != Role.LEADER) {
            return;
        }

        boolean committedInTerm = storage.termAt(commitIndex) == storage.currentTerm();
        boolean needRound = false;
        for (PendingRead read : pendingReads) {
            if (read.readIndex < 0 && committedInTerm) {
                read.readIndex = commitIndex;
                read.round = heartbeatRound + 1;
                needRound = true;
            }
        }
        if (needRound) {
            heartbeatRound++;
            broadcastAppend(true);
        }
        long confirmed = quorumRound();
        Iterator<PendingRead> it = pendingReads.iterator();
        while (it.hasNext()) {
            PendingRead read = it.next();
            if (read.readIndex >= 0 && read.round <= confirmed && lastApplied >= read.readIndex) {
                it.remove();
                read.future.complete(read.readIndex);
            }
        }
    }

    private long quorumRound() {
        List<Long> rounds = new ArrayList<>();
        rounds.add(heartbeatRound);
        for (String peer : config.peers()) {
            rounds.add(ackedRound.get(peer));
        }
        rounds.sort((a, b) -> Long.compare(b, a));
        return rounds.get(config.majority() - 1);
    }

    private void failPendingRequests() {
        NotLeaderException ex = new NotLeaderException(leaderId);
        for (PendingWrite w : pendingWrites.values()) {
            w.future().completeExceptionally(ex);
        }
        pendingWrites.clear();
        for (PendingRead r : pendingReads) {
            r.future.completeExceptionally(ex);
        }
        pendingReads.clear();
    }

    public synchronized void stop() {
        stopped = true;
        failPendingRequests();
    }

    public synchronized void setEventLog(Consumer<String> eventLog) {
        this.eventLog = eventLog;
    }

    private static LogEntryProto toProto(LogEntry e) {
        return LogEntryProto.newBuilder()
                .setTerm(e.term())
                .setIndex(e.index())
                .setCommand(ByteString.copyFrom(e.command()))
                .build();
    }

    private static LogEntry fromProto(LogEntryProto p) {
        return new LogEntry(p.getTerm(), p.getIndex(), p.getCommand().toByteArray());
    }

    public String id() {
        return config.id();
    }

    public synchronized Role role() {
        return role;
    }

    public synchronized boolean isLeader() {
        return role == Role.LEADER && !stopped;
    }

    public synchronized long currentTerm() {
        return storage.currentTerm();
    }

    public synchronized String leaderId() {
        return leaderId;
    }

    public synchronized long commitIndex() {
        return commitIndex;
    }

    public synchronized long lastApplied() {
        return lastApplied;
    }

    @Override
    public synchronized String toString() {
        return config.id() + "{" + role + " term=" + storage.currentTerm() + " last=" + storage.lastIndex()
                + " commit=" + commitIndex + " applied=" + lastApplied + " snap=" + storage.snapshotIndex() + "}";
    }
}
