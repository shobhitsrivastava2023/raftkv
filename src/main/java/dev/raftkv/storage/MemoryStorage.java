package dev.raftkv.storage;

import java.util.ArrayList;
import java.util.List;

public class MemoryStorage implements Storage {
    protected long currentTerm;
    protected String votedFor;
    protected Snapshot snapshot;

    protected final List<LogEntry> entries = new ArrayList<>();

    @Override
    public synchronized long currentTerm() {
        return currentTerm;
    }

    @Override
    public synchronized String votedFor() {
        return votedFor;
    }

    @Override
    public synchronized void setTermAndVote(long term, String votedFor) {
        this.currentTerm = term;
        this.votedFor = votedFor;
    }

    @Override
    public synchronized long snapshotIndex() {
        return snapshot == null ? 0 : snapshot.lastIncludedIndex();
    }

    @Override
    public synchronized long snapshotTerm() {
        return snapshot == null ? 0 : snapshot.lastIncludedTerm();
    }

    @Override
    public synchronized long lastIndex() {
        return snapshotIndex() + entries.size();
    }

    @Override
    public synchronized long termAt(long index) {
        if (index == snapshotIndex()) {
            return snapshotTerm();
        }
        return entry(index).term();
    }

    @Override
    public synchronized LogEntry entry(long index) {
        long offset = index - snapshotIndex() - 1;
        if (offset < 0 || offset >= entries.size()) {
            throw new IndexOutOfBoundsException("index " + index + " not in log ("
                    + (snapshotIndex() + 1) + ".." + lastIndex() + ")");
        }
        return entries.get((int) offset);
    }

    @Override
    public synchronized List<LogEntry> entries(long from, long toInclusive) {
        if (from > toInclusive) {
            return List.of();
        }
        int start = (int) (from - snapshotIndex() - 1);
        int end = (int) (toInclusive - snapshotIndex());
        if (start < 0 || end > entries.size()) {
            throw new IndexOutOfBoundsException("range " + from + ".." + toInclusive + " not in log");
        }
        return List.copyOf(entries.subList(start, end));
    }

    @Override
    public synchronized void append(List<LogEntry> newEntries) {
        long expected = lastIndex() + 1;
        for (LogEntry e : newEntries) {
            if (e.index() != expected++) {
                throw new IllegalArgumentException("gap in log: expected " + (expected - 1) + " got " + e.index());
            }
        }
        entries.addAll(newEntries);
    }

    @Override
    public synchronized void truncateFrom(long index) {
        int keep = (int) (index - snapshotIndex() - 1);
        if (keep < 0) {
            throw new IllegalArgumentException("cannot truncate into the snapshot at " + index);
        }
        while (entries.size() > keep) {
            entries.remove(entries.size() - 1);
        }
    }

    @Override
    public synchronized void installSnapshot(Snapshot s) {
        long idx = s.lastIncludedIndex();
        boolean keepSuffix = idx > snapshotIndex() && idx <= lastIndex() && termAt(idx) == s.lastIncludedTerm();
        List<LogEntry> suffix = keepSuffix ? entries(idx + 1, lastIndex()) : List.of();
        entries.clear();
        entries.addAll(suffix);
        snapshot = s;
    }

    @Override
    public synchronized Snapshot snapshot() {
        return snapshot;
    }
}
