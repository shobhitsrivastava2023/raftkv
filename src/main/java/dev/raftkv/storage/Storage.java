package dev.raftkv.storage;

import java.util.List;

public interface Storage {
    long currentTerm();

    String votedFor();

    void setTermAndVote(long term, String votedFor);

    long snapshotIndex();

    long snapshotTerm();

    long lastIndex();

    long termAt(long index);

    LogEntry entry(long index);

    List<LogEntry> entries(long from, long toInclusive);

    void append(List<LogEntry> entries);

    void truncateFrom(long index);

    void installSnapshot(Snapshot snapshot);

    Snapshot snapshot();
}
