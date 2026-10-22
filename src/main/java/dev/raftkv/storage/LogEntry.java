package dev.raftkv.storage;

public record LogEntry(long term, long index, byte[] command) {
    public boolean isNoop() {
        return command.length == 0;
    }
}
