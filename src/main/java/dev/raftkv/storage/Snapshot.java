package dev.raftkv.storage;

public record Snapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] data) {
}
