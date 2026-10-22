package dev.raftkv.raft;

public interface Clock {
    long nowMillis();

    Clock SYSTEM = System::currentTimeMillis;
}
