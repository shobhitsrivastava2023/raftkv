package dev.raftkv.raft;

public interface StateMachine {
    Object apply(long index, byte[] command);

    byte[] snapshot();

    void restore(byte[] snapshot);
}
