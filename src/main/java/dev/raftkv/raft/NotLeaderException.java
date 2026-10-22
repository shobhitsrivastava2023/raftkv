package dev.raftkv.raft;

public class NotLeaderException extends RuntimeException {
    private final String leaderId;

    public NotLeaderException(String leaderId) {
        super("not leader; leader is " + leaderId);
        this.leaderId = leaderId;
    }

    public String leaderId() {
        return leaderId;
    }
}
