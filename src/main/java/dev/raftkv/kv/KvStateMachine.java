package dev.raftkv.kv;

import dev.raftkv.raft.StateMachine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public final class KvStateMachine implements StateMachine {
    private final Map<String, String> data = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSequence = new HashMap<>();

    @Override
    public synchronized Object apply(long index, byte[] bytes) {
        Command cmd = Command.decode(bytes);
        Long seen = lastSequence.get(cmd.clientId());
        if (seen != null && cmd.sequence() <= seen) {
            return Boolean.FALSE;
        }
        lastSequence.put(cmd.clientId(), cmd.sequence());
        switch (cmd.type()) {
            case PUT -> data.put(cmd.key(), cmd.value());
            case DELETE -> data.remove(cmd.key());
        }
        return Boolean.TRUE;
    }

    public String get(String key) {
        return data.get(key);
    }

    public synchronized Map<String, String> asMap() {
        return new TreeMap<>(data);
    }

    @Override
    public synchronized byte[] snapshot() {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf);
            out.writeInt(data.size());
            for (Map.Entry<String, String> e : new TreeMap<>(data).entrySet()) {
                out.writeUTF(e.getKey());
                out.writeUTF(e.getValue());
            }
            out.writeInt(lastSequence.size());
            for (Map.Entry<String, Long> e : new TreeMap<>(lastSequence).entrySet()) {
                out.writeUTF(e.getKey());
                out.writeLong(e.getValue());
            }
            return buf.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void restore(byte[] snapshot) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshot));
            data.clear();
            lastSequence.clear();
            int n = in.readInt();
            for (int i = 0; i < n; i++) {
                data.put(in.readUTF(), in.readUTF());
            }
            int m = in.readInt();
            for (int i = 0; i < m; i++) {
                lastSequence.put(in.readUTF(), in.readLong());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
