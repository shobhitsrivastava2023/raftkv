package dev.raftkv.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

public record Command(Type type, String key, String value, String clientId, long sequence) {
    public enum Type { PUT, DELETE }

    public static Command put(String key, String value, String clientId, long sequence) {
        return new Command(Type.PUT, key, value, clientId, sequence);
    }

    public static Command delete(String key, String clientId, long sequence) {
        return new Command(Type.DELETE, key, "", clientId, sequence);
    }

    public byte[] encode() {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf);
            out.writeByte(type.ordinal() + 1);
            out.writeUTF(key);
            out.writeUTF(value);
            out.writeUTF(clientId);
            out.writeLong(sequence);
            return buf.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Command decode(byte[] bytes) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            Type type = Type.values()[in.readByte() - 1];
            return new Command(type, in.readUTF(), in.readUTF(), in.readUTF(), in.readLong());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
