package dev.raftkv.storage;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public final class FileStorage extends MemoryStorage implements AutoCloseable {
    private final Path dir;
    private final Path metaFile;
    private final Path logFile;
    private final Path snapshotFile;
    private FileChannel log;

    private final List<Long> offsets = new ArrayList<>();

    public FileStorage(Path dir) {
        this.dir = dir;
        this.metaFile = dir.resolve("meta");
        this.logFile = dir.resolve("log");
        this.snapshotFile = dir.resolve("snapshot");
        try {
            Files.createDirectories(dir);
            loadMeta();
            loadSnapshot();
            loadLog();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open storage in " + dir, e);
        }
    }

    private void loadMeta() throws IOException {
        if (!Files.exists(metaFile)) {
            return;
        }
        try (DataInputStream in = new DataInputStream(Files.newInputStream(metaFile))) {
            currentTerm = in.readLong();
            String v = in.readUTF();
            votedFor = v.isEmpty() ? null : v;
        }
    }

    private void loadSnapshot() throws IOException {
        if (!Files.exists(snapshotFile)) {
            return;
        }
        try (DataInputStream in = new DataInputStream(Files.newInputStream(snapshotFile))) {
            long index = in.readLong();
            long term = in.readLong();
            byte[] data = new byte[in.readInt()];
            in.readFully(data);
            snapshot = new Snapshot(index, term, data);
        }
    }

    private void loadLog() throws IOException {
        log = FileChannel.open(logFile, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        long validEnd = 0;
        try (DataInputStream in = new DataInputStream(Files.newInputStream(logFile))) {
            long pos = 0;
            while (true) {
                LogEntry e;
                int recordSize;
                try {
                    int len = in.readInt();
                    if (len < 0 || len > 64 * 1024 * 1024) {
                        break;
                    }
                    long term = in.readLong();
                    long index = in.readLong();
                    byte[] cmd = new byte[len];
                    in.readFully(cmd);
                    int crc = in.readInt();
                    if (crc != crc(term, index, cmd)) {
                        break;
                    }
                    e = new LogEntry(term, index, cmd);
                    recordSize = 4 + 8 + 8 + len + 4;
                } catch (EOFException eof) {
                    break;
                }
                if (e.index() > snapshotIndex()) {
                    if (e.index() != lastIndex() + 1) {
                        break;
                    }
                    entries.add(e);
                    offsets.add(pos);
                }
                pos += recordSize;
                validEnd = pos;
            }
        }
        if (log.size() != validEnd) {
            log.truncate(validEnd);
            log.force(true);
        }
        log.position(validEnd);
    }

    @Override
    public synchronized void setTermAndVote(long term, String votedFor) {
        writeAtomically(metaFile, out -> {
            out.writeLong(term);
            out.writeUTF(votedFor == null ? "" : votedFor);
        });
        super.setTermAndVote(term, votedFor);
    }

    @Override
    public synchronized void append(List<LogEntry> newEntries) {
        super.append(newEntries);
        try {
            long pos = log.size();
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf);
            for (LogEntry e : newEntries) {
                offsets.add(pos + buf.size());
                writeRecord(out, e);
            }
            log.write(ByteBuffer.wrap(buf.toByteArray()), pos);
            log.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void truncateFrom(long index) {
        super.truncateFrom(index);
        try {
            long cut = entries.size() < offsets.size() ? offsets.get(entries.size()) : log.size();
            while (offsets.size() > entries.size()) {
                offsets.remove(offsets.size() - 1);
            }
            log.truncate(cut);
            log.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void installSnapshot(Snapshot s) {
        writeAtomically(snapshotFile, out -> {
            out.writeLong(s.lastIncludedIndex());
            out.writeLong(s.lastIncludedTerm());
            out.writeInt(s.data().length);
            out.write(s.data());
        });
        super.installSnapshot(s);
        rewriteLog();
    }

    private void rewriteLog() {
        try {
            log.close();
            Path tmp = dir.resolve("log.tmp");
            offsets.clear();
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(buf);
                for (LogEntry e : entries) {
                    offsets.add((long) buf.size());
                    writeRecord(out, e);
                }
                ch.write(ByteBuffer.wrap(buf.toByteArray()));
                ch.force(true);
            }
            Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            log = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
            log.position(log.size());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private interface Writer {
        void write(DataOutputStream out) throws IOException;
    }

    private void writeAtomically(Path target, Writer writer) {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            writer.write(new DataOutputStream(buf));
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                ch.write(ByteBuffer.wrap(buf.toByteArray()));
                ch.force(true);
            }
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeRecord(DataOutputStream out, LogEntry e) throws IOException {
        out.writeInt(e.command().length);
        out.writeLong(e.term());
        out.writeLong(e.index());
        out.write(e.command());
        out.writeInt(crc(e.term(), e.index(), e.command()));
    }

    private static int crc(long term, long index, byte[] cmd) {
        CRC32 crc = new CRC32();
        crc.update(ByteBuffer.allocate(16).putLong(term).putLong(index).array());
        crc.update(cmd);
        return (int) crc.getValue();
    }

    @Override
    public synchronized void close() {
        try {
            log.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
