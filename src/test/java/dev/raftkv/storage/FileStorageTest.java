package dev.raftkv.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FileStorageTest {
    @TempDir
    Path dir;

    private static LogEntry entry(long term, long index) {
        return new LogEntry(term, index, ("cmd" + index).getBytes());
    }

    @Test
    void termVoteAndLogSurviveRestart() {
        try (FileStorage s = new FileStorage(dir)) {
            s.setTermAndVote(5, "n2");
            s.append(List.of(entry(1, 1), entry(1, 2), entry(5, 3)));
        }
        try (FileStorage s = new FileStorage(dir)) {
            assertThat(s.currentTerm()).isEqualTo(5);
            assertThat(s.votedFor()).isEqualTo("n2");
            assertThat(s.lastIndex()).isEqualTo(3);
            assertThat(s.termAt(3)).isEqualTo(5);
            assertThat(new String(s.entry(2).command())).isEqualTo("cmd2");
        }
    }

    @Test
    void truncationIsDurable() {
        try (FileStorage s = new FileStorage(dir)) {
            s.append(List.of(entry(1, 1), entry(1, 2), entry(1, 3)));
            s.truncateFrom(2);
            s.append(List.of(entry(2, 2)));
        }
        try (FileStorage s = new FileStorage(dir)) {
            assertThat(s.lastIndex()).isEqualTo(2);
            assertThat(s.termAt(2)).isEqualTo(2);
        }
    }

    @Test
    void snapshotCompactsLogAndSurvivesRestart() {
        try (FileStorage s = new FileStorage(dir)) {
            s.append(List.of(entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4)));
            s.installSnapshot(new Snapshot(2, 1, new byte[]{9, 9}));
            assertThat(s.snapshotIndex()).isEqualTo(2);
            assertThat(s.lastIndex()).isEqualTo(4);
            s.append(List.of(entry(2, 5)));
        }
        try (FileStorage s = new FileStorage(dir)) {
            assertThat(s.snapshot().data()).containsExactly(9, 9);
            assertThat(s.snapshotIndex()).isEqualTo(2);
            assertThat(s.termAt(2)).isEqualTo(1);
            assertThat(s.entries(3, 5)).extracting(LogEntry::index).containsExactly(3L, 4L, 5L);
        }
    }

    @Test
    void snapshotThatDisagreesWithLogDiscardsWholeLog() {
        try (FileStorage s = new FileStorage(dir)) {
            s.append(List.of(entry(1, 1), entry(1, 2), entry(1, 3)));
            s.installSnapshot(new Snapshot(2, 7, new byte[0]));
            assertThat(s.lastIndex()).isEqualTo(2);
        }
    }

    @Test
    void tornWriteAtEndOfLogIsDiscarded() throws IOException {
        try (FileStorage s = new FileStorage(dir)) {
            s.append(List.of(entry(1, 1), entry(1, 2)));
        }

        Files.write(dir.resolve("log"), new byte[]{0, 0, 0, 4, 0, 0, 0}, StandardOpenOption.APPEND);
        try (FileStorage s = new FileStorage(dir)) {
            assertThat(s.lastIndex()).isEqualTo(2);
            s.append(List.of(entry(1, 3)));
        }
        try (FileStorage s = new FileStorage(dir)) {
            assertThat(s.lastIndex()).isEqualTo(3);
        }
    }
}
