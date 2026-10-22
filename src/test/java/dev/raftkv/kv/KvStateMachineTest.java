package dev.raftkv.kv;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KvStateMachineTest {
    @Test
    void putAndDelete() {
        KvStateMachine kv = new KvStateMachine();
        kv.apply(1, Command.put("a", "1", "c", 1).encode());
        kv.apply(2, Command.put("b", "2", "c", 2).encode());
        kv.apply(3, Command.delete("a", "c", 3).encode());
        assertThat(kv.get("a")).isNull();
        assertThat(kv.get("b")).isEqualTo("2");
    }

    @Test
    void retriedWriteIsAppliedOnce() {
        KvStateMachine kv = new KvStateMachine();
        kv.apply(1, Command.put("x", "first", "c", 1).encode());
        kv.apply(2, Command.put("x", "second", "c", 2).encode());

        Object result = kv.apply(3, Command.put("x", "first", "c", 1).encode());
        assertThat(result).isEqualTo(Boolean.FALSE);
        assertThat(kv.get("x")).isEqualTo("second");
    }

    @Test
    void snapshotKeepsDataAndDedupTable() {
        KvStateMachine kv = new KvStateMachine();
        kv.apply(1, Command.put("x", "1", "c", 7).encode());
        KvStateMachine restored = new KvStateMachine();
        restored.restore(kv.snapshot());
        assertThat(restored.asMap()).isEqualTo(kv.asMap());
        restored.apply(2, Command.put("x", "dup", "c", 7).encode());
        assertThat(restored.get("x")).isEqualTo("1");
    }
}
