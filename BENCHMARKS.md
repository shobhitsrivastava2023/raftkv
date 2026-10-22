# Benchmarks

Every number here was printed by `bench.Bench`. Nothing is estimated or rounded up.

## Machine

| | |
|---|---|
| CPU | AMD Ryzen 5 5600H (6 cores / 12 threads) |
| RAM | 7.3 GB |
| Disk | SK hynix PC711 512 GB NVMe SSD |
| OS | Windows 11 Home |
| JDK | 25.0.1 for the client and local nodes (bytecode targets Java 21) |

## 3-node Docker cluster (`docker compose up` + `./gradlew bench`)

Docker Desktop 28.1.1 (WSL2 backend; the Linux VM has 12 CPUs and 3.5 GB RAM). The nodes run
Temurin 21 in containers, with data on Docker named volumes. The client ran on the Windows host.
Command: `./gradlew bench` (defaults: 16 threads, 30 s, 3 failover trials).

```
RaftKV benchmark: 16 client threads, 30 s, value size 100 B, cluster [localhost:9001, localhost:9002, localhost:9003]
JVM 25.0.1, 12 CPUs available to the client

== Throughput ==
acknowledged writes : 4562 in 30.1 s
writes/sec          : 152
latency p50         : 105.06 ms
latency p99         : 209.88 ms
latency max         : 551.35 ms
failed writes       : 0

== Leader failover (docker kill leader -> first successful write) ==
trial 1: killed raftkv-n1 (localhost:9001); writes succeeded again after 963 ms (docker kill itself took 825 ms)
trial 2: killed raftkv-n2 (localhost:9002); writes succeeded again after 631 ms (docker kill itself took 449 ms)
trial 3: killed raftkv-n3 (localhost:9003); writes succeeded again after 527 ms (docker kill itself took 398 ms)
failover median     : 631 ms (min 527, max 963, 3 trials)
```

**Why throughput is low: fsync.** I measured sync-write latency inside a container on the same volume:

```
$ docker exec raftkv-n1 sh -c 'cd /data && dd if=/dev/zero of=fsync-probe bs=100 count=300 oflag=dsync'
30000 bytes (30 kB, 29 KiB) copied, 2.00182 s, 15 kB/s
```

That is about 6.7 ms per synced write, and 1 / 6.7 ms ≈ 150 writes/s, which matches the 152 writes/s
above. The leader fsyncs each client write separately while holding the node lock, so writes are
serialized on that fsync, and with 16 clients queued, each one waits about 16 × 6.7 ms ≈ 105 ms (the
p50). This is the "one fsync per append, no group commit" limitation from the README. Group commit
(one fsync for every write that arrived meanwhile) would remove most of this.

**Reading the failover numbers.** The timer starts *before* `docker kill` is issued and stops at the
first acknowledged write, so it includes the time Docker takes to kill the container, the followers'
election timeout (300–600 ms after the last heartbeat), the election, the new leader's no-op commit,
and the client finding the new leader. Three trials is a small sample.

## 3 local processes (no Docker), throughput only

This is a smoke test of the benchmark tool, *not* the Docker benchmark. Three nodes ran as plain
processes on the machine above (`build/install/raftkv/bin/raftkv`, data on the NVMe SSD, every append
fsynced), and the client ran on the same machine. Command: `./gradlew bench --args="--seconds 10 --trials 0"`
(10 s, no failover trials, because failover uses `docker kill`).

```
RaftKV benchmark: 16 client threads, 10 s, value size 100 B, cluster [localhost:9001, localhost:9002, localhost:9003]
JVM 25.0.1, 12 CPUs available to the client

== Throughput ==
acknowledged writes : 7155 in 10.0 s
writes/sec          : 715
latency p50         : 15.38 ms
latency p99         : 59.58 ms
latency max         : 119.02 ms
failed writes       : 0
```

**Reading it:** each write costs a leader fsync plus a follower fsync (in parallel on two followers)
before it commits, and RaftKV does one fsync per append, with no group commit. 715 writes/s
means the leader got through one write about every 1.4 ms, roughly one NTFS fsync on this SSD. It's the
same bottleneck as in Docker, where each fsync is slower (about 6.7 ms) because it goes through the VM's
virtual disk.
