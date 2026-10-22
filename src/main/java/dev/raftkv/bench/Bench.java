package dev.raftkv.bench;

import dev.raftkv.client.RaftKvClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Bench {
    private static final List<String> CLUSTER = List.of("localhost:9001", "localhost:9002", "localhost:9003");
    private static final Map<String, String> CONTAINER = Map.of(
            "localhost:9001", "raftkv-n1", "localhost:9002", "raftkv-n2", "localhost:9003", "raftkv-n3");

    public static void main(String[] args) throws Exception {
        int seconds = intArg(args, "--seconds", 30);
        int threads = intArg(args, "--threads", 16);
        int trials = intArg(args, "--trials", 3);

        System.out.printf("RaftKV benchmark: %d client threads, %d s, value size 100 B, cluster %s%n",
                threads, seconds, CLUSTER);
        System.out.printf("JVM %s, %d CPUs available to the client%n",
                System.getProperty("java.version"), Runtime.getRuntime().availableProcessors());
        try (RaftKvClient probe = new RaftKvClient(CLUSTER)) {
            probe.put("bench-warmup", "x");
        }
        throughput(threads, seconds);
        if (trials > 0) {
            failover(trials);
        }
    }

    private static void throughput(int threads, int seconds) throws InterruptedException {
        String value = "x".repeat(100);
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch done = new CountDownLatch(threads);
        List<long[]> perThread = new ArrayList<>();
        int[] counts = new int[threads];
        int[] errors = new int[threads];
        for (int t = 0; t < threads; t++) {
            long[] latencies = new long[500_000];
            perThread.add(latencies);
            int id = t;
            Thread.ofPlatform().start(() -> {
                try (RaftKvClient client = new RaftKvClient(CLUSTER)) {
                    int n = 0;
                    while (running.get()) {
                        long start = System.nanoTime();
                        try {
                            client.put("t" + id + "-" + n, value);
                        } catch (RuntimeException e) {
                            errors[id]++;
                            continue;
                        }
                        if (n < latencies.length) {
                            latencies[n] = System.nanoTime() - start;
                        }
                        n++;
                    }
                    counts[id] = n;
                } finally {
                    done.countDown();
                }
            });
        }
        long start = System.nanoTime();
        Thread.sleep(seconds * 1000L);
        running.set(false);
        done.await();
        double elapsed = (System.nanoTime() - start) / 1e9;

        int total = Arrays.stream(counts).sum();
        long[] all = new long[total];
        int pos = 0;
        for (int t = 0; t < threads; t++) {
            int n = Math.min(counts[t], perThread.get(t).length);
            System.arraycopy(perThread.get(t), 0, all, pos, n);
            pos += n;
        }
        all = Arrays.copyOf(all, pos);
        Arrays.sort(all);
        System.out.println();
        System.out.println("== Throughput ==");
        System.out.printf("acknowledged writes : %d in %.1f s%n", total, elapsed);
        System.out.printf("writes/sec          : %.0f%n", total / elapsed);
        System.out.printf("latency p50         : %.2f ms%n", percentile(all, 0.50));
        System.out.printf("latency p99         : %.2f ms%n", percentile(all, 0.99));
        System.out.printf("latency max         : %.2f ms%n", all.length == 0 ? 0 : all[all.length - 1] / 1e6);
        System.out.printf("failed writes       : %d%n", Arrays.stream(errors).sum());
    }

    private static void failover(int trials) throws Exception {
        System.out.println();
        System.out.println("== Leader failover (docker kill leader -> first successful write) ==");
        List<Long> results = new ArrayList<>();
        for (int trial = 1; trial <= trials; trial++) {
            try (RaftKvClient client = new RaftKvClient(CLUSTER, 30_000)) {
                client.put("failover-probe", "before");
                String leader = client.currentLeader();
                String container = CONTAINER.get(leader);
                long killedAt = System.nanoTime();
                docker("kill", container);
                long afterKill = System.nanoTime();
                client.put("failover-probe", "after-" + trial);
                long recovered = System.nanoTime();
                long ms = (recovered - killedAt) / 1_000_000;
                results.add(ms);
                System.out.printf("trial %d: killed %s (%s); writes succeeded again after %d ms "
                                + "(docker kill itself took %d ms)%n",
                        trial, container, leader, ms, (afterKill - killedAt) / 1_000_000);
                docker("start", container);
                Thread.sleep(3000);
            }
        }
        long[] sorted = results.stream().mapToLong(Long::longValue).sorted().toArray();
        System.out.printf("failover median     : %d ms (min %d, max %d, %d trials)%n",
                sorted[sorted.length / 2], sorted[0], sorted[sorted.length - 1], sorted.length);
    }

    private static void docker(String... args) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        cmd.addAll(List.of(args));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        p.getInputStream().readAllBytes();
        if (p.waitFor() != 0) {
            throw new IllegalStateException("command failed: " + cmd);
        }
    }

    private static double percentile(long[] sorted, double q) {
        if (sorted.length == 0) {
            return 0;
        }
        int i = (int) Math.ceil(q * sorted.length) - 1;
        return sorted[Math.max(0, i)] / 1e6;
    }

    private static int intArg(String[] args, String name, int def) {
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].equals(name)) {
                return Integer.parseInt(args[i + 1]);
            }
        }
        return def;
    }
}
