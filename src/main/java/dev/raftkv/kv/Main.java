package dev.raftkv.kv;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Main {
    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parseArgs(args);
        String id = require(opts, "id");
        int port = Integer.parseInt(opts.getOrDefault("port", "9000"));
        Path dataDir = Path.of(opts.getOrDefault("data", "data/" + id));
        Map<String, String> peers = parseMap(require(opts, "peers"));
        Map<String, String> clientAddrs = parseMap(opts.getOrDefault("client-addrs", ""));
        long snapshotEvery = Long.parseLong(opts.getOrDefault("snapshot-every", "10000"));

        RaftKvServer server = new RaftKvServer(id, port, dataDir, peers, clientAddrs, snapshotEvery, true);
        System.out.println("RaftKV node " + id + " listening on " + port + ", peers " + peers + ", data " + dataDir);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        server.awaitTermination();
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (String name : new String[]{"id", "port", "data", "peers", "client-addrs", "snapshot-every"}) {
            String env = System.getenv("RAFTKV_" + name.toUpperCase().replace('-', '_'));
            if (env != null && !env.isBlank()) {
                opts.put(name, env);
            }
        }
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("expected --flag value, got " + args[i]);
            }
            opts.put(args[i].substring(2), args[i + 1]);
        }
        return opts;
    }

    private static String require(Map<String, String> opts, String name) {
        String v = opts.get(name);
        if (v == null) {
            throw new IllegalArgumentException("missing --" + name);
        }
        return v;
    }

    static Map<String, String> parseMap(String s) {
        Map<String, String> map = new LinkedHashMap<>();
        for (String part : s.split(",")) {
            if (part.isBlank()) {
                continue;
            }
            String[] kv = part.trim().split("=", 2);
            map.put(kv[0], kv[1]);
        }
        return map;
    }
}
