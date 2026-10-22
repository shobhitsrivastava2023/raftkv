package dev.raftkv.client;

import java.util.Arrays;
import java.util.List;

public final class Cli {
    public static void main(String[] args) {
        String cluster = System.getenv().getOrDefault("RAFTKV_CLUSTER", "localhost:9001,localhost:9002,localhost:9003");
        List<String> rest = Arrays.asList(args);
        if (rest.size() >= 2 && rest.get(0).equals("--cluster")) {
            cluster = rest.get(1);
            rest = rest.subList(2, rest.size());
        }
        if (rest.isEmpty()) {
            usage();
        }
        try (RaftKvClient client = new RaftKvClient(List.of(cluster.split(",")))) {
            switch (rest.get(0)) {
                case "put" -> {
                    if (rest.size() != 3) usage();
                    client.put(rest.get(1), rest.get(2));
                    System.out.println("OK");
                }
                case "get" -> {
                    if (rest.size() != 2) usage();
                    System.out.println(client.get(rest.get(1)).orElse("(not found)"));
                }
                case "delete" -> {
                    if (rest.size() != 2) usage();
                    client.delete(rest.get(1));
                    System.out.println("OK");
                }
                default -> usage();
            }
        } catch (IllegalStateException e) {
            System.err.println("error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void usage() {
        System.err.println("usage: cli [--cluster h:p,h:p,...] put KEY VALUE | get KEY | delete KEY");
        System.exit(2);
    }
}
