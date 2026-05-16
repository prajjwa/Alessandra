package com.alessandra.widecolumn.cluster;

import com.alessandra.widecolumn.config.DatabaseProperties;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

@Component
public class ConsistentHashRing {
    private static final int VIRTUAL_NODES = 64;
    private final SortedMap<Long, DatabaseProperties.Node> ring = new TreeMap<>();
    private final DatabaseProperties properties;

    public ConsistentHashRing(DatabaseProperties properties) {
        this.properties = properties;
        rebuild(properties.getNodes());
    }

    public final void rebuild(List<DatabaseProperties.Node> nodes) {
        ring.clear();
        nodes.stream().sorted(Comparator.comparing(DatabaseProperties.Node::id)).forEach(node -> {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                ring.put(hash(node.id() + "#" + i), node);
            }
        });
    }

    public List<DatabaseProperties.Node> replicasFor(String table, String rowKey) {
        if (ring.isEmpty()) {
            return List.of();
        }
        int desired = Math.min(properties.getReplicationFactor(), properties.getNodes().size());
        List<DatabaseProperties.Node> replicas = new ArrayList<>();
        long token = hash(table + ":" + rowKey);
        appendUniqueReplicas(ring.tailMap(token), replicas, desired);
        appendUniqueReplicas(ring, replicas, desired);
        return replicas;
    }

    private static void appendUniqueReplicas(SortedMap<Long, DatabaseProperties.Node> candidates,
                                             List<DatabaseProperties.Node> replicas,
                                             int desired) {
        for (DatabaseProperties.Node node : candidates.values()) {
            if (replicas.stream().noneMatch(existing -> existing.id().equals(node.id()))) {
                replicas.add(node);
            }
            if (replicas.size() == desired) {
                return;
            }
        }
    }

    private static long hash(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            long result = 0;
            for (int i = 0; i < Long.BYTES; i++) {
                result = (result << 8) | (digest[i] & 0xffL);
            }
            return result & Long.MAX_VALUE;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
