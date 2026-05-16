package com.alessandra.widecolumn;

import com.alessandra.widecolumn.cluster.ConsistentHashRing;
import com.alessandra.widecolumn.config.DatabaseProperties;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ConsistentHashRingTest {
    @Test
    void choosesUniqueReplicasForShard() {
        DatabaseProperties properties = new DatabaseProperties();
        properties.setReplicationFactor(3);
        properties.setNodes(List.of(
                new DatabaseProperties.Node("node-a", "localhost", 9090),
                new DatabaseProperties.Node("node-b", "localhost", 9091),
                new DatabaseProperties.Node("node-c", "localhost", 9092)));

        List<DatabaseProperties.Node> replicas = new ConsistentHashRing(properties).replicasFor("users", "u1");

        assertThat(replicas).hasSize(3);
        assertThat(replicas.stream().map(DatabaseProperties.Node::id)).doesNotHaveDuplicates();
    }
}
