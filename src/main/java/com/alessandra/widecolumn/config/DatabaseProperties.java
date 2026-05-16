package com.alessandra.widecolumn.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "alessandra")
public class DatabaseProperties {
    private String nodeId = "node-a";
    private int grpcPort = 9090;
    private int replicationFactor = 3;
    private int readQuorum = 2;
    private int writeQuorum = 2;
    private List<Node> nodes = new ArrayList<>(List.of(new Node("node-a", "localhost", 9090)));

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public int getGrpcPort() { return grpcPort; }
    public void setGrpcPort(int grpcPort) { this.grpcPort = grpcPort; }
    public int getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; }
    public int getReadQuorum() { return readQuorum; }
    public void setReadQuorum(int readQuorum) { this.readQuorum = readQuorum; }
    public int getWriteQuorum() { return writeQuorum; }
    public void setWriteQuorum(int writeQuorum) { this.writeQuorum = writeQuorum; }
    public List<Node> getNodes() { return nodes; }
    public void setNodes(List<Node> nodes) { this.nodes = nodes; }

    public record Node(String id, String host, int grpcPort) {}
}
