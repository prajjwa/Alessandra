package com.alessandra.widecolumn.cluster;

import com.alessandra.widecolumn.config.DatabaseProperties;
import com.alessandra.widecolumn.proto.GetRequest;
import com.alessandra.widecolumn.proto.ReadResponse;
import com.alessandra.widecolumn.proto.ReplicationEnvelope;
import com.alessandra.widecolumn.proto.VersionHistoryRequest;
import com.alessandra.widecolumn.proto.VersionHistoryResponse;
import com.alessandra.widecolumn.proto.WideColumnNodeGrpc;
import com.alessandra.widecolumn.proto.WriteResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ReplicaClient implements AutoCloseable {
    private final DatabaseProperties properties;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public ReplicaClient(DatabaseProperties properties) {
        this.properties = properties;
    }

    public WriteResponse replicate(DatabaseProperties.Node node, ReplicationEnvelope envelope) {
        return blockingStub(node).replicate(envelope);
    }

    public ReadResponse get(DatabaseProperties.Node node, GetRequest request) {
        return blockingStub(node).get(request);
    }

    public VersionHistoryResponse getVersions(DatabaseProperties.Node node, VersionHistoryRequest request) {
        return blockingStub(node).getVersions(request);
    }

    public boolean isLocal(DatabaseProperties.Node node) {
        return properties.getNodeId().equals(node.id());
    }

    private WideColumnNodeGrpc.WideColumnNodeBlockingStub blockingStub(DatabaseProperties.Node node) {
        ManagedChannel channel = channels.computeIfAbsent(node.id(), ignored -> ManagedChannelBuilder
                .forAddress(node.host(), node.grpcPort())
                .usePlaintext()
                .build());
        return WideColumnNodeGrpc.newBlockingStub(channel)
                .withDeadlineAfter(Duration.ofSeconds(2).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdown);
    }
}
