package com.alessandra.widecolumn.cluster;

import com.alessandra.widecolumn.config.DatabaseProperties;
import com.alessandra.widecolumn.grpc.GrpcMapper;
import com.alessandra.widecolumn.model.ColumnMutation;
import com.alessandra.widecolumn.model.RowSnapshot;
import com.alessandra.widecolumn.proto.ColumnValue;
import com.alessandra.widecolumn.proto.DeleteRequest;
import com.alessandra.widecolumn.proto.GetRequest;
import com.alessandra.widecolumn.proto.PutRequest;
import com.alessandra.widecolumn.proto.ReadResponse;
import com.alessandra.widecolumn.proto.ReplicationEnvelope;
import com.alessandra.widecolumn.store.WideColumnStore;
import com.google.protobuf.ByteString;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Service
public class QuorumCoordinator {
    private final DatabaseProperties properties;
    private final ConsistentHashRing ring;
    private final WideColumnStore store;
    private final ReplicaClient replicaClient;

    public QuorumCoordinator(DatabaseProperties properties, ConsistentHashRing ring, WideColumnStore store, ReplicaClient replicaClient) {
        this.properties = properties;
        this.ring = ring;
        this.store = store;
        this.replicaClient = replicaClient;
    }

    public WriteResult put(String table, String rowKey, Collection<ColumnMutation> columns) {
        long timestamp = System.currentTimeMillis();
        PutRequest put = PutRequest.newBuilder()
                .setTable(table)
                .setRowKey(rowKey)
                .setTimestamp(timestamp)
                .addAllColumns(columns.stream().map(column -> ColumnValue.newBuilder()
                        .setFamily(column.family())
                        .setQualifier(column.qualifier())
                        .setValue(ByteString.copyFrom(column.value()))
                        .build()).toList())
                .build();
        return replicate(table, rowKey, ReplicationEnvelope.newBuilder()
                .setSourceNodeId(properties.getNodeId())
                .setPut(put)
                .build(), () -> store.put(table, rowKey, columns, timestamp));
    }

    public WriteResult delete(String table, String rowKey, Collection<String> selectors) {
        long timestamp = System.currentTimeMillis();
        DeleteRequest delete = DeleteRequest.newBuilder()
                .setTable(table)
                .setRowKey(rowKey)
                .setTimestamp(timestamp)
                .addAllColumnSelectors(selectors)
                .build();
        return replicate(table, rowKey, ReplicationEnvelope.newBuilder()
                .setSourceNodeId(properties.getNodeId())
                .setDelete(delete)
                .build(), () -> store.delete(table, rowKey, selectors, timestamp));
    }

    public RowSnapshot get(String table, String rowKey, Collection<String> selectors, long readTimestamp) {
        GetRequest request = GetRequest.newBuilder()
                .setTable(table)
                .setRowKey(rowKey)
                .setReadTimestamp(readTimestamp)
                .addAllColumnSelectors(selectors)
                .build();
        List<ReadResponse> responses = new ArrayList<>();
        for (DatabaseProperties.Node replica : ring.replicasFor(table, rowKey)) {
            try {
                if (replicaClient.isLocal(replica)) {
                    responses.add(GrpcMapper.toReadResponse(store.get(table, rowKey, selectors, readTimestamp), System.currentTimeMillis()));
                } else {
                    responses.add(replicaClient.get(replica, request));
                }
                if (responses.size() >= properties.getReadQuorum()) {
                    break;
                }
            } catch (RuntimeException ignored) {
                // Prototype behavior: skip unavailable replicas and fail if quorum is not reached.
            }
        }
        if (responses.size() < properties.getReadQuorum()) {
            throw new QuorumUnavailableException("Read quorum not reached for " + table + "/" + rowKey);
        }
        return GrpcMapper.merge(table, rowKey, responses);
    }

    private WriteResult replicate(String table, String rowKey, ReplicationEnvelope envelope, LocalMutation localMutation) {
        List<String> acknowledgements = new ArrayList<>();
        for (DatabaseProperties.Node replica : ring.replicasFor(table, rowKey)) {
            try {
                if (replicaClient.isLocal(replica)) {
                    localMutation.apply();
                    acknowledgements.add(replica.id());
                } else if (replicaClient.replicate(replica, envelope).getAcknowledged()) {
                    acknowledgements.add(replica.id());
                }
                if (acknowledgements.size() >= properties.getWriteQuorum()) {
                    break;
                }
            } catch (RuntimeException ignored) {
                // Prototype behavior: skip unavailable replicas and fail if quorum is not reached.
            }
        }
        if (acknowledgements.size() < properties.getWriteQuorum()) {
            throw new QuorumUnavailableException("Write quorum not reached for " + table + "/" + rowKey);
        }
        return new WriteResult(acknowledgements);
    }

    @FunctionalInterface
    private interface LocalMutation {
        void apply();
    }

    public record WriteResult(List<String> acknowledgements) {}
}
