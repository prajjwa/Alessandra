package com.alessandra.widecolumn.grpc;

import com.alessandra.widecolumn.config.DatabaseProperties;
import com.alessandra.widecolumn.proto.DeleteRequest;
import com.alessandra.widecolumn.proto.GetRequest;
import com.alessandra.widecolumn.proto.PutRequest;
import com.alessandra.widecolumn.proto.ReadResponse;
import com.alessandra.widecolumn.proto.ReplicationEnvelope;
import com.alessandra.widecolumn.proto.WideColumnNodeGrpc;
import com.alessandra.widecolumn.proto.WriteResponse;
import com.alessandra.widecolumn.store.WideColumnStore;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

@Service
public class WideColumnNodeService extends WideColumnNodeGrpc.WideColumnNodeImplBase {
    private final DatabaseProperties properties;
    private final WideColumnStore store;

    public WideColumnNodeService(DatabaseProperties properties, WideColumnStore store) {
        this.properties = properties;
        this.store = store;
    }

    @Override
    public void put(PutRequest request, StreamObserver<WriteResponse> responseObserver) {
        long timestamp = store.put(request.getTable(), request.getRowKey(), request.getColumnsList().stream()
                .map(GrpcMapper::toMutation).toList(), request.getTimestamp());
        acknowledge(timestamp, "local put", responseObserver);
    }

    @Override
    public void get(GetRequest request, StreamObserver<ReadResponse> responseObserver) {
        responseObserver.onNext(GrpcMapper.toReadResponse(store.get(request.getTable(), request.getRowKey(),
                request.getColumnSelectorsList(), request.getReadTimestamp()), System.currentTimeMillis()));
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<WriteResponse> responseObserver) {
        long timestamp = store.delete(request.getTable(), request.getRowKey(), request.getColumnSelectorsList(), request.getTimestamp());
        acknowledge(timestamp, "local delete", responseObserver);
    }

    @Override
    public void replicate(ReplicationEnvelope request, StreamObserver<WriteResponse> responseObserver) {
        if (request.hasPut()) {
            put(request.getPut(), responseObserver);
        } else if (request.hasDelete()) {
            delete(request.getDelete(), responseObserver);
        } else {
            acknowledge(0, "empty replication envelope", responseObserver);
        }
    }

    private void acknowledge(long timestamp, String message, StreamObserver<WriteResponse> responseObserver) {
        responseObserver.onNext(WriteResponse.newBuilder()
                .setAcknowledged(true)
                .setNodeId(properties.getNodeId())
                .setAppliedTimestamp(timestamp)
                .setMessage(message)
                .build());
        responseObserver.onCompleted();
    }
}
