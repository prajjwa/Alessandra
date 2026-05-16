package com.alessandra.widecolumn.grpc;

import com.alessandra.widecolumn.model.Cell;
import com.alessandra.widecolumn.model.ColumnMutation;
import com.alessandra.widecolumn.model.RowHistory;
import com.alessandra.widecolumn.model.RowSnapshot;
import com.alessandra.widecolumn.proto.CellVersion;
import com.alessandra.widecolumn.proto.ColumnValue;
import com.alessandra.widecolumn.proto.ReadResponse;
import com.alessandra.widecolumn.proto.VersionHistoryResponse;
import com.google.protobuf.ByteString;

import java.util.List;

public final class GrpcMapper {
    private GrpcMapper() {}

    public static ColumnMutation toMutation(ColumnValue value) {
        return new ColumnMutation(value.getFamily(), value.getQualifier(), value.getValue().toByteArray());
    }

    public static ReadResponse toReadResponse(RowSnapshot snapshot, long coordinatorTimestamp) {
        ReadResponse.Builder builder = ReadResponse.newBuilder()
                .setFound(snapshot.found())
                .setTable(snapshot.table())
                .setRowKey(snapshot.rowKey())
                .setCoordinatorTimestamp(coordinatorTimestamp);
        snapshot.cells().stream().map(GrpcMapper::toCellVersion).forEach(builder::addCells);
        return builder.build();
    }


    public static VersionHistoryResponse toVersionHistoryResponse(RowHistory history, long coordinatorTimestamp) {
        VersionHistoryResponse.Builder builder = VersionHistoryResponse.newBuilder()
                .setFound(history.found())
                .setTable(history.table())
                .setRowKey(history.rowKey())
                .setCoordinatorTimestamp(coordinatorTimestamp);
        history.versions().stream().map(GrpcMapper::toCellVersion).forEach(builder::addVersions);
        return builder.build();
    }

    public static CellVersion toCellVersion(Cell cell) {
        return CellVersion.newBuilder()
                .setFamily(cell.family())
                .setQualifier(cell.qualifier())
                .setValue(ByteString.copyFrom(cell.value()))
                .setTimestamp(cell.timestamp())
                .setTombstone(cell.tombstone())
                .build();
    }


    public static RowHistory mergeHistory(String table, String rowKey, List<VersionHistoryResponse> responses, int limit) {
        int maxVersions = limit <= 0 ? Integer.MAX_VALUE : limit;
        return new RowHistory(table, rowKey, responses.stream()
                .flatMap(response -> response.getVersionsList().stream())
                .collect(java.util.stream.Collectors.toMap(
                        cell -> cell.getFamily() + ":" + cell.getQualifier() + "@" + cell.getTimestamp(),
                        cell -> new Cell(cell.getFamily(), cell.getQualifier(), cell.getValue().toByteArray(), cell.getTimestamp(), cell.getTombstone()),
                        (left, right) -> left.tombstone() == right.tombstone() ? left : (left.tombstone() ? left : right),
                        java.util.LinkedHashMap::new))
                .values().stream()
                .sorted(java.util.Comparator.comparingLong(Cell::timestamp).reversed()
                        .thenComparing(Cell::selector))
                .limit(maxVersions)
                .toList());
    }

    public static RowSnapshot merge(String table, String rowKey, List<ReadResponse> responses) {
        return new RowSnapshot(table, rowKey, responses.stream()
                .flatMap(response -> response.getCellsList().stream())
                .collect(java.util.stream.Collectors.toMap(
                        cell -> cell.getFamily() + ":" + cell.getQualifier(),
                        cell -> new Cell(cell.getFamily(), cell.getQualifier(), cell.getValue().toByteArray(), cell.getTimestamp(), cell.getTombstone()),
                        (left, right) -> left.timestamp() >= right.timestamp() ? left : right,
                        java.util.LinkedHashMap::new))
                .values().stream()
                .filter(cell -> !cell.tombstone())
                .toList());
    }
}
