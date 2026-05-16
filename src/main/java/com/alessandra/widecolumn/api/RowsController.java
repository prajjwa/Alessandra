package com.alessandra.widecolumn.api;

import com.alessandra.widecolumn.cluster.QuorumCoordinator;
import com.alessandra.widecolumn.cluster.QuorumUnavailableException;
import com.alessandra.widecolumn.model.Cell;
import com.alessandra.widecolumn.model.ColumnMutation;
import com.alessandra.widecolumn.model.RowSnapshot;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@RestController
@RequestMapping("/tables/{table}/rows/{rowKey}")
public class RowsController {
    private final QuorumCoordinator coordinator;

    public RowsController(QuorumCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @PutMapping
    public QuorumCoordinator.WriteResult put(@PathVariable String table,
                                             @PathVariable String rowKey,
                                             @RequestBody PutRowRequest request) {
        return coordinator.put(table, rowKey, Optional.ofNullable(request.columns()).orElse(List.of()).stream()
                .filter(column -> column.family() != null && column.qualifier() != null && column.value() != null)
                .map(column -> new ColumnMutation(column.family(), column.qualifier(), column.value().getBytes(StandardCharsets.UTF_8)))
                .toList());
    }

    @GetMapping
    public RowResponse get(@PathVariable String table,
                           @PathVariable String rowKey,
                           @RequestParam(defaultValue = "") List<String> columns,
                           @RequestParam(defaultValue = "0") long readTimestamp) {
        RowSnapshot snapshot = coordinator.get(table, rowKey, normalizeSelectors(columns), readTimestamp);
        return new RowResponse(snapshot.table(), snapshot.rowKey(), snapshot.cells().stream().map(RowResponse.CellResponse::from).toList());
    }

    @DeleteMapping
    public QuorumCoordinator.WriteResult delete(@PathVariable String table,
                                                @PathVariable String rowKey,
                                                @RequestParam List<String> columns) {
        return coordinator.delete(table, rowKey, normalizeSelectors(columns));
    }

    private List<String> normalizeSelectors(List<String> selectors) {
        return selectors.stream()
                .filter(Objects::nonNull)
                .filter(selector -> !selector.isBlank())
                .toList();
    }

    @ExceptionHandler(QuorumUnavailableException.class)
    public ResponseEntity<Map<String, String>> quorumUnavailable(QuorumUnavailableException exception) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", exception.getMessage()));
    }

    public record PutRowRequest(List<ColumnRequest> columns) {}
    public record ColumnRequest(String family, String qualifier, String value) {}

    public record RowResponse(String table, String rowKey, List<CellResponse> cells) {
        public record CellResponse(String family, String qualifier, String value, long timestamp) {
            static CellResponse from(Cell cell) {
                return new CellResponse(cell.family(), cell.qualifier(), new String(cell.value(), StandardCharsets.UTF_8), cell.timestamp());
            }
        }
    }
}
