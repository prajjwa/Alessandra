package com.alessandra.widecolumn;

import com.alessandra.widecolumn.model.ColumnMutation;
import com.alessandra.widecolumn.store.RocksDbWideColumnStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RocksDbWideColumnStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void returnsLatestVisibleMvccVersionAtReadTimestamp() throws Exception {
        try (RocksDbWideColumnStore store = new RocksDbWideColumnStore(tempDir.toString())) {
            long first = store.put("users", "u1", List.of(new ColumnMutation("profile", "name", bytes("Ada"))), 100);
            long second = store.put("users", "u1", List.of(new ColumnMutation("profile", "name", bytes("Grace"))), 200);

            assertThat(first).isEqualTo(100);
            assertThat(second).isEqualTo(200);
            assertThat(store.get("users", "u1", List.of("profile:name"), 150).cells())
                    .singleElement()
                    .satisfies(cell -> assertThat(new String(cell.value(), StandardCharsets.UTF_8)).isEqualTo("Ada"));
            assertThat(store.get("users", "u1", List.of("profile:name"), 0).cells())
                    .singleElement()
                    .satisfies(cell -> assertThat(new String(cell.value(), StandardCharsets.UTF_8)).isEqualTo("Grace"));
        }
    }

    @Test
    void tombstonesHideDeletedColumns() throws Exception {
        try (RocksDbWideColumnStore store = new RocksDbWideColumnStore(tempDir.toString())) {
            store.put("users", "u1", List.of(new ColumnMutation("profile", "email", bytes("ada@example.com"))), 100);
            store.delete("users", "u1", List.of("profile:email"), 200);

            assertThat(store.get("users", "u1", List.of("profile:email"), 0).cells()).isEmpty();
            assertThat(store.get("users", "u1", List.of("profile:email"), 150).cells()).hasSize(1);
        }
    }


    @Test
    void returnsMvccVersionHistoryWithTimestampBoundsAndLimit() throws Exception {
        try (RocksDbWideColumnStore store = new RocksDbWideColumnStore(tempDir.toString())) {
            store.put("users", "u1", List.of(new ColumnMutation("profile", "name", bytes("Ada"))), 100);
            store.put("users", "u1", List.of(new ColumnMutation("profile", "email", bytes("ada@example.com"))), 125);
            store.put("users", "u1", List.of(new ColumnMutation("profile", "name", bytes("Grace"))), 200);
            store.put("users", "u1", List.of(new ColumnMutation("profile", "name", bytes("Katherine"))), 300);

            assertThat(store.getVersions("users", "u1", List.of("profile:name"), 100, 250, false, 10).versions())
                    .extracting(cell -> new String(cell.value(), StandardCharsets.UTF_8))
                    .containsExactly("Grace", "Ada");
            assertThat(store.getVersions("users", "u1", List.of(), 0, 0, false, 2).versions())
                    .extracting(cell -> new String(cell.value(), StandardCharsets.UTF_8))
                    .containsExactly("Katherine", "Grace");
        }
    }

    @Test
    void versionHistoryCanIncludeTombstones() throws Exception {
        try (RocksDbWideColumnStore store = new RocksDbWideColumnStore(tempDir.toString())) {
            store.put("users", "u1", List.of(new ColumnMutation("profile", "email", bytes("ada@example.com"))), 100);
            store.delete("users", "u1", List.of("profile:email"), 200);

            assertThat(store.getVersions("users", "u1", List.of("profile:email"), 0, 0, false, 10).versions())
                    .singleElement()
                    .satisfies(cell -> assertThat(cell.timestamp()).isEqualTo(100));
            assertThat(store.getVersions("users", "u1", List.of("profile:email"), 0, 0, true, 10).versions())
                    .extracting("timestamp")
                    .containsExactly(200L, 100L);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
