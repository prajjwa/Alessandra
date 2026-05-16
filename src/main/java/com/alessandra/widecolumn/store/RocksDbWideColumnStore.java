package com.alessandra.widecolumn.store;

import com.alessandra.widecolumn.model.Cell;
import com.alessandra.widecolumn.model.ColumnMutation;
import com.alessandra.widecolumn.model.RowHistory;
import com.alessandra.widecolumn.model.RowSnapshot;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class RocksDbWideColumnStore implements WideColumnStore, DisposableBean {
    private static final byte SEP = 0;
    private static final long MAX_TS = Long.MAX_VALUE;

    static {
        RocksDB.loadLibrary();
    }

    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    private final Options options;
    private final RocksDB db;

    public RocksDbWideColumnStore(@Value("${alessandra.storage.path:data/rocksdb}") String path) throws Exception {
        Files.createDirectories(Path.of(path));
        this.options = new Options().setCreateIfMissing(true);
        this.db = RocksDB.open(options, path);
    }

    @Override
    public long put(String table, String rowKey, Collection<ColumnMutation> columns, long timestamp) {
        long ts = assignTimestamp(timestamp);
        for (ColumnMutation column : columns) {
            writeCell(table, rowKey, column.family(), column.qualifier(), column.value(), ts, false);
        }
        return ts;
    }

    @Override
    public long delete(String table, String rowKey, Collection<String> selectors, long timestamp) {
        long ts = assignTimestamp(timestamp);
        for (String selector : selectors) {
            String[] parts = selector.split(":", 2);
            if (parts.length == 2) {
                writeCell(table, rowKey, parts[0], parts[1], new byte[0], ts, true);
            }
        }
        return ts;
    }

    @Override
    public RowSnapshot get(String table, String rowKey, Collection<String> selectors, long readTimestamp) {
        long effectiveReadTs = readTimestamp <= 0 ? MAX_TS : readTimestamp;
        Set<String> requested = selectors == null || selectors.isEmpty() ? Set.of() : new HashSet<>(selectors);
        Map<String, Cell> latestVisible = new LinkedHashMap<>();
        byte[] prefix = prefix(table, rowKey);
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix);
            while (iterator.isValid() && startsWith(iterator.key(), prefix)) {
                DecodedKey decoded = decode(iterator.key());
                if ((requested.isEmpty() || requested.contains(decoded.selector())) && decoded.timestamp() <= effectiveReadTs) {
                    latestVisible.putIfAbsent(decoded.selector(), deserialize(decoded, iterator.value()));
                }
                iterator.next();
            }
        }
        List<Cell> visible = latestVisible.values().stream()
                .filter(cell -> !cell.tombstone())
                .toList();
        return new RowSnapshot(table, rowKey, visible);
    }


    @Override
    public RowHistory getVersions(String table, String rowKey, Collection<String> selectors, long fromTimestamp, long toTimestamp, boolean includeTombstones, int limit) {
        Set<String> requested = selectors == null || selectors.isEmpty() ? Set.of() : new HashSet<>(selectors);
        long lowerBound = Math.max(0, fromTimestamp);
        long upperBound = toTimestamp <= 0 ? MAX_TS : toTimestamp;
        int maxVersions = limit <= 0 ? Integer.MAX_VALUE : limit;
        List<Cell> versions = new ArrayList<>();
        byte[] prefix = prefix(table, rowKey);
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix);
            while (iterator.isValid() && startsWith(iterator.key(), prefix)) {
                DecodedKey decoded = decode(iterator.key());
                if (matchesHistoryFilter(decoded, requested, lowerBound, upperBound)) {
                    Cell cell = deserialize(decoded, iterator.value());
                    if (includeTombstones || !cell.tombstone()) {
                        versions.add(cell);
                    }
                }
                iterator.next();
            }
        }
        List<Cell> ordered = versions.stream()
                .sorted(java.util.Comparator.comparingLong(Cell::timestamp).reversed()
                        .thenComparing(Cell::selector))
                .limit(maxVersions)
                .toList();
        return new RowHistory(table, rowKey, ordered);
    }

    @Override
    public List<String> scanRowKeys(String table, String startRow, int limit) {
        List<String> keys = new ArrayList<>();
        String prefix = table + "\u0000";
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek((prefix + (startRow == null ? "" : startRow)).getBytes(StandardCharsets.UTF_8));
            String previous = null;
            while (iterator.isValid() && keys.size() < limit) {
                DecodedKey decoded = decode(iterator.key());
                if (!decoded.table().equals(table)) {
                    break;
                }
                if (!decoded.rowKey().equals(previous)) {
                    keys.add(decoded.rowKey());
                    previous = decoded.rowKey();
                }
                iterator.next();
            }
        }
        return keys;
    }


    private static boolean matchesHistoryFilter(DecodedKey decoded, Set<String> requested, long lowerBound, long upperBound) {
        return (requested.isEmpty() || requested.contains(decoded.selector()))
                && decoded.timestamp() >= lowerBound
                && decoded.timestamp() <= upperBound;
    }

    private void writeCell(String table, String rowKey, String family, String qualifier, byte[] value, long timestamp, boolean tombstone) {
        try {
            db.put(key(table, rowKey, family, qualifier, timestamp), value(tombstone, value));
        } catch (RocksDBException e) {
            throw new IllegalStateException("Unable to write cell", e);
        }
    }

    private long assignTimestamp(long requestedTimestamp) {
        if (requestedTimestamp > 0) {
            clock.updateAndGet(current -> Math.max(current, requestedTimestamp));
            return requestedTimestamp;
        }
        return clock.incrementAndGet();
    }

    private static byte[] key(String table, String rowKey, String family, String qualifier, long timestamp) {
        byte[] logical = (table + "\u0000" + rowKey + "\u0000" + family + "\u0000" + qualifier + "\u0000").getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(logical.length + Long.BYTES);
        buffer.put(logical);
        buffer.putLong(MAX_TS - timestamp);
        return buffer.array();
    }

    private static byte[] prefix(String table, String rowKey) {
        return (table + "\u0000" + rowKey + "\u0000").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] value(boolean tombstone, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + payload.length);
        buffer.put((byte) (tombstone ? 1 : 0));
        buffer.put(payload);
        return buffer.array();
    }

    private static Cell deserialize(DecodedKey key, byte[] value) {
        byte[] payload = new byte[Math.max(0, value.length - 1)];
        if (payload.length > 0) {
            System.arraycopy(value, 1, payload, 0, payload.length);
        }
        return new Cell(key.family(), key.qualifier(), payload, key.timestamp(), value.length > 0 && value[0] == 1);
    }

    private static DecodedKey decode(byte[] key) {
        int[] positions = separatorPositions(key);
        String table = text(key, 0, positions[0]);
        String row = text(key, positions[0] + 1, positions[1]);
        String family = text(key, positions[1] + 1, positions[2]);
        String qualifier = text(key, positions[2] + 1, positions[3]);
        long inverted = ByteBuffer.wrap(key, key.length - Long.BYTES, Long.BYTES).getLong();
        return new DecodedKey(table, row, family, qualifier, MAX_TS - inverted);
    }

    private static int[] separatorPositions(byte[] key) {
        int[] positions = new int[4];
        int found = 0;
        for (int i = 0; i < key.length - Long.BYTES && found < positions.length; i++) {
            if (key[i] == SEP) {
                positions[found++] = i;
            }
        }
        if (found < positions.length) {
            throw new IllegalArgumentException("Malformed storage key");
        }
        return positions;
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        if (value.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (value[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static String text(byte[] bytes, int start, int endExclusive) {
        return new String(bytes, start, endExclusive - start, StandardCharsets.UTF_8);
    }

    @Override
    public void destroy() {
        close();
    }

    @Override
    public void close() {
        db.close();
        options.close();
    }

    private record DecodedKey(String table, String rowKey, String family, String qualifier, long timestamp) {
        String selector() {
            return family + ":" + qualifier;
        }
    }
}
