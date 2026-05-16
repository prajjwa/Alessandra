package com.alessandra.widecolumn.store;

import com.alessandra.widecolumn.model.ColumnMutation;
import com.alessandra.widecolumn.model.RowSnapshot;

import java.util.Collection;
import java.util.List;

public interface WideColumnStore extends AutoCloseable {
    long put(String table, String rowKey, Collection<ColumnMutation> columns, long timestamp);

    long delete(String table, String rowKey, Collection<String> selectors, long timestamp);

    RowSnapshot get(String table, String rowKey, Collection<String> selectors, long readTimestamp);

    List<String> scanRowKeys(String table, String startRow, int limit);

    @Override
    void close();
}
