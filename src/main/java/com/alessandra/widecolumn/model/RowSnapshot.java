package com.alessandra.widecolumn.model;

import java.util.List;

public record RowSnapshot(String table, String rowKey, List<Cell> cells) {
    public boolean found() {
        return !cells.isEmpty();
    }
}
