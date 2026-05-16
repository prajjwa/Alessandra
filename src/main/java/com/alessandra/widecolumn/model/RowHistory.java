package com.alessandra.widecolumn.model;

import java.util.List;

public record RowHistory(String table, String rowKey, List<Cell> versions) {
    public boolean found() {
        return !versions.isEmpty();
    }
}
