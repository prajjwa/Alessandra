package com.alessandra.widecolumn.model;

public record ColumnMutation(String family, String qualifier, byte[] value) {
    public String selector() {
        return family + ":" + qualifier;
    }
}
