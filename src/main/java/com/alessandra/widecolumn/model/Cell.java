package com.alessandra.widecolumn.model;

import java.util.Arrays;

public record Cell(String family, String qualifier, byte[] value, long timestamp, boolean tombstone) {
    public Cell {
        value = value == null ? new byte[0] : Arrays.copyOf(value, value.length);
    }

    @Override
    public byte[] value() {
        return Arrays.copyOf(value, value.length);
    }

    public String selector() {
        return family + ":" + qualifier;
    }
}
