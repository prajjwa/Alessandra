package com.alessandra.widecolumn.cluster;

public class QuorumUnavailableException extends RuntimeException {
    public QuorumUnavailableException(String message) {
        super(message);
    }
}
