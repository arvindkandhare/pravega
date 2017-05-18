package io.pravega.segmentstore.server.containers;

import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.ReadIndex;

import java.util.concurrent.CompletableFuture;

/**
 * Created by kandha on 5/17/17.
 */
public class StreamSegment extends CompletableFuture<Void> {
    private final OperationLog durableLog;
    private final ReadIndex readIndex;

    public StreamSegment(OperationLog durableLog, ReadIndex readIndex) {
        this.durableLog = durableLog;
        this.readIndex = readIndex;
    }

    public CompletableFuture<Void> create() {
        return null;
    }
}
