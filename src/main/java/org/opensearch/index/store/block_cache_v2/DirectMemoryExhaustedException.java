/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

/**
 * Thrown when direct memory utilization exceeds the hard threshold and GC
 * does not reclaim sufficient memory within the configured timeout.
 */
public class DirectMemoryExhaustedException extends RuntimeException {

    private final double utilization;
    private final long outstandingBytes;
    private final long maxDirectMemory;

    public DirectMemoryExhaustedException(String message, double utilization, long outstandingBytes, long maxDirectMemory) {
        super(message);
        this.utilization = utilization;
        this.outstandingBytes = outstandingBytes;
        this.maxDirectMemory = maxDirectMemory;
    }

    public double utilization() {
        return utilization;
    }

    public long outstandingBytes() {
        return outstandingBytes;
    }

    public long maxDirectMemory() {
        return maxDirectMemory;
    }
}
