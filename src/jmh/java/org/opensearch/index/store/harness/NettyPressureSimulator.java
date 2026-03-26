/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.harness;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Simulates external direct memory pressure from Netty or other NIO consumers
 * by holding a fixed amount of direct {@link ByteBuffer}s. This exercises
 * {@code DirectMemoryAllocator}'s external usage detection and ensures
 * gc_assisted doesn't over-shrink the cache when the pressure source is external.
 */
public final class NettyPressureSimulator implements AutoCloseable {

    private static final int CHUNK_SIZE = 4 * 1024 * 1024; // 4 MB chunks
    private final List<ByteBuffer> held = new ArrayList<>();
    private final long totalBytes;

    /**
     * @param megabytes amount of direct memory to hold (simulating Netty)
     */
    public NettyPressureSimulator(int megabytes) {
        this.totalBytes = (long) megabytes * 1024 * 1024;
        long remaining = totalBytes;
        while (remaining > 0) {
            int size = (int) Math.min(CHUNK_SIZE, remaining);
            held.add(ByteBuffer.allocateDirect(size));
            remaining -= size;
        }
        System.out.println("[NettyPressure] Holding " + megabytes + " MB direct memory (" + held.size() + " chunks)");
    }

    public long heldBytes() {
        return totalBytes;
    }

    @Override
    public void close() {
        held.clear(); // drop refs, let GC reclaim
        System.out.println("[NettyPressure] Released all chunks");
    }
}
