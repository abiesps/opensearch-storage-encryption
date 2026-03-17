/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.lang.foreign.MemorySegment;

/**
 * A no-op implementation of {@link SparseLongBlockTable} for benchmarking.
 *
 * <p>{@code get()} always returns null (forcing fallback to L2 Caffeine cache),
 * and {@code put()} is a no-op. This allows measuring the overhead of the L1
 * block table by comparing benchmarks with and without it.
 */
public class NoOpSparseLongBlockTable extends SparseLongBlockTable {

    public static final NoOpSparseLongBlockTable INSTANCE = new NoOpSparseLongBlockTable();

    private NoOpSparseLongBlockTable() {
        super(0);
    }

    @Override
    public MemorySegment get(long blockId) {
        return null;
    }

    @Override
    public void put(long blockId, MemorySegment segment) {
        // no-op
    }

    @Override
    public MemorySegment remove(long blockId) {
        return null;
    }

    @Override
    public void clear() {
        // no-op
    }
}
