/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.lang.foreign.MemorySegment;

/**
 * A no-op implementation of {@link RadixBlockTable} for benchmarking.
 *
 * <p>{@code get()} always returns null (forcing fallback to L2 Caffeine cache),
 * and {@code put()} is a no-op. This allows measuring the overhead of the L1
 * block table by comparing benchmarks with and without it.
 */
public class NoOpRadixBlockTable extends RadixBlockTable<MemorySegment> {

    public static final NoOpRadixBlockTable INSTANCE = new NoOpRadixBlockTable();

    private NoOpRadixBlockTable() {
        super(1);
    }

    @Override
    public MemorySegment get(long blockId) {
        return null;
    }

    @Override
    public void put(long blockId, MemorySegment value) {
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
