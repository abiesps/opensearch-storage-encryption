/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

/**
 * A no-op RadixBlockTable for benchmarking. get() always returns null
 * (forcing fallback to L2 Caffeine cache), put() and remove() are no-ops.
 */
public final class NoOpRadixBlockTable<V> extends RadixBlockTable<V> {

    @SuppressWarnings("rawtypes")
    private static final NoOpRadixBlockTable INSTANCE_RAW = new NoOpRadixBlockTable<>();

    @SuppressWarnings("unchecked")
    public static <V> NoOpRadixBlockTable<V> instance() {
        return (NoOpRadixBlockTable<V>) INSTANCE_RAW;
    }

    private NoOpRadixBlockTable() {
        super(1);
    }

    @Override
    public V get(long blockId) {
        return null;
    }

    @Override
    public void put(long blockId, V value) {
        // no-op
    }

    @Override
    public V remove(long blockId) {
        return null;
    }

    @Override
    public void clear() {
        // no-op
    }
}
