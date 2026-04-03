/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks prefetch effectiveness: how many prefetched blocks are actually read.
 * Thread-safe. Zero overhead when disabled (caller checks before calling).
 *
 * An "effective" prefetch is one where a block was prefetched and then subsequently read.
 * A "wasted" prefetch is one where a block was prefetched but never read.
 *
 * @opensearch.internal
 */
public final class PrefetchEffectivenessTracker {
    private static final PrefetchEffectivenessTracker INSTANCE = new PrefetchEffectivenessTracker();

    // Set of block offsets that have been prefetched but not yet read
    // Key: blockOffset, Value: sentinel (just tracking presence)
    private final ConcurrentHashMap<Long, Boolean> prefetchedBlocks = new ConcurrentHashMap<>();

    private final LongAdder totalPrefetches = new LongAdder();
    private final LongAdder effectivePrefetches = new LongAdder();
    private final LongAdder reads = new LongAdder();

    private PrefetchEffectivenessTracker() {}

    public static PrefetchEffectivenessTracker getInstance() {
        return INSTANCE;
    }

    /** Called when a block is prefetched. */
    public void recordPrefetch(long blockOffset) {
        totalPrefetches.increment();
        prefetchedBlocks.put(blockOffset, Boolean.TRUE);
    }

    /** Called when a block is read. Returns true if this block was previously prefetched. */
    public boolean recordRead(long blockOffset) {
        reads.increment();
        if (prefetchedBlocks.remove(blockOffset) != null) {
            effectivePrefetches.increment();
            return true;
        }
        return false;
    }

    /** Get stats as formatted string. */
    public String stats() {
        long total = totalPrefetches.sum();
        long effective = effectivePrefetches.sum();
        long wasted = total - effective;
        long totalReads = reads.sum();
        double effectiveRate = total > 0 ? (effective * 100.0 / total) : 0;
        return String.format(
            "prefetch_tracking{total_prefetches=%d, effective=%d, wasted=%d, effective_rate=%.1f%%, total_reads=%d, pending=%d}",
            total,
            effective,
            wasted,
            effectiveRate,
            totalReads,
            prefetchedBlocks.size()
        );
    }

    /** Reset all counters. */
    public void reset() {
        totalPrefetches.reset();
        effectivePrefetches.reset();
        reads.reset();
        prefetchedBlocks.clear();
    }

    public long getTotalPrefetches() {
        return totalPrefetches.sum();
    }

    public long getEffectivePrefetches() {
        return effectivePrefetches.sum();
    }

    public long getWastedPrefetches() {
        return totalPrefetches.sum() - effectivePrefetches.sum();
    }

    public long getTotalReads() {
        return reads.sum();
    }

    public int getPendingCount() {
        return prefetchedBlocks.size();
    }
}
