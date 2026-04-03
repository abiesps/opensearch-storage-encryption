/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks prefetch effectiveness with latency measurement.
 * Thread-safe. Zero overhead when disabled (caller checks volatile boolean before calling).
 *
 * Metrics tracked:
 * <ul>
 *   <li><b>Effective prefetch</b>: block was prefetched, then read by a search thread</li>
 *   <li><b>Wasted prefetch</b>: block was prefetched but never read</li>
 *   <li><b>Prefetch-to-read latency</b>: nanos between prefetch() and the subsequent read()
 *       for the same block — tells you if async IO completed before the search thread needed it</li>
 *   <li><b>Latency histogram</b>: bucketed distribution of prefetch-to-read times</li>
 *   <li><b>Cold reads</b>: reads for blocks that were never prefetched (demand misses)</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class PrefetchEffectivenessTracker {
    private static final PrefetchEffectivenessTracker INSTANCE = new PrefetchEffectivenessTracker();

    // Key: blockOffset → Value: System.nanoTime() when prefetch was issued
    private final ConcurrentHashMap<Long, Long> prefetchedBlocks = new ConcurrentHashMap<>();

    // Counters
    private final LongAdder totalPrefetches = new LongAdder();
    private final LongAdder effectivePrefetches = new LongAdder();
    private final LongAdder totalReads = new LongAdder();
    private final LongAdder coldReads = new LongAdder(); // reads with no prior prefetch
    private final LongAdder totalLatencyNanos = new LongAdder();

    // Latency histogram buckets (prefetch-to-read time)
    private final LongAdder latencyUnder1ms = new LongAdder();   // < 1ms — prefetch arrived well ahead
    private final LongAdder latency1to5ms = new LongAdder();     // 1-5ms — tight but useful
    private final LongAdder latency5to20ms = new LongAdder();    // 5-20ms — marginal benefit
    private final LongAdder latency20to100ms = new LongAdder();  // 20-100ms — search thread likely stalled
    private final LongAdder latencyOver100ms = new LongAdder();  // > 100ms — prefetch too early, data may be evicted

    private PrefetchEffectivenessTracker() {}

    public static PrefetchEffectivenessTracker getInstance() {
        return INSTANCE;
    }

    /** Called when a block is prefetched. Records the timestamp. */
    public void recordPrefetch(long blockOffset) {
        totalPrefetches.increment();
        prefetchedBlocks.put(blockOffset, System.nanoTime());
    }

    /**
     * Called when a block is read by a search/searcher thread.
     * If the block was previously prefetched, records the prefetch-to-read latency.
     */
    public void recordRead(long blockOffset) {
        totalReads.increment();
        Long prefetchTime = prefetchedBlocks.remove(blockOffset);
        if (prefetchTime != null) {
            effectivePrefetches.increment();
            long latencyNanos = System.nanoTime() - prefetchTime;
            totalLatencyNanos.add(latencyNanos);
            bucketLatency(latencyNanos);
        } else {
            coldReads.increment();
        }
    }

    private void bucketLatency(long nanos) {
        long micros = nanos / 1000;
        if (micros < 1000) {          // < 1ms
            latencyUnder1ms.increment();
        } else if (micros < 5000) {   // 1-5ms
            latency1to5ms.increment();
        } else if (micros < 20000) {  // 5-20ms
            latency5to20ms.increment();
        } else if (micros < 100000) { // 20-100ms
            latency20to100ms.increment();
        } else {                       // > 100ms
            latencyOver100ms.increment();
        }
    }

    /** Get stats as formatted string. */
    public String stats() {
        long total = totalPrefetches.sum();
        long effective = effectivePrefetches.sum();
        long wasted = total - effective;
        long reads = totalReads.sum();
        long cold = coldReads.sum();
        double effectiveRate = total > 0 ? (effective * 100.0 / total) : 0;
        long avgLatencyUs = effective > 0 ? (totalLatencyNanos.sum() / effective / 1000) : 0;

        return String.format(
            "prefetch_tracking{"
                + "total_prefetches=%d, effective=%d, wasted=%d, effective_rate=%.1f%%, "
                + "total_reads=%d, cold_reads=%d, pending=%d, "
                + "avg_prefetch_to_read_latency_us=%d, "
                + "latency_histogram={<1ms=%d, 1-5ms=%d, 5-20ms=%d, 20-100ms=%d, >100ms=%d}"
                + "}",
            total, effective, wasted, effectiveRate,
            reads, cold, prefetchedBlocks.size(),
            avgLatencyUs,
            latencyUnder1ms.sum(), latency1to5ms.sum(), latency5to20ms.sum(),
            latency20to100ms.sum(), latencyOver100ms.sum()
        );
    }

    /** Reset all counters and pending map. */
    public void reset() {
        totalPrefetches.reset();
        effectivePrefetches.reset();
        totalReads.reset();
        coldReads.reset();
        totalLatencyNanos.reset();
        latencyUnder1ms.reset();
        latency1to5ms.reset();
        latency5to20ms.reset();
        latency20to100ms.reset();
        latencyOver100ms.reset();
        prefetchedBlocks.clear();
    }

    // Getters for programmatic access
    public long getTotalPrefetches() { return totalPrefetches.sum(); }
    public long getEffectivePrefetches() { return effectivePrefetches.sum(); }
    public long getWastedPrefetches() { return totalPrefetches.sum() - effectivePrefetches.sum(); }
    public long getTotalReads() { return totalReads.sum(); }
    public long getColdReads() { return coldReads.sum(); }
    public int getPendingCount() { return prefetchedBlocks.size(); }
    public long getAvgLatencyMicros() {
        long eff = effectivePrefetches.sum();
        return eff > 0 ? totalLatencyNanos.sum() / eff / 1000 : 0;
    }
}
