/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.memory;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * Singleton metrics service that bridges {@link DirectMemoryAllocator.DiagnosticSnapshot}
 * to OpenSearch's {@link MetricsRegistry}. Follows the {@code CryptoMetricsService} pattern.
 *
 * <p>Records 6 gauge values as {@link Histogram} observations (tagged by {@code stat_type})
 * and 6 monotonic counters using delta computation from successive snapshots.
 *
 * @see DirectMemoryAllocator.DiagnosticSnapshot
 * @see CacheController
 */
public class DirectMemoryAllocatorMetricsService {

    private static volatile DirectMemoryAllocatorMetricsService instance;

    // Metric names
    private static final String GAUGE_HISTOGRAM_NAME = "allocator.memory.gauge";
    private static final String TOTAL_ALLOCATIONS_NAME = "allocator.memory.total_allocations";
    private static final String STALL_COUNT_NAME = "allocator.memory.stall_count";
    private static final String GC_HINT_COUNT_NAME = "allocator.memory.gc_hint_count";
    private static final String CACHE_SHRINK_COUNT_NAME = "allocator.memory.cache_shrink_count";
    private static final String CACHE_RESTORE_COUNT_NAME = "allocator.memory.cache_restore_count";
    private static final String BACKPRESSURE_COUNT_NAME = "allocator.memory.backpressure_count";

    // Metric descriptions
    private static final String GAUGE_HISTOGRAM_DESC = "Allocator memory gauge metrics";
    private static final String TOTAL_ALLOCATIONS_DESC = "Total allocations performed";
    private static final String STALL_COUNT_DESC = "Total allocation stalls";
    private static final String GC_HINT_COUNT_DESC = "Total GC hints issued";
    private static final String CACHE_SHRINK_COUNT_DESC = "Total cache shrink operations";
    private static final String CACHE_RESTORE_COUNT_DESC = "Total cache restore operations";
    private static final String BACKPRESSURE_COUNT_DESC = "Total backpressure rejections";

    // Units
    private static final String COUNT_UNIT = "count";

    // Tag names
    private static final String STAT_TYPE_TAG = "stat_type";
    private static final String ACTION_TAG = "action";

    // Error message
    private static final String NOT_INITIALIZED_ERROR = "DirectMemoryAllocatorMetricsService not initialized.";

    // Histogram for gauge metrics
    private final Histogram gaugeHistogram;

    // Counters
    private final Counter totalAllocationsCounter;
    private final Counter stallCountCounter;
    private final Counter gcHintCountCounter;
    private final Counter cacheShrinkCountCounter;
    private final Counter cacheRestoreCountCounter;
    private final Counter backpressureCountCounter;

    // Previous snapshot values for delta computation
    private long prevTotalAllocations;
    private long prevStallCount;
    private long prevGcHintCount;
    private long prevCacheShrinkCount;
    private long prevCacheRestoreCount;
    private long prevBackpressureCount;

    /**
     * Private constructor for singleton pattern.
     *
     * @param metricsRegistry the metrics registry for collecting metrics
     */
    private DirectMemoryAllocatorMetricsService(MetricsRegistry metricsRegistry) {
        this.gaugeHistogram = createHistogram(metricsRegistry, GAUGE_HISTOGRAM_NAME, GAUGE_HISTOGRAM_DESC, COUNT_UNIT);
        this.totalAllocationsCounter = createCounter(metricsRegistry, TOTAL_ALLOCATIONS_NAME, TOTAL_ALLOCATIONS_DESC, COUNT_UNIT);
        this.stallCountCounter = createCounter(metricsRegistry, STALL_COUNT_NAME, STALL_COUNT_DESC, COUNT_UNIT);
        this.gcHintCountCounter = createCounter(metricsRegistry, GC_HINT_COUNT_NAME, GC_HINT_COUNT_DESC, COUNT_UNIT);
        this.cacheShrinkCountCounter = createCounter(metricsRegistry, CACHE_SHRINK_COUNT_NAME, CACHE_SHRINK_COUNT_DESC, COUNT_UNIT);
        this.cacheRestoreCountCounter = createCounter(metricsRegistry, CACHE_RESTORE_COUNT_NAME, CACHE_RESTORE_COUNT_DESC, COUNT_UNIT);
        this.backpressureCountCounter = createCounter(metricsRegistry, BACKPRESSURE_COUNT_NAME, BACKPRESSURE_COUNT_DESC, COUNT_UNIT);
    }

    /**
     * Initializes the singleton instance.
     *
     * @param metricsRegistry the metrics registry for collecting metrics
     */
    public static synchronized void initialize(MetricsRegistry metricsRegistry) {
        if (instance == null) {
            instance = new DirectMemoryAllocatorMetricsService(metricsRegistry);
        }
    }

    /**
     * Gets the singleton instance.
     *
     * @return the DirectMemoryAllocatorMetricsService instance
     * @throws IllegalStateException if not initialized
     */
    public static DirectMemoryAllocatorMetricsService getInstance() {
        if (instance == null) {
            throw new IllegalStateException(NOT_INITIALIZED_ERROR);
        }
        return instance;
    }

    /**
     * Records gauge observations and counter deltas from a diagnostic snapshot.
     *
     * <p>Synchronized to ensure atomic delta computation across all 6 counters.
     *
     * @param snap            the diagnostic snapshot to record
     * @param cacheController the cache controller (may be null)
     */
    public synchronized void recordSnapshot(DirectMemoryAllocator.DiagnosticSnapshot snap, CacheController cacheController) {
        // --- Record 6 gauge observations ---
        recordGauges(snap, cacheController);

        // --- Compute and record 6 counter deltas ---
        recordCounterDeltas(snap);
    }

    private void recordGauges(DirectMemoryAllocator.DiagnosticSnapshot snap, CacheController cacheController) {
        if (gaugeHistogram == null) {
            return;
        }

        gaugeHistogram.record(snap.pressureLevel(), Tags.create().addTag(STAT_TYPE_TAG, "pressure_level"));
        gaugeHistogram.record(snap.lastHeadroomBytes(), Tags.create().addTag(STAT_TYPE_TAG, "last_headroom_bytes"));
        gaugeHistogram.record(snap.allocatedByUs(), Tags.create().addTag(STAT_TYPE_TAG, "allocated_by_us"));
        gaugeHistogram.record(snap.gcDebtEma(), Tags.create().addTag(STAT_TYPE_TAG, "gc_debt_ema"));
        gaugeHistogram.record(
            snap.lastAction().ordinal(),
            Tags.create().addTag(STAT_TYPE_TAG, "last_action").addTag(ACTION_TAG, snap.lastAction().name().toLowerCase())
        );

        long cacheMaxBlocks = (cacheController != null) ? cacheController.currentMaxBlocks() : 0;
        gaugeHistogram.record(cacheMaxBlocks, Tags.create().addTag(STAT_TYPE_TAG, "cache_max_capacity_blocks"));
    }

    private void recordCounterDeltas(DirectMemoryAllocator.DiagnosticSnapshot snap) {
        addDelta(totalAllocationsCounter, snap.totalAllocations(), prevTotalAllocations);
        addDelta(stallCountCounter, snap.stallCount(), prevStallCount);
        addDelta(gcHintCountCounter, snap.gcHintCount(), prevGcHintCount);
        addDelta(cacheShrinkCountCounter, snap.cacheShrinkCount(), prevCacheShrinkCount);
        addDelta(cacheRestoreCountCounter, snap.cacheRestoreCount(), prevCacheRestoreCount);
        addDelta(backpressureCountCounter, snap.backpressureCount(), prevBackpressureCount);

        // Update prev values
        prevTotalAllocations = snap.totalAllocations();
        prevStallCount = snap.stallCount();
        prevGcHintCount = snap.gcHintCount();
        prevCacheShrinkCount = snap.cacheShrinkCount();
        prevCacheRestoreCount = snap.cacheRestoreCount();
        prevBackpressureCount = snap.backpressureCount();
    }

    private static void addDelta(Counter counter, long current, long previous) {
        if (counter == null) {
            return;
        }
        long delta = current - previous;
        if (delta > 0) {
            counter.add(delta);
        }
    }

    // Private helper methods
    private static Counter createCounter(MetricsRegistry registry, String name, String description, String unit) {
        return registry != null ? registry.createCounter(name, description, unit) : null;
    }

    private static Histogram createHistogram(MetricsRegistry registry, String name, String description, String unit) {
        return registry != null ? registry.createHistogram(name, description, unit) : null;
    }
}
