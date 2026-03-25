/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import org.mockito.ArgumentCaptor;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

/**
 * Property-based tests for {@link DirectMemoryAllocatorMetricsService}.
 *
 * <p>Feature: allocator-settings-metrics
 */
class MetricsServicePropertyTests {

    private static void resetSingleton() throws Exception {
        Field instanceField = DirectMemoryAllocatorMetricsService.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, null);
    }

    private static MetricsRegistry createMockRegistry(
        Histogram histogram, Counter totalAlloc, Counter stall, Counter gcHint,
        Counter cacheShrink, Counter cacheRestore, Counter backpressure
    ) {
        MetricsRegistry registry = mock(MetricsRegistry.class);
        when(registry.createHistogram(eq("allocator.memory.gauge"), any(), any())).thenReturn(histogram);
        when(registry.createCounter(eq("allocator.memory.total_allocations"), any(), any())).thenReturn(totalAlloc);
        when(registry.createCounter(eq("allocator.memory.stall_count"), any(), any())).thenReturn(stall);
        when(registry.createCounter(eq("allocator.memory.gc_hint_count"), any(), any())).thenReturn(gcHint);
        when(registry.createCounter(eq("allocator.memory.cache_shrink_count"), any(), any())).thenReturn(cacheShrink);
        when(registry.createCounter(eq("allocator.memory.cache_restore_count"), any(), any())).thenReturn(cacheRestore);
        when(registry.createCounter(eq("allocator.memory.backpressure_count"), any(), any())).thenReturn(backpressure);
        return registry;
    }

    // ======================== Property 5: Gauge recording completeness ========================

    /**
     * Property 5: Gauge recording completeness.
     *
     * <p>For any DiagnosticSnapshot and CacheController state, calling recordSnapshot
     * records exactly 6 gauge observations on the histogram.
     *
     * <p><b>Validates: Requirements 3.4, 7.3</b>
     */
    @Property(tries = 100)
    void recordSnapshotRecordsExactlySixGauges(
        @ForAll @DoubleRange(min = 0.0, max = 1.0) double pressureLevel,
        @ForAll @LongRange(min = 0, max = 1_000_000_000) long lastHeadroomBytes,
        @ForAll @LongRange(min = 0, max = 1_000_000_000) long allocatedByUs,
        @ForAll @DoubleRange(min = 0.0, max = 1_000_000.0) double gcDebtEma,
        @ForAll @IntRange(min = 0, max = 3) int actionOrdinal,
        @ForAll @LongRange(min = 0, max = 100_000) long cacheMaxBlocks
    ) throws Exception {
        resetSingleton();

        Histogram mockHistogram = mock(Histogram.class);
        MetricsRegistry registry = createMockRegistry(
            mockHistogram, mock(Counter.class), mock(Counter.class), mock(Counter.class),
            mock(Counter.class), mock(Counter.class), mock(Counter.class)
        );
        DirectMemoryAllocatorMetricsService.initialize(registry);

        LastAction action = LastAction.values()[actionOrdinal];
        CacheController mockCC = mock(CacheController.class);
        when(mockCC.currentMaxBlocks()).thenReturn(cacheMaxBlocks);

        DirectMemoryAllocator.DiagnosticSnapshot snap = DirectMemoryAllocatorMetricsServiceTests.createSnapshot(
            0L, 0L, 0L, 0L, 0L, 0L,
            pressureLevel, lastHeadroomBytes, allocatedByUs, gcDebtEma, action
        );

        DirectMemoryAllocatorMetricsService.getInstance().recordSnapshot(snap, mockCC);

        // Exactly 6 gauge observations
        verify(mockHistogram, times(6)).record(anyDouble(), any(Tags.class));
    }

    // ======================== Property 6: Counter delta correctness ========================

    /**
     * Property 6: Counter delta correctness.
     *
     * <p>For any two monotonically-increasing snapshots, the counter deltas recorded
     * equal (second - first) for each counter, and all deltas are non-negative.
     *
     * <p><b>Validates: Requirements 3.5, 7.1</b>
     */
    @Property(tries = 100)
    void counterDeltasAreCorrectForMonotonicSnapshots(
        @ForAll @LongRange(min = 0, max = 500_000) long totalAlloc1,
        @ForAll @LongRange(min = 0, max = 500_000) long stall1,
        @ForAll @LongRange(min = 0, max = 500_000) long gcHint1,
        @ForAll @LongRange(min = 0, max = 500_000) long cacheShrink1,
        @ForAll @LongRange(min = 0, max = 500_000) long cacheRestore1,
        @ForAll @LongRange(min = 0, max = 500_000) long bp1,
        @ForAll @LongRange(min = 0, max = 500_000) long deltaAlloc,
        @ForAll @LongRange(min = 0, max = 500_000) long deltaStall,
        @ForAll @LongRange(min = 0, max = 500_000) long deltaGcHint,
        @ForAll @LongRange(min = 0, max = 500_000) long deltaCacheShrink,
        @ForAll @LongRange(min = 0, max = 500_000) long deltaCacheRestore,
        @ForAll @LongRange(min = 0, max = 500_000) long deltaBp
    ) throws Exception {
        resetSingleton();

        Counter mockTotalAlloc = mock(Counter.class);
        Counter mockStall = mock(Counter.class);
        Counter mockGcHint = mock(Counter.class);
        Counter mockCacheShrink = mock(Counter.class);
        Counter mockCacheRestore = mock(Counter.class);
        Counter mockBp = mock(Counter.class);

        MetricsRegistry registry = createMockRegistry(
            mock(Histogram.class), mockTotalAlloc, mockStall, mockGcHint,
            mockCacheShrink, mockCacheRestore, mockBp
        );
        DirectMemoryAllocatorMetricsService.initialize(registry);

        DirectMemoryAllocator.DiagnosticSnapshot snap1 = DirectMemoryAllocatorMetricsServiceTests.createSnapshot(
            totalAlloc1, stall1, gcHint1, cacheShrink1, cacheRestore1, bp1,
            0.0, 0L, 0L, 0.0, LastAction.NONE
        );
        DirectMemoryAllocator.DiagnosticSnapshot snap2 = DirectMemoryAllocatorMetricsServiceTests.createSnapshot(
            totalAlloc1 + deltaAlloc, stall1 + deltaStall, gcHint1 + deltaGcHint,
            cacheShrink1 + deltaCacheShrink, cacheRestore1 + deltaCacheRestore, bp1 + deltaBp,
            0.0, 0L, 0L, 0.0, LastAction.NONE
        );

        DirectMemoryAllocatorMetricsService service = DirectMemoryAllocatorMetricsService.getInstance();

        // First call establishes baseline
        service.recordSnapshot(snap1, null);
        reset(mockTotalAlloc, mockStall, mockGcHint, mockCacheShrink, mockCacheRestore, mockBp);

        // Second call should produce deltas
        service.recordSnapshot(snap2, null);

        verifyCounterDelta(mockTotalAlloc, deltaAlloc);
        verifyCounterDelta(mockStall, deltaStall);
        verifyCounterDelta(mockGcHint, deltaGcHint);
        verifyCounterDelta(mockCacheShrink, deltaCacheShrink);
        verifyCounterDelta(mockCacheRestore, deltaCacheRestore);
        verifyCounterDelta(mockBp, deltaBp);
    }

    // ======================== Property 7: Identical snapshot idempotence ========================

    /**
     * Property 7: Identical snapshot idempotence.
     *
     * <p>Calling recordSnapshot twice with the same snapshot produces zero deltas
     * for all 6 counters on the second call.
     *
     * <p><b>Validates: Requirements 7.2</b>
     */
    @Property(tries = 100)
    void identicalSnapshotProducesZeroDeltasOnSecondCall(
        @ForAll @LongRange(min = 0, max = 1_000_000) long totalAlloc,
        @ForAll @LongRange(min = 0, max = 1_000_000) long stall,
        @ForAll @LongRange(min = 0, max = 1_000_000) long gcHint,
        @ForAll @LongRange(min = 0, max = 1_000_000) long cacheShrink,
        @ForAll @LongRange(min = 0, max = 1_000_000) long cacheRestore,
        @ForAll @LongRange(min = 0, max = 1_000_000) long bp
    ) throws Exception {
        resetSingleton();

        Counter mockTotalAlloc = mock(Counter.class);
        Counter mockStall = mock(Counter.class);
        Counter mockGcHint = mock(Counter.class);
        Counter mockCacheShrink = mock(Counter.class);
        Counter mockCacheRestore = mock(Counter.class);
        Counter mockBp = mock(Counter.class);

        MetricsRegistry registry = createMockRegistry(
            mock(Histogram.class), mockTotalAlloc, mockStall, mockGcHint,
            mockCacheShrink, mockCacheRestore, mockBp
        );
        DirectMemoryAllocatorMetricsService.initialize(registry);

        DirectMemoryAllocator.DiagnosticSnapshot snap = DirectMemoryAllocatorMetricsServiceTests.createSnapshot(
            totalAlloc, stall, gcHint, cacheShrink, cacheRestore, bp,
            0.5, 1000L, 500L, 0.1, LastAction.NONE
        );

        DirectMemoryAllocatorMetricsService service = DirectMemoryAllocatorMetricsService.getInstance();

        // First call
        service.recordSnapshot(snap, null);
        reset(mockTotalAlloc, mockStall, mockGcHint, mockCacheShrink, mockCacheRestore, mockBp);

        // Second call with identical snapshot — should produce zero deltas
        service.recordSnapshot(snap, null);

        // Zero deltas means add() should never be called
        verifyCounterDelta(mockTotalAlloc, 0);
        verifyCounterDelta(mockStall, 0);
        verifyCounterDelta(mockGcHint, 0);
        verifyCounterDelta(mockCacheShrink, 0);
        verifyCounterDelta(mockCacheRestore, 0);
        verifyCounterDelta(mockBp, 0);
    }

    // ======================== Helpers ========================

    private static void verifyCounterDelta(Counter counter, long expectedDelta) {
        if (expectedDelta > 0) {
            ArgumentCaptor<Double> captor = ArgumentCaptor.forClass(Double.class);
            verify(counter, times(1)).add(captor.capture());
            assertEquals((double) expectedDelta, captor.getValue(), 0.001,
                "Counter delta should be " + expectedDelta);
        } else {
            verify(counter, times(0)).add(anyDouble());
        }
    }
}
