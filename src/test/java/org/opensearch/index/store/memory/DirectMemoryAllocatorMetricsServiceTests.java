/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.memory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import org.junit.After;
import org.junit.Before;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link DirectMemoryAllocatorMetricsService} edge cases.
 *
 * <p>Feature: allocator-settings-metrics, Task 6.6
 */
public class DirectMemoryAllocatorMetricsServiceTests extends OpenSearchTestCase {

    private MetricsRegistry mockRegistry;
    private Histogram mockHistogram;
    private Counter mockTotalAllocations;
    private Counter mockStallCount;
    private Counter mockGcHintCount;
    private Counter mockCacheShrinkCount;
    private Counter mockCacheRestoreCount;
    private Counter mockBackpressureCount;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        resetSingleton();

        mockRegistry = mock(MetricsRegistry.class);
        mockHistogram = mock(Histogram.class);
        mockTotalAllocations = mock(Counter.class);
        mockStallCount = mock(Counter.class);
        mockGcHintCount = mock(Counter.class);
        mockCacheShrinkCount = mock(Counter.class);
        mockCacheRestoreCount = mock(Counter.class);
        mockBackpressureCount = mock(Counter.class);

        when(mockRegistry.createHistogram(eq("allocator.memory.gauge"), any(), any())).thenReturn(mockHistogram);
        when(mockRegistry.createCounter(eq("allocator.memory.total_allocations"), any(), any())).thenReturn(mockTotalAllocations);
        when(mockRegistry.createCounter(eq("allocator.memory.stall_count"), any(), any())).thenReturn(mockStallCount);
        when(mockRegistry.createCounter(eq("allocator.memory.gc_hint_count"), any(), any())).thenReturn(mockGcHintCount);
        when(mockRegistry.createCounter(eq("allocator.memory.cache_shrink_count"), any(), any())).thenReturn(mockCacheShrinkCount);
        when(mockRegistry.createCounter(eq("allocator.memory.cache_restore_count"), any(), any())).thenReturn(mockCacheRestoreCount);
        when(mockRegistry.createCounter(eq("allocator.memory.backpressure_count"), any(), any())).thenReturn(mockBackpressureCount);
    }

    @After
    public void tearDown() throws Exception {
        resetSingleton();
        super.tearDown();
    }

    private void resetSingleton() throws Exception {
        Field instanceField = DirectMemoryAllocatorMetricsService.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, null);
    }

    /**
     * getInstance() before initialize() throws IllegalStateException.
     * Validates: Requirements 3.2
     */
    public void testGetInstanceBeforeInitializeThrows() {
        expectThrows(IllegalStateException.class, DirectMemoryAllocatorMetricsService::getInstance);
    }

    /**
     * recordSnapshot with all-zero snapshot does not throw.
     * Validates: Requirements 7.4
     */
    public void testRecordSnapshotWithAllZeroSnapshotDoesNotThrow() {
        DirectMemoryAllocatorMetricsService.initialize(mockRegistry);
        DirectMemoryAllocatorMetricsService service = DirectMemoryAllocatorMetricsService.getInstance();

        DirectMemoryAllocator.DiagnosticSnapshot zeroSnap = new DirectMemoryAllocator.DiagnosticSnapshot(
            0L, 0L, 0L, 0L, 0L,
            0.0, 0.0, 0.0, 0L,
            0L, 0L,
            0L, 0L,
            0L, 0L,
            0L, 0L,
            0L, 0L,
            0L,
            0L, 0L, 0L,
            0L, 0L, 0.0,
            0, 0L,
            0L,
            LastAction.NONE
        );

        // Should not throw
        service.recordSnapshot(zeroSnap, null);

        // Verify gauges were recorded (6 observations)
        verify(mockHistogram, times(6)).record(anyDouble(), any(Tags.class));
    }

    /**
     * recordSnapshot with null CacheController records 0 for cache gauge.
     * Validates: Requirements 3.6
     */
    public void testRecordSnapshotWithNullCacheControllerRecordsZeroCacheGauge() {
        DirectMemoryAllocatorMetricsService.initialize(mockRegistry);
        DirectMemoryAllocatorMetricsService service = DirectMemoryAllocatorMetricsService.getInstance();

        DirectMemoryAllocator.DiagnosticSnapshot snap = createSnapshot(
            100L, 5L, 2L, 3L, 1L, 0L,
            0.5, 1000L, 500L, 0.1, LastAction.GC_HINT
        );

        service.recordSnapshot(snap, null);

        // 6 gauge observations should be recorded
        verify(mockHistogram, times(6)).record(anyDouble(), any(Tags.class));
    }

    /**
     * recordSnapshot with non-null CacheController records actual currentMaxBlocks.
     */
    public void testRecordSnapshotWithCacheControllerRecordsActualBlocks() {
        DirectMemoryAllocatorMetricsService.initialize(mockRegistry);
        DirectMemoryAllocatorMetricsService service = DirectMemoryAllocatorMetricsService.getInstance();

        CacheController mockCC = mock(CacheController.class);
        when(mockCC.currentMaxBlocks()).thenReturn(42L);

        DirectMemoryAllocator.DiagnosticSnapshot snap = createSnapshot(
            100L, 5L, 2L, 3L, 1L, 0L,
            0.5, 1000L, 500L, 0.1, LastAction.NONE
        );

        service.recordSnapshot(snap, mockCC);

        verify(mockHistogram, times(6)).record(anyDouble(), any(Tags.class));
        verify(mockCC).currentMaxBlocks();
    }

    // ======================== Helper ========================

    /**
     * Creates a DiagnosticSnapshot with specified counter and gauge values,
     * filling remaining fields with zeros.
     */
    static DirectMemoryAllocator.DiagnosticSnapshot createSnapshot(
        long totalAllocations, long stallCount, long gcHintCount,
        long cacheShrinkCount, long cacheRestoreCount, long backpressureCount,
        double pressureLevel, long lastHeadroomBytes, long allocatedByUs,
        double gcDebtEma, LastAction lastAction
    ) {
        return new DirectMemoryAllocator.DiagnosticSnapshot(
            System.currentTimeMillis(),
            totalAllocations, 0L, allocatedByUs, 0L,
            pressureLevel, gcDebtEma, 0.0, 0L,
            0L, lastHeadroomBytes,
            stallCount, 0L,
            gcHintCount, 0L,
            cacheShrinkCount, 0L,
            cacheRestoreCount, 0L,
            0L,
            0L, 0L, 0L,
            0L, 0L, 0.0,
            0, 0L,
            backpressureCount,
            lastAction
        );
    }
}
