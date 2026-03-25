/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

/**
 * Property-based tests for backpressure count tracking on {@link DirectMemoryAllocator}.
 *
 * <p>Feature: allocator-settings-metrics
 */
class BackpressureCountPropertyTests {

    // ======================== Property 8 ========================

    /**
     * Property 8: Backpressure count tracks rejections.
     *
     * <p>Configure allocator with tiny {@code maxDirectMemoryBytes} to force
     * backpressure on every allocation attempt. Attempt N allocations, count
     * caught {@link MemoryBackPressureException} instances, and assert equals
     * {@code getBackpressureCount()}.
     *
     * <p>Strategy: Use {@code sampleInterval=1} so the first allocation triggers
     * a diagnostic (setting {@code lastDiagnosticNanos > 0}), enabling the hard
     * floor fast-fail path. With a very small {@code maxDirectMemoryBytes}, the
     * hard floor or OOM path will reject every allocation after the first diagnostic.
     *
     * <p><b>Validates: Requirements 6.1</b>
     */
    @Property(tries = 100)
    void backpressureCountTracksRejections(
        @ForAll @IntRange(min = 1, max = 20) int attemptCount
    ) {
        // Tiny maxDirect to force backpressure. Use Integer.MAX_VALUE-style
        // allocation sizes to reliably trigger OOM from allocateDirect().
        // But first we need lastDiagnosticNanos > 0 for hard floor to work.
        // Strategy: use a maxDirect that's just barely enough for the first
        // small allocation to succeed (triggering diagnostic), then all
        // subsequent allocations hit the hard floor.
        int blockSize = 4096;
        // maxDirect large enough for one small allocation + diagnostic overhead,
        // but small enough that subsequent allocations hit hard floor.
        // The hard floor checks: lastHeadroomBytes < size + hardFloorMargin
        // After diagnostic, lastHeadroomBytes will be sampled from MXBean.
        // We use a very large requested size (Integer.MAX_VALUE) to guarantee
        // the OOM path fires even if hard floor doesn't.
        long maxDirect = 1_000_000_000L; // 1 GB — generous for first alloc
        DirectMemoryAllocator allocator = new DirectMemoryAllocator(
            maxDirect, blockSize,
            /* sampleInterval */ 1,
            DirectMemoryAllocator.DEFAULT_EMA_ALPHA,
            DirectMemoryAllocator.DEFAULT_SAFETY_MULTIPLIER,
            DirectMemoryAllocator.DEFAULT_MIN_CACHE_FRACTION,
            DirectMemoryAllocator.DEFAULT_GC_HINT_COOLDOWN_MS,
            DirectMemoryAllocator.DEFAULT_SHRINK_COOLDOWN_MS,
            DirectMemoryAllocator.DEFAULT_MAX_SAMPLE_INTERVAL_MS,
            DirectMemoryAllocator.DEFAULT_MIN_HEADROOM_FRACTION,
            DirectMemoryAllocator.DEFAULT_STALL_RATIO_THRESHOLD
        );

        long caughtExceptions = 0;
        for (int i = 0; i < attemptCount; i++) {
            try {
                // Request Integer.MAX_VALUE bytes — always triggers OOM from allocateDirect()
                allocator.allocate(Integer.MAX_VALUE - 1);
            } catch (MemoryBackPressureException e) {
                caughtExceptions++;
            }
        }

        assertEquals(caughtExceptions, allocator.getBackpressureCount(),
            "backpressureCount must equal the number of caught MemoryBackPressureException instances");
    }

    // ======================== Property 9 ========================

    /**
     * Property 9: Reset clears backpressure count.
     *
     * <p>Set up allocator with non-zero {@code backpressureCount} by forcing
     * allocation failures, then call {@code resetMetrics()} and assert
     * {@code getBackpressureCount() == 0}.
     *
     * <p><b>Validates: Requirements 6.4</b>
     */
    @Property(tries = 100)
    void resetClearsBackpressureCount(
        @ForAll @IntRange(min = 1, max = 10) int failureCount
    ) {
        long maxDirect = 1_000_000_000L;
        int blockSize = 4096;
        DirectMemoryAllocator allocator = new DirectMemoryAllocator(
            maxDirect, blockSize,
            /* sampleInterval */ 1,
            DirectMemoryAllocator.DEFAULT_EMA_ALPHA,
            DirectMemoryAllocator.DEFAULT_SAFETY_MULTIPLIER,
            DirectMemoryAllocator.DEFAULT_MIN_CACHE_FRACTION,
            DirectMemoryAllocator.DEFAULT_GC_HINT_COOLDOWN_MS,
            DirectMemoryAllocator.DEFAULT_SHRINK_COOLDOWN_MS,
            DirectMemoryAllocator.DEFAULT_MAX_SAMPLE_INTERVAL_MS,
            DirectMemoryAllocator.DEFAULT_MIN_HEADROOM_FRACTION,
            DirectMemoryAllocator.DEFAULT_STALL_RATIO_THRESHOLD
        );

        // Force backpressure by requesting huge allocations
        for (int i = 0; i < failureCount; i++) {
            try {
                allocator.allocate(Integer.MAX_VALUE - 1);
            } catch (MemoryBackPressureException e) {
                // Expected
            }
        }

        // Verify non-zero before reset
        assertEquals(failureCount, allocator.getBackpressureCount(),
            "backpressureCount must be non-zero before reset");

        // Reset and verify
        allocator.resetMetrics();

        assertEquals(0, allocator.getBackpressureCount(),
            "backpressureCount must be 0 after resetMetrics()");
    }
}
