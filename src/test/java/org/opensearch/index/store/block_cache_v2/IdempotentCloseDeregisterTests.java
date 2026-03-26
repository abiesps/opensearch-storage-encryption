/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/**
 * HC-5: Idempotent Close and Deregister.
 *
 * <p>Validates the full lifecycle of CacheController registration:
 * register → deregister → continue allocating → re-register.
 * Asserts:
 * <ol>
 *   <li>No {@code NullPointerException} after deregister</li>
 *   <li>No stale reference errors</li>
 *   <li>Diagnostic skips cache-dependent actions when controller is null</li>
 *   <li>Re-registration works correctly after deregister</li>
 * </ol>
 *
 * <p><b>Validates: Requirements 14.1, 14.2, 14.3</b>
 */
class IdempotentCloseDeregisterTests {

    private static final int BLOCK_SIZE = 8192;
    private static final long MAX_DIRECT = 512L * 1024 * 1024; // 512 MB for CI
    private static final long ORIGINAL_MAX_BLOCKS = 10_000;

    @Test
    void registerDeregisterContinueAllocateReregister_noNpeNoStaleRef()
            throws Exception {

        DirectMemoryAllocator allocator = new DirectMemoryAllocator(
            MAX_DIRECT,
            BLOCK_SIZE,
            1,    // sampleInterval = 1 (diagnose every allocation)
            0.3,  // emaAlpha
            2.0,  // safetyMultiplier
            0.5,  // minCacheFraction
            50,   // gcHintCooldownMs
            50,   // shrinkCooldownMs
            50,   // maxSampleIntervalMs
            0.10, // minHeadroomFraction
            0.001 // stallRatioThreshold
        );

        // --- Phase 1: Register CacheController and allocate ---
        AtomicLong currentMax = new AtomicLong(ORIGINAL_MAX_BLOCKS);
        AtomicLong shrinkCount = new AtomicLong(0);

        CacheController controller1 = createController(
            allocator, currentMax, shrinkCount, ORIGINAL_MAX_BLOCKS);
        allocator.registerCacheController(controller1);

        // Allocate several buffers to trigger diagnostics
        for (int i = 0; i < 100; i++) {
            ByteBuffer buf = allocator.allocate(BLOCK_SIZE);
            buf.put(0, (byte) 0x01);
        }

        long shrinksBefore = shrinkCount.get();
        System.out.println("[HC-5] Phase 1 (registered): allocated 100 buffers"
            + ", shrinks=" + shrinksBefore
            + ", gcDebtEma=" + String.format("%.0f",
                allocator.getGcDebtEma()));

        // --- Phase 2: Deregister and continue allocating ---
        allocator.deregisterCacheController();

        // Allocate after deregister — must NOT throw NPE.
        // diagnoseAndRespond() should skip cache-dependent actions.
        assertDoesNotThrow(() -> {
            for (int i = 0; i < 200; i++) {
                try {
                    ByteBuffer buf = allocator.allocate(BLOCK_SIZE);
                    buf.put(0, (byte) 0x02);
                } catch (MemoryBackPressureException e) {
                    // Backpressure is acceptable — NPE is not
                }
            }
        }, "Allocating after deregisterCacheController() must not throw NPE");

        // Verify no cache shrinks happened while deregistered
        // (diagnostic should skip cache-dependent actions when null)
        long shrinksAfterDeregister = shrinkCount.get();
        assertTrue(shrinksAfterDeregister == shrinksBefore,
            "No cache shrinks should occur while CacheController is null. "
                + "shrinksBefore=" + shrinksBefore
                + ", shrinksAfter=" + shrinksAfterDeregister);

        System.out.println("[HC-5] Phase 2 (deregistered): allocated 200 "
            + "buffers with no NPE, shrinks still=" + shrinksAfterDeregister);

        // --- Phase 3: Re-register a NEW controller and allocate ---
        AtomicLong currentMax2 = new AtomicLong(ORIGINAL_MAX_BLOCKS);
        AtomicLong shrinkCount2 = new AtomicLong(0);

        CacheController controller2 = createController(
            allocator, currentMax2, shrinkCount2, ORIGINAL_MAX_BLOCKS);
        allocator.registerCacheController(controller2);

        // Allocate after re-register — should work normally
        assertDoesNotThrow(() -> {
            for (int i = 0; i < 100; i++) {
                try {
                    ByteBuffer buf = allocator.allocate(BLOCK_SIZE);
                    buf.put(0, (byte) 0x03);
                } catch (MemoryBackPressureException e) {
                    // Backpressure is acceptable
                }
            }
        }, "Allocating after re-register must not throw");

        System.out.println("[HC-5] Phase 3 (re-registered): allocated 100 "
            + "buffers, shrinks=" + shrinkCount2.get()
            + ", gcDebtEma=" + String.format("%.0f",
                allocator.getGcDebtEma()));

        // --- Phase 4: Double deregister (idempotent) ---
        assertDoesNotThrow(() -> {
            allocator.deregisterCacheController();
            allocator.deregisterCacheController(); // second call is no-op
        }, "Double deregister must not throw");

        // Allocate after double deregister
        assertDoesNotThrow(() -> {
            for (int i = 0; i < 50; i++) {
                try {
                    ByteBuffer buf = allocator.allocate(BLOCK_SIZE);
                    buf.put(0, (byte) 0x04);
                } catch (MemoryBackPressureException e) {
                    // Backpressure is acceptable
                }
            }
        }, "Allocating after double deregister must not throw NPE");

        // Verify allocatedByUs is non-negative
        assertTrue(allocator.getAllocatedByUs() >= 0,
            "allocatedByUs must be non-negative: "
                + allocator.getAllocatedByUs());

        System.out.println("[HC-5] Phase 4 (double deregister): "
            + "no NPE, allocatedByUs=" + allocator.getAllocatedByUs());
    }

    @Test
    void diagnosticSkipsCacheActionsWhenControllerNull() throws Exception {
        DirectMemoryAllocator allocator = new DirectMemoryAllocator(
            MAX_DIRECT,
            BLOCK_SIZE,
            1,    // sampleInterval = 1
            0.3,  // emaAlpha
            2.0,  // safetyMultiplier
            0.5,  // minCacheFraction
            50,   // gcHintCooldownMs
            50,   // shrinkCooldownMs
            50,   // maxSampleIntervalMs
            0.10, // minHeadroomFraction
            0.001 // stallRatioThreshold
        );

        // Never register a controller — allocator starts with null
        // Allocate enough to trigger multiple diagnostics
        int allocCount = 0;
        for (int i = 0; i < 500; i++) {
            try {
                ByteBuffer buf = allocator.allocate(BLOCK_SIZE);
                buf.put(0, (byte) 0xFF);
                allocCount++;
            } catch (MemoryBackPressureException e) {
                // Acceptable under pressure
            }
        }

        // With no CacheController, cache shrink and restore should never fire
        assertTrue(allocator.getCacheShrinkCount() == 0,
            "No cache shrinks should occur without a CacheController. "
                + "cacheShrinkCount=" + allocator.getCacheShrinkCount());
        assertTrue(allocator.getCacheRestoreCount() == 0,
            "No cache restores should occur without a CacheController. "
                + "cacheRestoreCount=" + allocator.getCacheRestoreCount());
        assertTrue(allocCount > 0,
            "Should have completed at least some allocations");

        System.out.println("[HC-5] Null controller test: allocated "
            + allocCount + " buffers, cacheShrink=0, cacheRestore=0");
    }

    /**
     * Creates a thread-safe mock CacheController for testing.
     */
    private CacheController createController(
            DirectMemoryAllocator allocator,
            AtomicLong currentMax,
            AtomicLong shrinkCount,
            long originalMax) {
        return new CacheController() {
            @Override
            public long cacheHeldBytes() {
                return (long) (allocator.getAllocatedByUs() * 0.7);
            }

            @Override
            public void setMaxBlocks(long n) {
                long prev = currentMax.getAndSet(n);
                if (n < prev) {
                    shrinkCount.incrementAndGet();
                }
            }

            @Override
            public void cleanUp() { /* no-op for mock */ }

            @Override
            public long currentMaxBlocks() {
                return currentMax.get();
            }

            @Override
            public long originalMaxBlocks() {
                return originalMax;
            }
        };
    }
}
