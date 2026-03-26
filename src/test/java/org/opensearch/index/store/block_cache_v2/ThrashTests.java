/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/**
 * MB-3: Thrash Test (0% hit rate).
 *
 * <p>Simulates a worst-case scenario where every cache access is a miss —
 * the key space is 10× the cache size, so every allocation evicts an existing
 * entry. Under sustained 0% hit rate with 4 threads, the allocator should:
 * <ol>
 *   <li>Shrink the cache to its floor ({@code originalMaxBlocks × minCacheFraction = 500})</li>
 *   <li>Fire {@code MemoryBackPressureException} when memory is exhausted</li>
 *   <li>Not oscillate — cache should not ping-pong between shrink and restore</li>
 *   <li>Keep {@code nativeUsed ≤ maxDirect} at all times</li>
 * </ol>
 *
 * <p>Strategy: We hold a pool of live buffers to keep {@code nativeUsed} high
 * and create genuine memory pressure. Each thread allocates a new buffer and
 * drops an old one (simulating 0% hit rate churn). The CacheController reports
 * {@code cacheHeldBytes} based on the live pool size, creating realistic GC debt
 * as evicted buffers await GC collection.
 *
 * <p><b>Validates: Requirements 6.3, 17.2</b>
 */
class ThrashTests {

    private static final int BLOCK_SIZE = 8192;
    private static final long MAX_DIRECT = 512L * 1024 * 1024; // 512 MB — generous for CI
    private static final int ORIGINAL_MAX_BLOCKS = 1000;
    private static final int THREAD_COUNT = 4;
    private static final long TEST_DURATION_MS = 10_000; // 10 seconds for CI

    @Test
    void thrashZeroHitRateCacheHitsFloorAndBackpressureFires() throws Exception {
        // --- Setup: allocator with frequent diagnostics and short cooldowns ---
        DirectMemoryAllocator allocator = new DirectMemoryAllocator(
            MAX_DIRECT,
            BLOCK_SIZE,
            1,    // sampleInterval = 1 (diagnose every allocation for max sensitivity)
            0.3,  // emaAlpha = 0.3 (faster EMA convergence for thrash test)
            2.0,  // safetyMultiplier
            0.5,  // minCacheFraction (floor = 1000 × 0.5 = 500)
            50,   // gcHintCooldownMs — short for stress test
            50,   // shrinkCooldownMs — short so shrinks can trigger quickly
            50,   // maxSampleIntervalMs
            0.10, // minHeadroomFraction
            0.001 // stallRatioThreshold
        );

        // Shared pool of live buffers — simulates the "cache" holding memory.
        // Threads churn through this pool: allocate new, drop old (0% hit rate).
        ConcurrentLinkedQueue<ByteBuffer> livePool = new ConcurrentLinkedQueue<>();

        // Pre-fill the pool to create baseline memory pressure.
        // Fill ~70% of maxDirect to ensure the allocator sees real pressure.
        int prefillCount = (int) ((MAX_DIRECT * 0.70) / BLOCK_SIZE);
        for (int i = 0; i < prefillCount; i++) {
            livePool.add(allocator.allocate(BLOCK_SIZE));
        }

        // CacheController: reports cacheHeldBytes based on live pool size.
        // As the pool churns, evicted buffers create GC debt.
        AtomicLong currentMax = new AtomicLong(ORIGINAL_MAX_BLOCKS);
        AtomicLong shrinkCallCount = new AtomicLong(0);
        AtomicLong restoreCallCount = new AtomicLong(0);

        CacheController controller = new CacheController() {
            @Override
            public long cacheHeldBytes() {
                // Report held bytes as a fraction of what the pool actually holds.
                // Under 0% hit rate, the "cache" is constantly evicting, so held
                // bytes are lower than allocatedByUs — creating GC debt.
                int poolSize = livePool.size();
                return (long) poolSize * BLOCK_SIZE / 2; // ~50% of pool is "held"
            }

            @Override
            public void setMaxBlocks(long n) {
                long prev = currentMax.getAndSet(n);
                if (n < prev) {
                    shrinkCallCount.incrementAndGet();
                } else if (n > prev) {
                    restoreCallCount.incrementAndGet();
                }
            }

            @Override
            public void cleanUp() {
                // Simulate eviction: drop some buffers from the pool to free memory
                int toDrop = Math.min(100, livePool.size() / 10);
                for (int i = 0; i < toDrop; i++) {
                    livePool.poll();
                }
            }

            @Override
            public long currentMaxBlocks() {
                return currentMax.get();
            }

            @Override
            public long originalMaxBlocks() {
                return ORIGINAL_MAX_BLOCKS;
            }
        };
        allocator.registerCacheController(controller);

        // --- Run: 4 threads continuously allocate, use, drop (0% hit rate) ---
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong backpressureCount = new AtomicLong(0);
        AtomicLong totalAllocsByThreads = new AtomicLong(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);

        List<Thread> threads = new ArrayList<>(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            Thread t = new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                while (running.get()) {
                    try {
                        // Allocate a new block — cache miss
                        ByteBuffer buf = allocator.allocate(BLOCK_SIZE);
                        totalAllocsByThreads.incrementAndGet();

                        // Brief use
                        buf.put(0, (byte) 0xFF);

                        // Add to pool and evict an old one (0% hit rate churn)
                        livePool.add(buf);
                        livePool.poll(); // drop oldest — simulates eviction
                    } catch (MemoryBackPressureException e) {
                        // Expected under sustained thrashing
                        backpressureCount.incrementAndGet();
                        // Brief pause to avoid tight spin on backpressure
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
                doneLatch.countDown();
            }, "thrash-" + i);
            threads.add(t);
        }

        // Start all threads simultaneously
        for (Thread t : threads) {
            t.start();
        }
        startLatch.countDown();

        // Let the thrash run for the test duration
        Thread.sleep(TEST_DURATION_MS);

        // Signal threads to stop and wait for completion
        running.set(false);
        boolean allDone = doneLatch.await(10, TimeUnit.SECONDS);
        assertTrue(allDone, "All threads should complete within timeout");

        // --- Assertions ---

        long finalCurrentMax = currentMax.get();
        long floor = (long) (ORIGINAL_MAX_BLOCKS * allocator.getMinCacheFraction());
        long shrinks = allocator.getCacheShrinkCount();
        long restores = allocator.getCacheRestoreCount();

        // 1. Cache should have reached the floor (originalMaxBlocks × minCacheFraction = 500)
        //    Under sustained 0% hit rate with high memory pressure, the allocator
        //    should shrink to the minimum. (Req 6.3, 17.2)
        assertTrue(finalCurrentMax <= floor,
            "Cache should reach floor under sustained thrashing. "
                + "currentMaxBlocks=" + finalCurrentMax
                + ", floor=" + floor
                + ", cacheShrinkCount=" + shrinks
                + ", gcDebtEma=" + String.format("%.0f", allocator.getGcDebtEma())
                + ", lastPressure=" + String.format("%.3f", allocator.getLastPressureLevel())
                + ", lastHeadroom=" + allocator.getLastHeadroomBytes()
                + ", targetHeadroom=" + allocator.getTargetHeadroomBytes());

        // 2. MemoryBackPressureException should have fired at least once.
        //    Under sustained 0% hit rate with high memory pressure, the pre-check
        //    or hard floor should eventually reject allocations.
        assertTrue(backpressureCount.get() > 0,
            "MemoryBackPressureException should fire under sustained thrashing. "
                + "backpressureCount=" + backpressureCount.get()
                + ", totalAllocs=" + totalAllocsByThreads.get()
                + ", cacheShrinkCount=" + shrinks
                + ", lastPressure=" + String.format("%.3f", allocator.getLastPressureLevel()));

        // 3. No excessive oscillation: the cache should not ping-pong between
        //    shrink and restore. Under sustained 0% hit rate, restores should be
        //    rare relative to shrinks.
        if (shrinks > 0) {
            double oscillationRatio = (double) restores / shrinks;
            assertTrue(oscillationRatio < 0.5,
                "Cache should not oscillate (shrink/restore ping-pong). "
                    + "shrinkCount=" + shrinks
                    + ", restoreCount=" + restores
                    + ", oscillationRatio=" + String.format("%.2f", oscillationRatio)
                    + " (expected < 0.5)");
        }

        // 4. nativeUsed should stay within maxDirect bounds.
        long lastNativeUsed = allocator.getLastNativeUsedBytes();
        assertTrue(lastNativeUsed <= MAX_DIRECT,
            "nativeUsed must stay within maxDirect bounds. "
                + "lastNativeUsed=" + lastNativeUsed
                + ", maxDirect=" + MAX_DIRECT);

        // 5. Verify allocatedByUs is non-negative (accounting invariant)
        assertTrue(allocator.getAllocatedByUs() >= 0,
            "allocatedByUs must be non-negative: " + allocator.getAllocatedByUs());

        // Log summary for observability
        System.out.println("[ThrashTest] Summary:"
            + " totalAllocs=" + totalAllocsByThreads.get()
            + ", backpressureCount=" + backpressureCount.get()
            + ", cacheShrinkCount=" + shrinks
            + ", cacheRestoreCount=" + restores
            + ", finalCurrentMax=" + finalCurrentMax
            + ", floor=" + floor
            + ", gcHintCount=" + allocator.getGcHintCount()
            + ", tier3Count=" + allocator.getTier3LastResortCount()
            + ", gcDebtEma=" + String.format("%.0f", allocator.getGcDebtEma())
            + ", allocatedByUs=" + allocator.getAllocatedByUs()
            + ", reclaimedByGc=" + allocator.getReclaimedByGc()
            + ", lastPressure=" + String.format("%.3f", allocator.getLastPressureLevel())
            + ", lastNativeUsed=" + lastNativeUsed
            + ", lastHeadroom=" + allocator.getLastHeadroomBytes()
            + ", targetHeadroom=" + allocator.getTargetHeadroomBytes());
    }
}
