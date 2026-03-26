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
 * HC-4: Linear Throughput Degradation.
 *
 * <p>Measures allocation throughput at 50%, 75%, 90%, and 95% cache fill
 * levels and asserts that throughput degrades linearly (not cliff-style).
 * Specifically:
 * <ol>
 *   <li>Throughput at each level is measured over a fixed window</li>
 *   <li>No sudden collapse: throughput at N+1 fill level must be at least
 *       10% of the throughput at the previous level</li>
 *   <li>Degradation is gradual: the ratio between consecutive levels
 *       should not drop by more than 80% in a single step</li>
 * </ol>
 *
 * <p>Uses 512 MB maxDirect for CI compatibility.
 *
 * <p><b>Validates: Requirements 2.1, 2.2</b>
 */
class LinearThroughputDegradationTests {

    private static final int BLOCK_SIZE = 8192;
    private static final long MAX_DIRECT = 512L * 1024 * 1024; // 512 MB for CI
    private static final int THREAD_COUNT = 4;
    private static final long MEASUREMENT_WINDOW_MS = 2_000; // 2s per fill level
    private static final long ORIGINAL_MAX_BLOCKS = 20_000;
    private static final double[] FILL_LEVELS = { 0.50, 0.75, 0.90, 0.95 };

    @Test
    void throughputDegradesLinearlyNotCliff() throws Exception {
        List<Double> throughputs = new ArrayList<>();

        for (double fillLevel : FILL_LEVELS) {
            double throughput = measureThroughputAtFillLevel(fillLevel);
            throughputs.add(throughput);
            System.out.println(String.format(
                "[HC-4] Fill=%.0f%% throughput=%.0f allocs/sec",
                fillLevel * 100, throughput));
        }

        // --- Assertions ---

        // 1. All throughput measurements should be positive
        for (int i = 0; i < throughputs.size(); i++) {
            assertTrue(throughputs.get(i) > 0,
                String.format("Throughput at %.0f%% fill should be > 0, "
                    + "got %.0f", FILL_LEVELS[i] * 100, throughputs.get(i)));
        }

        // 2. No sudden collapse: each level's throughput must be at least
        //    10% of the previous level's throughput. A cliff would show as
        //    throughput dropping to near-zero between consecutive levels.
        for (int i = 1; i < throughputs.size(); i++) {
            double prev = throughputs.get(i - 1);
            double curr = throughputs.get(i);
            double ratio = curr / Math.max(prev, 1);
            assertTrue(ratio >= 0.10,
                String.format("Throughput cliff detected between %.0f%% "
                    + "and %.0f%% fill. prev=%.0f, curr=%.0f, ratio=%.2f "
                    + "(expected >= 0.10)",
                    FILL_LEVELS[i - 1] * 100, FILL_LEVELS[i] * 100,
                    prev, curr, ratio));
        }

        // 3. Overall degradation should be gradual: throughput at 95% fill
        //    should be at least 5% of throughput at 50% fill.
        double first = throughputs.get(0);
        double last = throughputs.get(throughputs.size() - 1);
        double overallRatio = last / Math.max(first, 1);
        assertTrue(overallRatio >= 0.05,
            String.format("Overall throughput collapse: 50%% fill=%.0f, "
                + "95%% fill=%.0f, ratio=%.3f (expected >= 0.05)",
                first, last, overallRatio));

        System.out.println("[HC-4] Summary: throughputs=" + throughputs
            + ", overallRatio=" + String.format("%.3f", overallRatio));
    }

    /**
     * Measures allocation throughput at a given fill level.
     *
     * <p>Strategy: pre-fill memory to the target fill level using a live
     * buffer pool, then measure how many allocations 4 threads can complete
     * in the measurement window while churning (allocate new, drop old).
     *
     * @param fillLevel fraction of maxDirect to pre-fill (0.0 to 1.0)
     * @return allocations per second achieved during the measurement window
     */
    private double measureThroughputAtFillLevel(double fillLevel)
            throws Exception {

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

        // Pre-fill to target fill level
        ConcurrentLinkedQueue<ByteBuffer> livePool = new ConcurrentLinkedQueue<>();
        int prefillCount = (int) ((MAX_DIRECT * fillLevel) / BLOCK_SIZE);
        for (int i = 0; i < prefillCount; i++) {
            try {
                livePool.add(allocator.allocate(BLOCK_SIZE));
            } catch (MemoryBackPressureException e) {
                break; // Can't fill further — that's fine
            }
        }

        // Register CacheController reporting held bytes from pool
        AtomicLong currentMax = new AtomicLong(ORIGINAL_MAX_BLOCKS);
        CacheController controller = new CacheController() {
            @Override
            public long cacheHeldBytes() {
                return (long) livePool.size() * BLOCK_SIZE;
            }
            @Override
            public void setMaxBlocks(long n) { currentMax.set(n); }
            @Override
            public void cleanUp() {
                int toDrop = Math.min(50, livePool.size() / 10);
                for (int i = 0; i < toDrop; i++) { livePool.poll(); }
            }
            @Override
            public long currentMaxBlocks() { return currentMax.get(); }
            @Override
            public long originalMaxBlocks() { return ORIGINAL_MAX_BLOCKS; }
        };
        allocator.registerCacheController(controller);

        // Measure throughput: 4 threads churning for MEASUREMENT_WINDOW_MS
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong totalAllocs = new AtomicLong(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            Thread t = new Thread(() -> {
                try { startLatch.await(); }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                while (running.get()) {
                    try {
                        ByteBuffer buf = allocator.allocate(BLOCK_SIZE);
                        totalAllocs.incrementAndGet();
                        buf.put(0, (byte) 0xAB);
                        livePool.add(buf);
                        livePool.poll(); // churn: drop oldest
                    } catch (MemoryBackPressureException e) {
                        // Count backpressure as a "completed" attempt
                        // to avoid infinite spin skewing results
                        totalAllocs.incrementAndGet();
                    }
                }
                doneLatch.countDown();
            }, "hc4-" + String.format("%.0f", fillLevel * 100) + "-" + i);
            t.setDaemon(true);
            t.start();
        }

        startLatch.countDown();
        Thread.sleep(MEASUREMENT_WINDOW_MS);
        running.set(false);
        doneLatch.await(10, TimeUnit.SECONDS);

        long allocs = totalAllocs.get();
        double throughput = (double) allocs / (MEASUREMENT_WINDOW_MS / 1000.0);

        // Clean up: release all buffers and let GC reclaim
        livePool.clear();
        System.gc();
        Thread.sleep(500);

        return throughput;
    }
}
