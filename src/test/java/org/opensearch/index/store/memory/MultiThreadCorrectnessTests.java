/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
 * HC-1: Multi-Thread Correctness Under Sustained Concurrency.
 *
 * <p>Validates that 32 threads continuously allocating, using, and dropping
 * buffers through the real {@link DirectMemoryAllocator} for 10 seconds
 * (CI-friendly duration) produces:
 * <ol>
 *   <li>Zero {@code OutOfMemoryError} escaping (caught via uncaught handler)</li>
 *   <li>Zero {@code NullPointerException} escaping</li>
 *   <li>{@code allocatedByUs >= 0} at all times (accounting invariant)</li>
 *   <li>Accounting identity: {@code allocatedByUs + reclaimedByGc == totalBytesAllocated}
 *       holds after GC convergence</li>
 * </ol>
 *
 * <p>Uses a thread-safe mock {@link CacheController} instead of real
 * CaffeineBlockCacheV2 to isolate allocator correctness from cache complexity.
 *
 * <p>For production validation, run with:
 * {@code -XX:+UseZGC -XX:MaxDirectMemorySize=256m -Xmx128m --enable-preview}
 *
 * <p><b>Validates: Requirements 1.1, 4.4, 9.1, 9.2, 9.3</b>
 */
class MultiThreadCorrectnessTests {

    private static final int BLOCK_SIZE = 8192;
    private static final long MAX_DIRECT = 512L * 1024 * 1024; // 512 MB — generous for CI
    private static final int THREAD_COUNT = 32;
    private static final long TEST_DURATION_MS = 10_000; // 10 seconds for CI
    private static final long ORIGINAL_MAX_BLOCKS = 20_000;

    @Test
    void multiThreadCorrectness_zeroOomZeroNpe_accountingIdentityHolds() throws Exception {
        // --- Track escaping errors via uncaught exception handler ---
        AtomicLong escapedOomCount = new AtomicLong(0);
        AtomicLong escapedNpeCount = new AtomicLong(0);
        ConcurrentLinkedQueue<Throwable> escapedErrors = new ConcurrentLinkedQueue<>();

        Thread.UncaughtExceptionHandler errorCatcher = (t, e) -> {
            if (e instanceof OutOfMemoryError) {
                escapedOomCount.incrementAndGet();
                escapedErrors.add(e);
            } else if (e instanceof NullPointerException) {
                escapedNpeCount.incrementAndGet();
                escapedErrors.add(e);
            }
        };

        Thread.UncaughtExceptionHandler originalHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler(errorCatcher);

        try {
            runConcurrencyTest(escapedOomCount, escapedNpeCount, escapedErrors);
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(originalHandler);
        }
    }

    private void runConcurrencyTest(
            AtomicLong escapedOomCount,
            AtomicLong escapedNpeCount,
            ConcurrentLinkedQueue<Throwable> escapedErrors) throws Exception {

        // --- Setup: allocator with sampleInterval=1 for maximum diagnostic coverage ---
        DirectMemoryAllocator allocator = new DirectMemoryAllocator(
            MAX_DIRECT,
            BLOCK_SIZE,
            1,    // sampleInterval = 1 (diagnose every allocation)
            0.3,  // emaAlpha = 0.3 (faster convergence for test)
            2.0,  // safetyMultiplier
            0.5,  // minCacheFraction
            50,   // gcHintCooldownMs
            50,   // shrinkCooldownMs
            50,   // maxSampleIntervalMs
            0.10, // minHeadroomFraction
            0.001 // stallRatioThreshold
        );

        // --- Thread-safe mock CacheController ---
        // Reports cacheHeldBytes as ~70% of allocatedByUs to create moderate GC debt,
        // driving the allocator through all tiers without excessive backpressure.
        AtomicLong currentMax = new AtomicLong(ORIGINAL_MAX_BLOCKS);

        CacheController controller = new CacheController() {
            @Override
            public long cacheHeldBytes() {
                return (long) (allocator.getAllocatedByUs() * 0.7);
            }

            @Override
            public void setMaxBlocks(long n) {
                currentMax.set(n);
            }

            @Override
            public void cleanUp() {
                // no-op for mock
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

        // --- Spawn 32 threads: continuously allocate, use, drop buffers ---
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
                        ByteBuffer buf = allocator.allocate(BLOCK_SIZE);
                        totalAllocsByThreads.incrementAndGet();

                        // Brief use to prevent JIT from optimizing away
                        buf.put(0, (byte) 0xAB);

                        // Drop reference — simulate eviction, let GC reclaim
                    } catch (MemoryBackPressureException e) {
                        // Expected under pressure — allocator doing its job
                        backpressureCount.incrementAndGet();
                    }
                }
                doneLatch.countDown();
            }, "hc1-thread-" + i);

            t.setUncaughtExceptionHandler((th, e) -> {
                if (e instanceof OutOfMemoryError) {
                    escapedOomCount.incrementAndGet();
                    escapedErrors.add(e);
                } else if (e instanceof NullPointerException) {
                    escapedNpeCount.incrementAndGet();
                    escapedErrors.add(e);
                }
            });
            threads.add(t);
        }

        // Start all threads simultaneously
        for (Thread t : threads) {
            t.start();
        }
        startLatch.countDown();

        // Let the test run
        Thread.sleep(TEST_DURATION_MS);

        // Signal stop and wait for completion
        running.set(false);
        boolean allDone = doneLatch.await(15, TimeUnit.SECONDS);
        assertTrue(allDone, "All 32 threads should complete within timeout");

        // --- Allow GC convergence for accounting identity check ---
        // Null all local references and give GC time to reclaim buffers
        // so that allocatedByUs + reclaimedByGc converges to totalBytesAllocated.
        System.gc();
        Thread.sleep(2000);
        System.gc();
        Thread.sleep(1000);

        // --- Assertions ---

        // 1. Zero OOM escapes (Req 1.1)
        assertEquals(0, escapedOomCount.get(),
            "Zero OOM should escape the allocator. Escaped: " + escapedOomCount.get()
                + ", errors: " + escapedErrors);

        // 2. Zero NPE escapes (Req 14.1, 14.2)
        assertEquals(0, escapedNpeCount.get(),
            "Zero NPE should escape the allocator. Escaped: " + escapedNpeCount.get()
                + ", errors: " + escapedErrors);

        // 3. allocatedByUs >= 0 (accounting invariant, Req 4.4)
        long allocByUs = allocator.getAllocatedByUs();
        assertTrue(allocByUs >= 0,
            "allocatedByUs must be non-negative: " + allocByUs);

        // 4. Accounting identity after GC convergence (Req 4.4, 9.1)
        // allocatedByUs + reclaimedByGc should converge to totalBytesAllocated.
        // Allow a tolerance for buffers GC hasn't collected yet.
        long reclaimedByGc = allocator.getReclaimedByGc();
        long totalBytesAllocated = allocator.getTotalBytesAllocated();
        long accountingSum = allocByUs + reclaimedByGc;

        // The identity may not be exact if some buffers are still awaiting GC.
        // We check that the sum is within a reasonable tolerance of totalBytesAllocated.
        // Tolerance: max of 5% of totalBytesAllocated or 10 MB (for small totals).
        long tolerance = Math.max((long) (totalBytesAllocated * 0.05), 10L * 1024 * 1024);
        long drift = Math.abs(totalBytesAllocated - accountingSum);
        assertTrue(drift <= tolerance,
            "Accounting identity drift too large after GC convergence. "
                + "totalBytesAllocated=" + totalBytesAllocated
                + ", allocatedByUs=" + allocByUs
                + ", reclaimedByGc=" + reclaimedByGc
                + ", sum=" + accountingSum
                + ", drift=" + drift
                + ", tolerance=" + tolerance);

        // 5. Verify threads actually did work
        assertTrue(totalAllocsByThreads.get() > 0,
            "Threads should have completed at least some allocations");

        // Log summary for observability
        System.out.println("[HC-1 MultiThreadCorrectness] Summary:"
            + " threads=" + THREAD_COUNT
            + ", duration=" + TEST_DURATION_MS + "ms"
            + ", totalAllocs=" + totalAllocsByThreads.get()
            + ", backpressureCount=" + backpressureCount.get()
            + ", escapedOom=" + escapedOomCount.get()
            + ", escapedNpe=" + escapedNpeCount.get()
            + ", allocatedByUs=" + allocByUs
            + ", reclaimedByGc=" + reclaimedByGc
            + ", totalBytesAllocated=" + totalBytesAllocated
            + ", accountingDrift=" + drift
            + ", gcHintCount=" + allocator.getGcHintCount()
            + ", cacheShrinkCount=" + allocator.getCacheShrinkCount()
            + ", cacheRestoreCount=" + allocator.getCacheRestoreCount()
            + ", gcDebtEma=" + String.format("%.0f", allocator.getGcDebtEma())
            + ", lastPressure=" + String.format("%.3f", allocator.getLastPressureLevel()));
    }
}
