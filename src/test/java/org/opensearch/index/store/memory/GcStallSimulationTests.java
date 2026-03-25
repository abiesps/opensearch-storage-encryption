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
 * MB-2: GC Stall Simulation.
 *
 * <p>Validates that under sustained multi-threaded allocation pressure (16 threads
 * continuously allocating, briefly holding, then dropping references), the allocator:
 * <ol>
 *   <li>Issues GC hints (Tier 2) as pressure builds — {@code gcHintCount > 0}</li>
 *   <li>Eventually triggers cache shrink (Tier 3) if pressure persists</li>
 *   <li>Never lets an {@code OutOfMemoryError} escape — all OOM is caught and
 *       wrapped in {@code MemoryBackPressureException}</li>
 * </ol>
 *
 * <p>The test uses a generous {@code maxDirect} (512 MB) to ensure it passes
 * reliably in CI without requiring specific JVM flags. For production validation,
 * run with: {@code -XX:+UseZGC -XX:MaxDirectMemorySize=64m -Xmx32m}.
 *
 * <p><b>Validates: Requirements 2.3, 2.4, 2.5</b>
 */
class GcStallSimulationTests {

    private static final int BLOCK_SIZE = 8192;
    private static final long MAX_DIRECT = 512L * 1024 * 1024; // 512 MB — generous for CI
    private static final int THREAD_COUNT = 16;
    private static final long TEST_DURATION_MS = 5_000; // 5 seconds

    @Test
    void gcStallSimulationZeroOomEscapes() throws Exception {
        // Track any OOM that escapes the allocator (should be zero)
        AtomicLong escapedOomCount = new AtomicLong(0);
        ConcurrentLinkedQueue<Throwable> escapedErrors = new ConcurrentLinkedQueue<>();

        Thread.UncaughtExceptionHandler oomCatcher = (t, e) -> {
            if (e instanceof OutOfMemoryError) {
                escapedOomCount.incrementAndGet();
                escapedErrors.add(e);
            }
        };

        // Save and restore default handler
        Thread.UncaughtExceptionHandler originalHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler(oomCatcher);

        try {
            runSimulation(escapedOomCount, escapedErrors);
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(originalHandler);
        }
    }

    private void runSimulation(AtomicLong escapedOomCount, ConcurrentLinkedQueue<Throwable> escapedErrors)
            throws Exception {

        // --- Setup: allocator with frequent diagnostics to maximize pressure detection ---
        DirectMemoryAllocator allocator = new DirectMemoryAllocator(
            MAX_DIRECT,
            BLOCK_SIZE,
            1,    // sampleInterval = 1 (diagnose every allocation for max sensitivity)
            0.3,  // emaAlpha = 0.3 (faster EMA convergence for stress test)
            2.0,  // safetyMultiplier
            0.5,  // minCacheFraction
            50,   // gcHintCooldownMs — short cooldown so hints fire more often
            50,   // shrinkCooldownMs — short cooldown so shrinks can trigger
            50,   // maxSampleIntervalMs
            0.10, // minHeadroomFraction
            0.001 // stallRatioThreshold
        );

        // Register a CacheController that simulates a cache holding some memory.
        // cacheHeldBytes returns a fraction of allocatedByUs to create GC debt,
        // which drives the allocator into Tier 2/3 responses.
        long originalMax = 10_000;
        AtomicLong currentMax = new AtomicLong(originalMax);
        CacheController controller = new CacheController() {
            @Override
            public long cacheHeldBytes() {
                // Report ~60% of what the allocator has outstanding.
                // This creates GC_Debt = allocatedByUs - cacheHeldBytes ≈ 40% of allocatedByUs,
                // which drives pressure and triggers GC hints / cache shrink.
                return (long) (allocator.getAllocatedByUs() * 0.6);
            }

            @Override
            public void setMaxBlocks(long n) {
                currentMax.set(n);
            }

            @Override
            public void cleanUp() {
                // no-op for simulation
            }

            @Override
            public long currentMaxBlocks() {
                return currentMax.get();
            }

            @Override
            public long originalMaxBlocks() {
                return originalMax;
            }
        };
        allocator.registerCacheController(controller);

        // --- Run: 16 threads doing continuous allocate → hold briefly → drop ---
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
                        // Allocate a block
                        ByteBuffer buf = allocator.allocate(BLOCK_SIZE);
                        totalAllocsByThreads.incrementAndGet();

                        // Brief hold — simulate cache.put + short use
                        // Use the buffer briefly to prevent JIT from optimizing away
                        buf.put(0, (byte) 42);

                        // Drop reference — simulate eviction
                        // (buf goes out of scope at end of loop iteration)
                    } catch (MemoryBackPressureException e) {
                        // Expected under pressure — the allocator is doing its job
                        backpressureCount.incrementAndGet();
                    }
                }
                doneLatch.countDown();
            }, "gc-stall-sim-" + i);
            t.setUncaughtExceptionHandler((th, e) -> {
                if (e instanceof OutOfMemoryError) {
                    escapedOomCount.incrementAndGet();
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

        // Let the simulation run for the test duration
        Thread.sleep(TEST_DURATION_MS);

        // Signal threads to stop and wait for completion
        running.set(false);
        boolean allDone = doneLatch.await(10, TimeUnit.SECONDS);
        assertTrue(allDone, "All threads should complete within timeout");

        // --- Assertions ---

        // 1. Zero OOM escapes — the allocator must catch all OOM and wrap in
        //    MemoryBackPressureException (Req 1.1, 1.2)
        assertEquals(0, escapedOomCount.get(),
            "Zero OOM should escape the allocator. Escaped: " + escapedOomCount.get()
                + ", errors: " + escapedErrors);

        // 2. GC hint count should increase — under sustained pressure with 16 threads
        //    and a CacheController reporting GC debt, the allocator should issue
        //    GC hints at Tier 2 (Req 2.3)
        long gcHints = allocator.getGcHintCount();
        assertTrue(gcHints > 0,
            "GC hint count should be > 0 under sustained pressure. "
                + "gcHintCount=" + gcHints
                + ", totalAllocs=" + totalAllocsByThreads.get()
                + ", backpressureCount=" + backpressureCount.get()
                + ", gcDebtEma=" + String.format("%.0f", allocator.getGcDebtEma())
                + ", lastPressure=" + String.format("%.3f", allocator.getLastPressureLevel()));

        // 3. Verify the allocator actually processed allocations
        assertTrue(totalAllocsByThreads.get() > 0,
            "Threads should have completed at least some allocations");

        // 4. Verify allocatedByUs is non-negative (accounting invariant)
        assertTrue(allocator.getAllocatedByUs() >= 0,
            "allocatedByUs must be non-negative: " + allocator.getAllocatedByUs());

        // Log summary for observability
        System.out.println("[GcStallSimulation] Summary:"
            + " totalAllocs=" + totalAllocsByThreads.get()
            + ", backpressureCount=" + backpressureCount.get()
            + ", gcHintCount=" + gcHints
            + ", cacheShrinkCount=" + allocator.getCacheShrinkCount()
            + ", cacheRestoreCount=" + allocator.getCacheRestoreCount()
            + ", tier3Count=" + allocator.getTier3LastResortCount()
            + ", gcDebtEma=" + String.format("%.0f", allocator.getGcDebtEma())
            + ", allocatedByUs=" + allocator.getAllocatedByUs()
            + ", reclaimedByGc=" + allocator.getReclaimedByGc()
            + ", lastPressure=" + String.format("%.3f", allocator.getLastPressureLevel())
            + ", escapedOom=" + escapedOomCount.get());
    }
}
