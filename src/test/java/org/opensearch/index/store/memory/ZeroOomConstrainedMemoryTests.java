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
 * HC-3: Zero OOM Under Constrained Direct Memory.
 *
 * <p>Validates that 8 threads continuously allocating under constrained direct
 * memory for 10 seconds (CI-friendly) produces:
 * <ol>
 *   <li>Zero {@code OutOfMemoryError} escaping via uncaught handler</li>
 *   <li>{@code MemoryBackPressureException} count &gt; 0 (backpressure fires)</li>
 *   <li>{@code allocatedByUs} converges toward 0 after threads stop and GC runs</li>
 * </ol>
 *
 * <p>Uses 512 MB maxDirect for CI compatibility (production: 32 MB with
 * {@code -XX:MaxDirectMemorySize=32m}). The constrained feel is achieved by
 * pre-filling ~85% of maxDirect with live buffers, leaving only ~15% headroom
 * for the 8 threads to fight over.
 *
 * <p><b>Validates: Requirements 1.1, 1.2, 1.5</b>
 */
class ZeroOomConstrainedMemoryTests {

    private static final int BLOCK_SIZE = 8192;
    private static final long MAX_DIRECT = 512L * 1024 * 1024; // 512 MB for CI
    private static final int THREAD_COUNT = 8;
    private static final long TEST_DURATION_MS = 10_000; // 10 seconds for CI
    private static final long ORIGINAL_MAX_BLOCKS = 10_000;

    @Test
    void zeroOomUnderConstrainedMemory_backpressureFires_allocatedByUsConverges() throws Exception {
        // --- Track escaping OOM via uncaught exception handler ---
        AtomicLong escapedOomCount = new AtomicLong(0);
        ConcurrentLinkedQueue<Throwable> escapedErrors = new ConcurrentLinkedQueue<>();

        Thread.UncaughtExceptionHandler oomCatcher = (t, e) -> {
            if (e instanceof OutOfMemoryError) {
                escapedOomCount.incrementAndGet();
                escapedErrors.add(e);
            }
        };

        Thread.UncaughtExceptionHandler originalHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler(oomCatcher);

        try {
            runConstrainedTest(escapedOomCount, escapedErrors);
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(originalHandler);
        }
    }

    private void runConstrainedTest(
            AtomicLong escapedOomCount,
            ConcurrentLinkedQueue<Throwable> escapedErrors) throws Exception {

        // --- Setup: allocator with frequent diagnostics ---
        DirectMemoryAllocator allocator = new DirectMemoryAllocator(
            MAX_DIRECT,
            BLOCK_SIZE,
            4096, // sampleInterval = 4096 during prefill (avoid pre-check triggers)
            0.3,  // emaAlpha = 0.3 (fast convergence)
            2.0,  // safetyMultiplier
            0.5,  // minCacheFraction
            50,   // gcHintCooldownMs
            50,   // shrinkCooldownMs
            50,   // maxSampleIntervalMs
            0.10, // minHeadroomFraction
            0.001 // stallRatioThreshold
        );

        // Pre-fill pool to ~80% of maxDirect to create constrained conditions.
        // Catch MemoryBackPressureException during prefill — if the allocator
        // starts rejecting, we've filled enough.
        ConcurrentLinkedQueue<ByteBuffer> livePool = new ConcurrentLinkedQueue<>();
        int prefillCount = (int) ((MAX_DIRECT * 0.80) / BLOCK_SIZE);
        for (int i = 0; i < prefillCount; i++) {
            try {
                livePool.add(allocator.allocate(BLOCK_SIZE));
            } catch (MemoryBackPressureException e) {
                break; // Filled enough — allocator is already under pressure
            }
        }

        // CacheController: reports cacheHeldBytes as ~40% of allocatedByUs
        // to create significant GC debt under constrained memory
        AtomicLong currentMax = new AtomicLong(ORIGINAL_MAX_BLOCKS);

        CacheController controller = new CacheController() {
            @Override
            public long cacheHeldBytes() {
                return (long) (allocator.getAllocatedByUs() * 0.4);
            }

            @Override
            public void setMaxBlocks(long n) {
                currentMax.set(n);
            }

            @Override
            public void cleanUp() {
                // Simulate eviction: drop some buffers to free memory
                int toDrop = Math.min(50, livePool.size() / 20);
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

        // --- Spawn 8 threads: continuously allocate under constrained memory ---
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
                        buf.put(0, (byte) 0xCD);
                        // Add to pool and evict old to maintain pressure
                        livePool.add(buf);
                        livePool.poll();
                    } catch (MemoryBackPressureException e) {
                        backpressureCount.incrementAndGet();
                        try { Thread.sleep(1); }
                        catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
                doneLatch.countDown();
            }, "hc3-thread-" + i);

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

        // Let the test run
        Thread.sleep(TEST_DURATION_MS);

        // Signal stop and wait for completion
        running.set(false);
        boolean allDone = doneLatch.await(15, TimeUnit.SECONDS);
        assertTrue(allDone, "All 8 threads should complete within timeout");

        // --- Release all live pool references and allow GC convergence ---
        livePool.clear();
        System.gc();
        Thread.sleep(2000);
        System.gc();
        Thread.sleep(1000);

        // --- Assertions ---

        // 1. Zero OOM escapes (Req 1.1, 1.2)
        assertEquals(0, escapedOomCount.get(),
            "Zero OOM should escape the allocator under constrained memory. "
                + "Escaped: " + escapedOomCount.get()
                + ", errors: " + escapedErrors);

        // 2. MemoryBackPressureException count > 0 (Req 1.5)
        assertTrue(backpressureCount.get() > 0,
            "MemoryBackPressureException should fire under constrained memory. "
                + "backpressureCount=" + backpressureCount.get()
                + ", totalAllocs=" + totalAllocsByThreads.get()
                + ", lastPressure=" + String.format("%.3f",
                    allocator.getLastPressureLevel()));

        // 3. allocatedByUs converges toward 0 after all references dropped
        //    and GC has had time to reclaim. Allow tolerance for GC lag.
        long allocByUs = allocator.getAllocatedByUs();
        long totalBytesAllocated = allocator.getTotalBytesAllocated();
        long reclaimedByGc = allocator.getReclaimedByGc();

        // After clearing the pool and running GC, most memory should be
        // reclaimed. allocatedByUs should be a small fraction of peak.
        // Tolerance: 10% of totalBytesAllocated or 50 MB, whichever is larger.
        long tolerance = Math.max(
            (long) (totalBytesAllocated * 0.10),
            50L * 1024 * 1024);
        assertTrue(allocByUs < tolerance,
            "allocatedByUs should converge toward 0 after GC. "
                + "allocatedByUs=" + allocByUs
                + ", reclaimedByGc=" + reclaimedByGc
                + ", totalBytesAllocated=" + totalBytesAllocated
                + ", tolerance=" + tolerance);

        // 4. allocatedByUs must be non-negative (accounting invariant)
        assertTrue(allocByUs >= 0,
            "allocatedByUs must be non-negative: " + allocByUs);

        // Log summary for observability
        System.out.println("[HC-3 ZeroOomConstrainedMemory] Summary:"
            + " threads=" + THREAD_COUNT
            + ", duration=" + TEST_DURATION_MS + "ms"
            + ", totalAllocs=" + totalAllocsByThreads.get()
            + ", backpressureCount=" + backpressureCount.get()
            + ", escapedOom=" + escapedOomCount.get()
            + ", allocatedByUs=" + allocByUs
            + ", reclaimedByGc=" + reclaimedByGc
            + ", totalBytesAllocated=" + totalBytesAllocated
            + ", gcHintCount=" + allocator.getGcHintCount()
            + ", cacheShrinkCount=" + allocator.getCacheShrinkCount()
            + ", gcDebtEma=" + String.format("%.0f",
                allocator.getGcDebtEma())
            + ", lastPressure=" + String.format("%.3f",
                allocator.getLastPressureLevel()));
    }
}
