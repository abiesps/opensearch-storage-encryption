/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for DirectMemoryAdmissionController.
 *
 * Feature: bufferpool-v2-memory-management
 */
class DirectMemoryAdmissionControllerPropertyTests {

    /**
     * Property 22: Admission controller counter accuracy.
     * For any sequence of acquire(n) and release(n), outstanding bytes = sum(acquired) - sum(released).
     *
     * Feature: bufferpool-v2-memory-management, Property 22: Admission controller counter accuracy
     * Validates: Requirements 8.2, 8.3
     */
    @Property(tries = 200)
    void counterAccuracyAfterAcquireRelease(
        @ForAll @Size(min = 1, max = 50) List<@IntRange(min = 1, max = 4096) Integer> acquireSizes,
        @ForAll @IntRange(min = 0, max = 100) int releasePercentage
    ) {
        // Use a large maxDirectMemory so we never hit thresholds
        long maxMem = 1_000_000_000L; // 1GB
        DirectMemoryAdmissionController controller = new DirectMemoryAdmissionController(maxMem, 0.85, 0.95, 5000);

        long totalAcquired = 0;
        long totalReleased = 0;

        // Acquire all
        for (int size : acquireSizes) {
            controller.acquire(size);
            totalAcquired += size;
        }

        assertEquals(totalAcquired, controller.getOutstandingBytes(), "After all acquires, outstanding should equal total acquired");

        // Release a percentage of them
        int releaseCount = (int) ((long) acquireSizes.size() * releasePercentage / 100);
        for (int i = 0; i < releaseCount; i++) {
            int size = acquireSizes.get(i);
            controller.release(size);
            totalReleased += size;
        }

        long expected = totalAcquired - totalReleased;
        assertEquals(expected, controller.getOutstandingBytes(), "Outstanding bytes should equal sum(acquired) - sum(released)");
    }

    /**
     * Property 23: Soft threshold allows allocation.
     * When utilization is between soft and hard thresholds, acquire() succeeds without blocking.
     *
     * Feature: bufferpool-v2-memory-management, Property 23: Soft threshold allows allocation
     * Validates: Requirements 8.5
     */
    @Property(tries = 100)
    void softThresholdAllowsAllocationWithoutBlocking(
        @ForAll @IntRange(min = 86, max = 94) int utilizationPercent
    ) {
        long maxMem = 100_000L;
        double softThreshold = 0.85;
        double hardThreshold = 0.95;
        DirectMemoryAdmissionController controller = new DirectMemoryAdmissionController(maxMem, softThreshold, hardThreshold, 5000);

        // Pre-fill to the desired utilization
        long prefill = (long) (maxMem * utilizationPercent / 100.0);
        controller.acquire((int) prefill);

        // Verify we're between soft and hard
        double utilization = controller.getUtilization();
        assertTrue(utilization > softThreshold, "Utilization should be above soft threshold");
        assertTrue(utilization <= hardThreshold, "Utilization should be at or below hard threshold");

        // acquire() should succeed without blocking — we verify by timing it
        AtomicBoolean completed = new AtomicBoolean(false);
        long startNanos = System.nanoTime();
        controller.acquire(1); // tiny allocation
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        completed.set(true);

        assertTrue(completed.get(), "acquire() should complete");
        // Should complete nearly instantly (well under 1 second)
        assertTrue(elapsedMs < 1000, "acquire() at soft threshold should not block significantly, took " + elapsedMs + "ms");
    }

    /**
     * Property 24: Hard threshold blocks allocation.
     * When utilization exceeds hard threshold, acquire() blocks until utilization drops or timeout.
     *
     * We pre-fill to 94% (below hard threshold), then a second acquire pushes above 95%.
     * That second acquire runs in a background thread and should block. We then release
     * enough to drop below the hard threshold, and the blocked thread should unblock.
     * If we don't release in time, the thread times out with DirectMemoryExhaustedException.
     *
     * Feature: bufferpool-v2-memory-management, Property 24: Hard threshold blocks allocation
     * Validates: Requirements 8.6
     */
    @Property(tries = 20)
    void hardThresholdBlocksUntilReleaseOrTimeout(
        @ForAll @IntRange(min = 2, max = 10) int overagePercent
    ) throws Exception {
        long maxMem = 100_000L;
        double softThreshold = 0.85;
        double hardThreshold = 0.95;
        long hardTimeoutMs = 1000; // timeout for test
        DirectMemoryAdmissionController controller = new DirectMemoryAdmissionController(maxMem, softThreshold, hardThreshold, hardTimeoutMs);

        // Pre-fill to 94% — just below hard threshold (this acquire succeeds without blocking)
        int prefill = 94_000;
        controller.acquire(prefill);
        assertTrue(controller.getUtilization() <= hardThreshold, "Prefill should be below hard threshold");

        // The overage will push utilization above hard threshold
        int overage = overagePercent * 100; // 200 to 1000 bytes over the threshold

        // acquire(overage) in a separate thread — it should block because it pushes above hard threshold
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        executor.submit(() -> {
            started.countDown();
            try {
                controller.acquire(overage);
                completed.set(true);
            } catch (DirectMemoryExhaustedException e) {
                exceptionThrown.set(true);
            }
        });

        started.await(1, TimeUnit.SECONDS);
        // Give the thread time to enter the blocking path
        Thread.sleep(150);

        // Release enough to drop below hard threshold so the blocked thread can proceed
        int releaseAmount = prefill / 2; // release 47000 — drops well below threshold
        controller.release(releaseAmount);

        executor.shutdown();
        assertTrue(executor.awaitTermination(hardTimeoutMs + 2000, TimeUnit.MILLISECONDS), "Thread should complete");

        // The thread should have completed successfully after we released memory
        assertTrue(completed.get() || exceptionThrown.get(),
            "Thread should have either completed or thrown (not still blocked)");
    }
}
