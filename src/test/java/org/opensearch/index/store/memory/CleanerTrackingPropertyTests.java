/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.memory;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

/**
 * Property-based tests for Cleaner-based allocation tracking in
 * {@link DirectMemoryAllocator}.
 *
 * <p>These tests validate that the Cleaner mechanism correctly maintains the
 * accounting identity: {@code AllocatedByUs + ReclaimedByGc == totalBytesAllocated}
 * after GC converges.
 */
class CleanerTrackingPropertyTests {

    /**
     * Property 10: Cleaner-based AllocatedByUs accounting identity.
     *
     * <p>After allocating N real ByteBuffers, nulling all references, and
     * allowing GC to collect them, the accounting identity
     * {@code AllocatedByUs + ReclaimedByGc == totalBytesAllocated} must hold
     * (with tolerance for GC non-determinism).
     *
     * <p>Strategy:
     * <ol>
     *   <li>Allocate {@code numBuffers} real direct ByteBuffers via the allocator</li>
     *   <li>Record {@code totalBytesAllocated} (cumulative)</li>
     *   <li>Null all buffer references to make them eligible for GC</li>
     *   <li>Call {@code System.gc()} and sleep to let GC + Cleaner process</li>
     *   <li>Assert that {@code AllocatedByUs + ReclaimedByGc} converges to
     *       {@code totalBytesAllocated}</li>
     * </ol>
     *
     * <p>Uses generous timeout and retry because GC collection is non-deterministic.
     * The tolerance allows for buffers that GC has not yet collected.
     *
     * <p><b>Validates: Requirements 4.2, 4.4</b>
     *
     * @param numBuffers number of buffers to allocate (varied by jqwik)
     * @param bufferSizeMultiplier multiplier for buffer size (varied by jqwik)
     */
    @Property(tries = 20)
    void allocatedByUsPlusReclaimedByGcConvergesToTotalBytesAllocated(
        @ForAll @IntRange(min = 5, max = 30) int numBuffers,
        @ForAll @IntRange(min = 1, max = 4) int bufferSizeMultiplier
    ) throws MemoryBackPressureException, InterruptedException {
        int bufferSize = 4096 * bufferSizeMultiplier;
        long maxDirect = 1_000_000_000L; // 1 GB — generous to avoid pressure
        DirectMemoryAllocator allocator = new DirectMemoryAllocator(maxDirect, bufferSize);

        // Step 1: Allocate real ByteBuffers and hold references
        List<ByteBuffer> buffers = new ArrayList<>(numBuffers);
        for (int i = 0; i < numBuffers; i++) {
            buffers.add(allocator.allocate(bufferSize));
        }

        // Step 2: Record cumulative totalBytesAllocated
        long totalAllocated = allocator.getTotalBytesAllocated();
        long expectedTotal = (long) numBuffers * bufferSize;
        assertTrue(totalAllocated >= expectedTotal,
            "totalBytesAllocated must be >= numBuffers * bufferSize. "
                + "totalAllocated=" + totalAllocated + " expected=" + expectedTotal);

        // Verify accounting identity holds BEFORE GC (all buffers still live):
        // AllocatedByUs should equal totalAllocated, ReclaimedByGc should be 0
        long allocByUsBefore = allocator.getAllocatedByUs();
        long reclaimedBefore = allocator.getReclaimedByGc();
        assertTrue(allocByUsBefore + reclaimedBefore == totalAllocated,
            "Accounting identity must hold before GC: "
                + "AllocatedByUs(" + allocByUsBefore + ") + ReclaimedByGc("
                + reclaimedBefore + ") == totalBytesAllocated(" + totalAllocated + ")");

        // Step 3: Null all references to make buffers eligible for GC
        buffers.clear();
        buffers = null;

        // Step 4: Encourage GC to collect the buffers and fire Cleaner actions.
        // GC is non-deterministic, so we retry with increasing sleep.
        boolean converged = false;
        for (int attempt = 0; attempt < 10; attempt++) {
            System.gc();
            Thread.sleep(100 + attempt * 50L);

            long allocByUs = allocator.getAllocatedByUs();
            long reclaimedByGc = allocator.getReclaimedByGc();
            long sum = allocByUs + reclaimedByGc;

            if (sum == totalAllocated) {
                converged = true;
                break;
            }
        }

        // Step 5: Final assertion — the identity must hold after GC converges.
        // Even if not all buffers were collected, the identity
        // AllocatedByUs + ReclaimedByGc == totalBytesAllocated must always hold
        // because every buffer either still contributes to AllocatedByUs (not yet
        // collected) or has been moved to ReclaimedByGc (collected by Cleaner).
        long finalAllocByUs = allocator.getAllocatedByUs();
        long finalReclaimed = allocator.getReclaimedByGc();
        long finalSum = finalAllocByUs + finalReclaimed;

        assertTrue(finalSum == totalAllocated,
            "Accounting identity MUST hold: AllocatedByUs(" + finalAllocByUs
                + ") + ReclaimedByGc(" + finalReclaimed
                + ") = " + finalSum + " == totalBytesAllocated(" + totalAllocated + ")");

        // Additionally verify that GC reclaimed at least some buffers
        // (convergence check — GC non-determinism means we can't guarantee all)
        if (converged) {
            assertTrue(finalReclaimed > 0,
                "After GC convergence, ReclaimedByGc should be > 0");
            assertTrue(finalAllocByUs < totalAllocated,
                "After GC convergence, AllocatedByUs should be less than totalBytesAllocated");
        }
    }
}
