/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.memory;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/**
 * MB-4: External Memory Pressure.
 *
 * <p>Validates that the allocator correctly detects external direct memory
 * consumers (buffers allocated outside the allocator via raw
 * {@code ByteBuffer.allocateDirect()}) and does NOT aggressively shrink its
 * own cache in response. External pressure is reflected in {@code nativeUsed}
 * (via BufferPoolMXBean) but NOT in {@code AllocatedByUs}, so the allocator
 * should compute positive {@code External_Usage} and track it via
 * {@code externalUsageEma}.
 *
 * <p>The key insight: when pressure comes from external consumers, shrinking
 * the allocator's cache won't help — the allocator's Tier 3 logic guards
 * against this by checking {@code externalUsageGrowthRate} and
 * {@code externalUsageEma} before shrinking.
 *
 * <p>Strategy:
 * <ol>
 *   <li>Create allocator with generous maxDirect (512 MB), sampleInterval=1</li>
 *   <li>Register CacheController with originalMaxBlocks=10000</li>
 *   <li>Allocate some buffers through the allocator (build up allocatedByUs)</li>
 *   <li>Allocate large ByteBuffer.allocateDirect() blocks OUTSIDE the allocator</li>
 *   <li>Run diagnoseAndRespond() multiple times to let EMAs converge</li>
 *   <li>Verify: externalUsageEma &gt; 0, cacheShrinkCount == 0 or very low,
 *       getLastExternalUsage() &gt; 0</li>
 * </ol>
 *
 * <p><b>Validates: Requirements 5.1, 5.4</b>
 */
class ExternalMemoryPressureTests {

    private static final int BLOCK_SIZE = 8192;
    private static final long MAX_DIRECT = 512L * 1024 * 1024; // 512 MB
    private static final long ORIGINAL_MAX_BLOCKS = 10_000;

    @Test
    void externalPressureDetectedAndCacheNotAggressivelyShrunk() throws Exception {
        // --- Setup: allocator with sampleInterval=1 for frequent diagnostics ---
        DirectMemoryAllocator allocator = new DirectMemoryAllocator(
            MAX_DIRECT,
            BLOCK_SIZE,
            1,    // sampleInterval = 1 (diagnose every allocation)
            0.3,  // emaAlpha = 0.3 (faster EMA convergence)
            2.0,  // safetyMultiplier
            0.5,  // minCacheFraction
            200,  // gcHintCooldownMs
            200,  // shrinkCooldownMs
            100,  // maxSampleIntervalMs
            0.10, // minHeadroomFraction
            0.001 // stallRatioThreshold
        );

        // CacheController: reports cacheHeldBytes matching what we allocate through
        // the allocator, so GC_Debt stays low (no internal pressure).
        AtomicLong currentMax = new AtomicLong(ORIGINAL_MAX_BLOCKS);
        AtomicLong shrinkCallCount = new AtomicLong(0);

        CacheController controller = new CacheController() {
            @Override
            public long cacheHeldBytes() {
                // Report held bytes close to allocatedByUs so GC_Debt is near zero.
                // This means any pressure the allocator sees is from external usage.
                return allocator.getAllocatedByUs();
            }

            @Override
            public void setMaxBlocks(long n) {
                long prev = currentMax.getAndSet(n);
                if (n < prev) {
                    shrinkCallCount.incrementAndGet();
                }
            }

            @Override
            public void cleanUp() {
                // no-op
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

        // --- Phase 1: Allocate some buffers through the allocator ---
        // Build up allocatedByUs so the allocator has a baseline.
        int allocatorBufferCount = 500;
        List<ByteBuffer> allocatorBuffers = new ArrayList<>(allocatorBufferCount);
        for (int i = 0; i < allocatorBufferCount; i++) {
            allocatorBuffers.add(allocator.allocate(BLOCK_SIZE));
        }

        long allocByUsAfterPhase1 = allocator.getAllocatedByUs();
        assertTrue(allocByUsAfterPhase1 > 0,
            "allocatedByUs should be positive after Phase 1: " + allocByUsAfterPhase1);

        // Reset metrics so we measure shrink count from this point forward
        allocator.resetMetrics();

        // --- Phase 2: Allocate large blocks OUTSIDE the allocator ---
        // These go through ByteBuffer.allocateDirect() directly, bypassing the
        // allocator's Cleaner tracking. They increase nativeUsed but NOT
        // allocatedByUs, creating positive External_Usage.
        int externalBlockSize = 1024 * 1024; // 1 MB each
        int externalBlockCount = 100;         // 100 MB total external
        List<ByteBuffer> externalBuffers = new ArrayList<>(externalBlockCount);
        for (int i = 0; i < externalBlockCount; i++) {
            externalBuffers.add(ByteBuffer.allocateDirect(externalBlockSize));
        }

        long expectedExternalBytes = (long) externalBlockSize * externalBlockCount;

        // --- Phase 3: Run diagnoseAndRespond() multiple times to let EMAs converge ---
        // Each call samples MXBean, computes External_Usage, and updates externalUsageEma.
        // We also do small allocations through the allocator to trigger diagnostics.
        for (int i = 0; i < 50; i++) {
            try {
                allocator.allocate(BLOCK_SIZE);
            } catch (MemoryBackPressureException e) {
                // Acceptable — hard floor may trigger if headroom is tight
            }
        }

        // --- Assertions ---

        // 1. External usage should be detected: getLastExternalUsage() > 0
        //    External_Usage = nativeUsed - AllocatedByUs. Since we allocated 100 MB
        //    outside the allocator, this should be significantly positive.
        long lastExternalUsage = allocator.getLastExternalUsage();
        assertTrue(lastExternalUsage > 0,
            "External usage should be detected (> 0). "
                + "lastExternalUsage=" + lastExternalUsage
                + ", allocatedByUs=" + allocator.getAllocatedByUs()
                + ", lastNativeUsed=" + allocator.getLastNativeUsedBytes()
                + ", expectedExternalBytes=" + expectedExternalBytes);

        // 2. externalUsageEma should track the external footprint (> 0)
        double extEma = allocator.getExternalUsageEma();
        assertTrue(extEma > 0,
            "externalUsageEma should be > 0 after external allocations. "
                + "externalUsageEma=" + String.format("%.0f", extEma)
                + ", lastExternalUsage=" + lastExternalUsage);

        // 3. Cache shrink count should be 0 or very low.
        //    The allocator should NOT aggressively shrink its cache for external
        //    pressure — the Tier 3 guard checks externalUsageGrowthRate and
        //    externalUsageEma before allowing a shrink.
        long shrinkCount = allocator.getCacheShrinkCount();
        assertTrue(shrinkCount <= 2,
            "Allocator should NOT aggressively shrink cache for external pressure. "
                + "cacheShrinkCount=" + shrinkCount
                + " (expected <= 2). "
                + "externalUsageEma=" + String.format("%.0f", extEma)
                + ", externalUsageGrowthRate=" + String.format("%.4f", allocator.getExternalUsageGrowthRate())
                + ", gcDebtEma=" + String.format("%.0f", allocator.getGcDebtEma())
                + ", lastPressure=" + String.format("%.3f", allocator.getLastPressureLevel()));

        // 4. Verify allocatedByUs is non-negative (accounting invariant)
        assertTrue(allocator.getAllocatedByUs() >= 0,
            "allocatedByUs must be non-negative: " + allocator.getAllocatedByUs());

        // 5. Verify the cache was not shrunk via the CacheController callback
        assertTrue(shrinkCallCount.get() <= 2,
            "CacheController.setMaxBlocks() shrink calls should be <= 2. "
                + "shrinkCallCount=" + shrinkCallCount.get());

        // Log summary for observability
        System.out.println("[ExternalMemoryPressureTest] Summary:"
            + " allocatedByUs=" + allocator.getAllocatedByUs()
            + ", lastNativeUsed=" + allocator.getLastNativeUsedBytes()
            + ", lastExternalUsage=" + lastExternalUsage
            + ", externalUsageEma=" + String.format("%.0f", extEma)
            + ", externalUsageGrowthRate=" + String.format("%.4f", allocator.getExternalUsageGrowthRate())
            + ", gcDebtEma=" + String.format("%.0f", allocator.getGcDebtEma())
            + ", cacheShrinkCount=" + shrinkCount
            + ", shrinkCallCount=" + shrinkCallCount.get()
            + ", lastPressure=" + String.format("%.3f", allocator.getLastPressureLevel())
            + ", lastHeadroom=" + allocator.getLastHeadroomBytes()
            + ", targetHeadroom=" + allocator.getTargetHeadroomBytes()
            + ", gcHintCount=" + allocator.getGcHintCount()
            + ", tier3Count=" + allocator.getTier3LastResortCount());

        // Clean up: release external buffers
        externalBuffers.clear();
        allocatorBuffers.clear();
    }
}
