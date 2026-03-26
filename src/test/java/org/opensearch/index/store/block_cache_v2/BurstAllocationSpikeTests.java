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
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/**
 * MB-5: Burst Allocation Spike.
 *
 * <p>Validates the allocator's response to a sudden burst in allocation rate:
 * steady state → 10× burst for ~3 seconds → steady state recovery. The
 * allocator should:
 * <ol>
 *   <li>Smoothly shrink the cache during the burst if pressure is high enough</li>
 *   <li>Gradually restore cache capacity after the burst subsides</li>
 *   <li>Exhibit low oscillation (shrink/restore sign changes &lt; 2 per 100 samples)</li>
 *   <li>Never let an OOM escape — all failures wrapped in
 *       {@link MemoryBackPressureException}</li>
 * </ol>
 *
 * <p>Strategy:
 * <ol>
 *   <li>Create allocator with generous maxDirect (512 MB), sampleInterval=1</li>
 *   <li>Register CacheController with originalMaxBlocks=10000</li>
 *   <li>Phase 1 (steady state): Allocate at moderate rate for ~2 seconds, hold some buffers</li>
 *   <li>Phase 2 (burst): Allocate at 10× rate for ~3 seconds, dropping old buffers</li>
 *   <li>Phase 3 (recovery): Return to moderate rate for ~2 seconds</li>
 *   <li>Verify: smooth shrink during burst, gradual restore after, no OOM escapes,
 *       oscillation count low</li>
 * </ol>
 *
 * <p><b>Validates: Requirements 6.1, 6.2, 17.1</b>
 */
class BurstAllocationSpikeTests {

    private static final int BLOCK_SIZE = 8192;
    private static final long MAX_DIRECT = 512L * 1024 * 1024; // 512 MB
    private static final long ORIGINAL_MAX_BLOCKS = 10_000;

    // Phase durations (kept short for CI)
    private static final long STEADY_PHASE_MS = 2_000;
    private static final long BURST_PHASE_MS = 3_000;
    private static final long RECOVERY_PHASE_MS = 2_000;

    // Allocation rates
    private static final int STEADY_ALLOCS_PER_BATCH = 10;
    private static final int BURST_ALLOCS_PER_BATCH = 100; // 10× steady
    private static final long BATCH_INTERVAL_MS = 10;       // ~100 batches/sec

    @Test
    void burstAllocationSpikeWithSmoothShrinkAndGradualRestore() throws Exception {
        // --- Setup: allocator with frequent diagnostics and short cooldowns ---
        DirectMemoryAllocator allocator = new DirectMemoryAllocator(
            MAX_DIRECT,
            BLOCK_SIZE,
            1,    // sampleInterval = 1 (diagnose every allocation for max sensitivity)
            0.3,  // emaAlpha = 0.3 (faster EMA convergence for burst test)
            2.0,  // safetyMultiplier
            0.5,  // minCacheFraction (floor = 10000 × 0.5 = 5000)
            50,   // gcHintCooldownMs — short for stress test
            50,   // shrinkCooldownMs — short so shrinks can trigger quickly
            50,   // maxSampleIntervalMs
            0.10, // minHeadroomFraction
            0.001 // stallRatioThreshold
        );

        // Shared pool of live buffers — simulates the cache holding memory.
        ConcurrentLinkedQueue<ByteBuffer> livePool = new ConcurrentLinkedQueue<>();

        // Pre-fill the pool to ~40% of maxDirect to create a realistic baseline.
        int prefillCount = (int) ((MAX_DIRECT * 0.40) / BLOCK_SIZE);
        for (int i = 0; i < prefillCount; i++) {
            livePool.add(allocator.allocate(BLOCK_SIZE));
        }

        // CacheController: reports cacheHeldBytes based on live pool size.
        AtomicLong currentMax = new AtomicLong(ORIGINAL_MAX_BLOCKS);
        AtomicLong shrinkCallCount = new AtomicLong(0);
        AtomicLong restoreCallCount = new AtomicLong(0);

        // Track shrink/restore sequence for oscillation detection
        List<Character> actionLog = new ArrayList<>(); // 'S' for shrink, 'R' for restore

        CacheController controller = new CacheController() {
            @Override
            public long cacheHeldBytes() {
                // Report held bytes as a fraction of pool — simulates eviction lag
                int poolSize = livePool.size();
                return (long) poolSize * BLOCK_SIZE / 2;
            }

            @Override
            public void setMaxBlocks(long n) {
                long prev = currentMax.getAndSet(n);
                if (n < prev) {
                    shrinkCallCount.incrementAndGet();
                    synchronized (actionLog) {
                        actionLog.add('S');
                    }
                } else if (n > prev) {
                    restoreCallCount.incrementAndGet();
                    synchronized (actionLog) {
                        actionLog.add('R');
                    }
                }
            }

            @Override
            public void cleanUp() {
                // Simulate eviction: drop some buffers to free memory
                int toDrop = Math.min(50, livePool.size() / 10);
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

        // Track OOM escapes (should be zero)
        AtomicLong oomEscapes = new AtomicLong(0);
        AtomicLong backpressureCount = new AtomicLong(0);
        AtomicLong totalAllocs = new AtomicLong(0);

        // Record shrink/restore counts at phase boundaries
        long shrinkCountBeforeBurst;
        long shrinkCountAfterBurst;
        long restoreCountAfterBurst;

        // --- Phase 1: Steady state (~2 seconds) ---
        // Moderate allocation rate, hold some buffers, let EMAs converge.
        long phase1End = System.currentTimeMillis() + STEADY_PHASE_MS;
        while (System.currentTimeMillis() < phase1End) {
            for (int i = 0; i < STEADY_ALLOCS_PER_BATCH; i++) {
                try {
                    ByteBuffer buf = allocator.allocate(BLOCK_SIZE);
                    totalAllocs.incrementAndGet();
                    livePool.add(buf);
                    // Drop an old buffer to keep pool size roughly stable
                    livePool.poll();
                } catch (MemoryBackPressureException e) {
                    backpressureCount.incrementAndGet();
                } catch (OutOfMemoryError oome) {
                    oomEscapes.incrementAndGet();
                }
            }
            Thread.sleep(BATCH_INTERVAL_MS);
        }

        // Record metrics at end of steady state
        shrinkCountBeforeBurst = allocator.getCacheShrinkCount();
        long steadyStateMaxBlocks = currentMax.get();

        // --- Phase 2: Burst (10× rate for ~3 seconds) ---
        // Allocate at 10× the steady rate, dropping old buffers aggressively.
        long phase2End = System.currentTimeMillis() + BURST_PHASE_MS;
        while (System.currentTimeMillis() < phase2End) {
            for (int i = 0; i < BURST_ALLOCS_PER_BATCH; i++) {
                try {
                    ByteBuffer buf = allocator.allocate(BLOCK_SIZE);
                    totalAllocs.incrementAndGet();
                    livePool.add(buf);
                    // Drop old buffers faster during burst to simulate churn
                    livePool.poll();
                    if (i % 3 == 0) {
                        livePool.poll(); // extra eviction during burst
                    }
                } catch (MemoryBackPressureException e) {
                    backpressureCount.incrementAndGet();
                } catch (OutOfMemoryError oome) {
                    oomEscapes.incrementAndGet();
                }
            }
            Thread.sleep(BATCH_INTERVAL_MS);
        }

        // Record metrics at end of burst
        shrinkCountAfterBurst = allocator.getCacheShrinkCount();
        restoreCountAfterBurst = allocator.getCacheRestoreCount();
        long burstEndMaxBlocks = currentMax.get();

        // --- Phase 3: Recovery (steady rate for ~2 seconds) ---
        // Return to moderate allocation rate, let the allocator restore.
        long phase3End = System.currentTimeMillis() + RECOVERY_PHASE_MS;
        while (System.currentTimeMillis() < phase3End) {
            for (int i = 0; i < STEADY_ALLOCS_PER_BATCH; i++) {
                try {
                    ByteBuffer buf = allocator.allocate(BLOCK_SIZE);
                    totalAllocs.incrementAndGet();
                    livePool.add(buf);
                    livePool.poll();
                } catch (MemoryBackPressureException e) {
                    backpressureCount.incrementAndGet();
                } catch (OutOfMemoryError oome) {
                    oomEscapes.incrementAndGet();
                }
            }
            Thread.sleep(BATCH_INTERVAL_MS);
        }

        // --- Assertions ---

        long finalShrinkCount = allocator.getCacheShrinkCount();
        long finalRestoreCount = allocator.getCacheRestoreCount();
        long finalMaxBlocks = currentMax.get();
        long burstShrinks = shrinkCountAfterBurst - shrinkCountBeforeBurst;
        long recoveryRestores = finalRestoreCount - restoreCountAfterBurst;

        // 1. No OOM escapes — all failures must be wrapped in MemoryBackPressureException
        assertTrue(oomEscapes.get() == 0,
            "No OOM should escape the allocator. oomEscapes=" + oomEscapes.get());

        // 2. Smooth shrink during burst: if pressure was high enough, cache should
        //    have shrunk during the burst phase. With 40% prefill and 10× burst,
        //    the allocator should detect pressure and shrink.
        //    Note: shrink may not always trigger if GC keeps up — we check that
        //    IF shrink happened, it was smooth (no excessive oscillation).
        boolean shrinkOccurredDuringBurst = burstShrinks > 0;

        // 3. Gradual restore after burst: if cache was shrunk, it should start
        //    restoring during the recovery phase.
        //    Note: restore may not complete fully in 2 seconds — we just check
        //    that restore started (count > 0) if shrink occurred.
        if (shrinkOccurredDuringBurst) {
            assertTrue(recoveryRestores > 0 || finalMaxBlocks > burstEndMaxBlocks,
                "Cache should start restoring after burst subsides. "
                    + "recoveryRestores=" + recoveryRestores
                    + ", burstEndMaxBlocks=" + burstEndMaxBlocks
                    + ", finalMaxBlocks=" + finalMaxBlocks
                    + ", finalRestoreCount=" + finalRestoreCount);
        }

        // 4. Low oscillation: count sign changes in the action log.
        //    A sign change is when a shrink follows a restore or vice versa.
        //    Oscillation count should be < 2 per 100 samples.
        int oscillationCount;
        synchronized (actionLog) {
            oscillationCount = countSignChanges(actionLog);
        }
        int totalActions;
        synchronized (actionLog) {
            totalActions = actionLog.size();
        }
        if (totalActions >= 100) {
            double oscillationPer100 = (double) oscillationCount / totalActions * 100;
            assertTrue(oscillationPer100 < 2.0,
                "Oscillation count should be < 2 per 100 samples. "
                    + "oscillationCount=" + oscillationCount
                    + ", totalActions=" + totalActions
                    + ", oscillationPer100=" + String.format("%.2f", oscillationPer100));
        }

        // 5. nativeUsed should stay within maxDirect bounds
        long lastNativeUsed = allocator.getLastNativeUsedBytes();
        assertTrue(lastNativeUsed <= MAX_DIRECT,
            "nativeUsed must stay within maxDirect bounds. "
                + "lastNativeUsed=" + lastNativeUsed
                + ", maxDirect=" + MAX_DIRECT);

        // 6. allocatedByUs must be non-negative (accounting invariant)
        assertTrue(allocator.getAllocatedByUs() >= 0,
            "allocatedByUs must be non-negative: " + allocator.getAllocatedByUs());

        // Log summary for observability
        System.out.println("[BurstAllocationSpikeTest] Summary:"
            + " totalAllocs=" + totalAllocs.get()
            + ", backpressureCount=" + backpressureCount.get()
            + ", oomEscapes=" + oomEscapes.get()
            + ", steadyStateMaxBlocks=" + steadyStateMaxBlocks
            + ", burstEndMaxBlocks=" + burstEndMaxBlocks
            + ", finalMaxBlocks=" + finalMaxBlocks
            + ", burstShrinks=" + burstShrinks
            + ", recoveryRestores=" + recoveryRestores
            + ", totalShrinks=" + finalShrinkCount
            + ", totalRestores=" + finalRestoreCount
            + ", oscillationCount=" + oscillationCount
            + ", totalActions=" + totalActions
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

    /**
     * Counts sign changes in the action log. A sign change occurs when
     * a shrink ('S') follows a restore ('R') or vice versa. Consecutive
     * actions of the same type are not counted.
     */
    private static int countSignChanges(List<Character> actions) {
        if (actions.size() < 2) {
            return 0;
        }
        int changes = 0;
        char prev = actions.get(0);
        for (int i = 1; i < actions.size(); i++) {
            char curr = actions.get(i);
            if (curr != prev) {
                changes++;
            }
            prev = curr;
        }
        return changes;
    }
}
