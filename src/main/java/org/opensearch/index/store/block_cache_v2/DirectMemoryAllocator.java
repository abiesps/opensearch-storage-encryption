/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

/**
 * Adaptive GC-assisted direct memory allocator with Cleaner-based tracking.
 *
 * <p>Wraps {@code ByteBuffer.allocateDirect()} with proactive memory pressure
 * management. Every {@code sampleInterval} allocations (or when
 * {@code maxSampleIntervalMs} elapses), samples {@link BufferPoolMXBean} and
 * computes <b>GC debt</b> using a {@link Cleaner}-based tracking mechanism:
 * {@code GC_Debt = AllocatedByUs - cacheHeldBytes}.
 *
 * <p>Uses an exponential moving average of GC debt to derive an adaptive
 * headroom target. When headroom falls below the target, takes tiered
 * corrective action:
 *
 * <ul>
 *   <li>Tier 1 (pressure &lt; 0.3): no-op — let ZGC catch up naturally</li>
 *   <li>Tier 2 (0.3 &le; pressure &lt; 0.8): rate-limited {@code System.gc()}</li>
 *   <li>Tier 3 (pressure &ge; 0.8): GC hint + proportional cache shrink</li>
 *   <li>Restore: gradual cache capacity recovery when pressure subsides</li>
 * </ul>
 *
 * <h2>Key formulas</h2>
 * <pre>
 *   GC_Debt          = max(0, AllocatedByUs - cacheHeldBytes)
 *   GC_Debt_EMA      = alpha * GC_Debt + (1-alpha) * GC_Debt_EMA
 *   Target_Headroom  = max(GC_Debt_EMA * safetyMultiplier + safetyMargin,
 *                          maxDirect * minHeadroomFraction)
 *   Headroom         = maxDirectMemoryBytes - nativeUsed
 * </pre>
 *
 * @see CacheController
 * @see MemoryBackPressureException
 * @see LastAction
 */
public final class DirectMemoryAllocator {

    /** Point-in-time snapshot of all allocator diagnostics for external observability. */
    public record DiagnosticSnapshot(
        long timestampMs,
        long totalAllocations, long totalBytesAllocated, long allocatedByUs, long reclaimedByGc,
        double pressureLevel, double gcDebtEma, double deficitEma, long cleanerLagApprox,
        long targetHeadroomBytes, long lastHeadroomBytes,
        long stallCount, long stallNanosTotal,
        long gcHintCount, long gcHintSkippedCooldown,
        long cacheShrinkCount, long cacheShrinkBlocksTotal,
        long cacheRestoreCount, long cacheRestoreBlocksTotal,
        long totalBlocksEvicted,
        long tier1Count, long tier2Count, long tier3Count,
        long lastNativeUsedBytes, long lastExternalUsage, double externalUsageEma,
        int consecutiveStallWindows, long lyapunovViolationCount,
        LastAction lastAction
    ) {}

    private static final Logger LOG = Logger.getLogger(DirectMemoryAllocator.class.getName());

    // ======================== Configuration defaults ========================

    static final int DEFAULT_SAMPLE_INTERVAL = 4096;
    static final double DEFAULT_EMA_ALPHA = 0.1;
    static final double DEFAULT_SAFETY_MULTIPLIER = 2.0;
    static final double DEFAULT_MIN_CACHE_FRACTION = 0.5;
    static final long DEFAULT_GC_HINT_COOLDOWN_MS = 200;
    static final long DEFAULT_SHRINK_COOLDOWN_MS = 200;
    static final long DEFAULT_MAX_SAMPLE_INTERVAL_MS = 100;
    static final double DEFAULT_MIN_HEADROOM_FRACTION = 0.10;
    static final double DEFAULT_STALL_RATIO_THRESHOLD = 0.001;
    static final long DEFAULT_STALL_THRESHOLD_NANOS = 1_000_000L;
    static final int DEFAULT_BLOCK_SIZE = 8192;

    // ======================== Configuration (final) ========================

    private final long maxDirectMemoryBytes;
    private final int blockSize;
    private final int sampleInterval;
    private final int sampleMask;
    private final double emaAlpha;
    private final double safetyMultiplier;
    private final double minCacheFraction;
    private final double minHeadroomFraction;
    private final long gcHintCooldownNanos;
    private final long shrinkCooldownNanos;
    private final long maxSampleIntervalNanos;
    private final double stallRatioThreshold;
    private final long stallThresholdNanos;
    private final boolean systemGcDisabled;

    // ======================== JVM references ========================

    private final BufferPoolMXBean directPool;
    private final Cleaner cleaner;

    // ======================== CacheController (volatile, nullable) ========================

    private volatile CacheController cacheController;

    // ======================== Atomic counters ========================

    private final AtomicLong allocCounter = new AtomicLong(0);
    private final AtomicLong allocatedByUs = new AtomicLong(0);
    private final LongAdder reclaimedByGc = new LongAdder();

    // ======================== Metric counters (LongAdder) ========================

    private final LongAdder totalAllocations = new LongAdder();
    private final LongAdder totalBytesAllocated = new LongAdder();
    private final LongAdder totalBytesRequested = new LongAdder();
    private final LongAdder stallCount = new LongAdder();
    private final LongAdder stallNanosTotal = new LongAdder();
    private final LongAdder windowedStalls = new LongAdder();
    private final LongAdder windowedAllocations = new LongAdder();
    private final LongAdder gcHintCount = new LongAdder();
    private final LongAdder gcHintModeratePressure = new LongAdder();
    private final LongAdder gcHintWithShrink = new LongAdder();
    private final LongAdder gcHintSkippedCooldown = new LongAdder();
    private final LongAdder cacheShrinkCount = new LongAdder();
    private final LongAdder cacheRestoreCount = new LongAdder();
    private final LongAdder totalBlocksEvicted = new LongAdder();
    private final LongAdder tier1NoActionCount = new LongAdder();
    private final LongAdder tier2ModeratePressureCount = new LongAdder();
    private final LongAdder tier3LastResortCount = new LongAdder();
    private final LongAdder driftDetectedCount = new LongAdder();
    private final LongAdder cacheShrinkBlocksTotal = new LongAdder();
    private final LongAdder cacheRestoreBlocksTotal = new LongAdder();

    // ======================== Diagnostic lock ========================

    private final AtomicBoolean diagnosticLock = new AtomicBoolean(false);

    // ======================== Volatile diagnostic state ========================

    private volatile double gcDebtEma = 0.0;
    private volatile long targetHeadroomBytes = 0;
    private volatile long lastGcDebtBytes = 0;
    private volatile long lastNativeUsedBytes = 0;
    private volatile long lastCacheHeldBytes = 0;
    private volatile long lastHeadroomBytes;
    private volatile double lastPressureLevel = 0.0;
    private volatile double deficitEma = 0.0;
    private volatile long lastExternalUsage = 0;
    private volatile double externalUsageEma = 0.0;
    private volatile double externalUsageGrowthRate = 0.0;
    private volatile double gcDebtGrowthRate = 0.0;
    private volatile LastAction lastAction = LastAction.NONE;
    private volatile boolean isCacheShrunk = false;
    private volatile long lastGcHintNanos = 0;
    private volatile long lastShrinkNanos = 0;
    private volatile long gcDebtAtLastShrink = 0;
    private volatile double gcDebtEmaAtLastShrink = 0.0;
    private volatile long deficitAtLastShrink = 0;
    private volatile long lastDiagnosticNanos = 0;
    private volatile int consecutiveStallWindows = 0;
    private volatile int lyapunovViolationCount = 0;
    private volatile long cleanerLagApprox = 0;
    private volatile long nativeGap = 0;
    private volatile long prevGcDebt = 0;
    private volatile double prevExternalUsage = 0.0;
    private volatile boolean forcePreCheck = false;
    private volatile int preCheckFailCount = 0;

    // Lyapunov monitoring state
    private volatile double prevDeficitEma = 0.0;
    private volatile double prevGcDebtEma = 0.0;
    private volatile double prevCacheDeviation = 0.0;

    // Baseline external usage (sampled at construction before cache allocations)
    private final long baselineExternalUsage;

    // ======================== CleanerAction inner class ========================

    /**
     * Registered on each ByteBuffer via {@link Cleaner}. When GC collects the
     * ByteBuffer, decrements {@code AllocatedByUs} and increments
     * {@code ReclaimedByGc}. Captures only the buffer size ({@code int}),
     * NOT a reference to the ByteBuffer, to avoid preventing GC collection.
     */
    private static final class CleanerAction implements Runnable {

        private final int size;
        private final AtomicLong allocatedByUs;
        private final LongAdder reclaimedByGc;

        CleanerAction(int size, AtomicLong allocatedByUs, LongAdder reclaimedByGc) {
            this.size = size;
            this.allocatedByUs = allocatedByUs;
            this.reclaimedByGc = reclaimedByGc;
        }

        @Override
        public void run() {
            allocatedByUs.addAndGet(-size);
            reclaimedByGc.add(size);
        }
    }

    // ======================== Constructors ========================

    /**
     * Full-parameter constructor.
     *
     * @param maxDirectMemoryBytes  upper bound on native direct memory (&gt; 0)
     * @param blockSize             cache block size in bytes (&gt; 0)
     * @param sampleInterval        allocations between diagnostics (power of 2)
     * @param emaAlpha              EMA smoothing factor, in (0, 1)
     * @param safetyMultiplier      multiplier on GC_Debt_EMA for target headroom (&ge; 1)
     * @param minCacheFraction      minimum cache capacity fraction, in (0, 1)
     * @param gcHintCooldownMs      minimum ms between System.gc() calls (&ge; 0)
     * @param shrinkCooldownMs      minimum ms between cache shrinks (&ge; 0)
     * @param maxSampleIntervalMs   max wall-clock ms between diagnostics (&ge; 0)
     * @param minHeadroomFraction   minimum headroom as fraction of maxDirect, in (0, 1)
     * @param stallRatioThreshold   stall ratio threshold for MXBean override, in (0, 1)
     */
    public DirectMemoryAllocator(long maxDirectMemoryBytes, int blockSize, int sampleInterval,
            double emaAlpha, double safetyMultiplier, double minCacheFraction,
            long gcHintCooldownMs, long shrinkCooldownMs, long maxSampleIntervalMs,
            double minHeadroomFraction, double stallRatioThreshold) {
        if (maxDirectMemoryBytes <= 0) {
            throw new IllegalArgumentException("maxDirectMemoryBytes must be positive");
        }
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive");
        }
        if (Integer.bitCount(sampleInterval) != 1) {
            throw new IllegalArgumentException("sampleInterval must be a power of 2");
        }
        if (emaAlpha <= 0 || emaAlpha >= 1) {
            throw new IllegalArgumentException("emaAlpha must be in (0, 1)");
        }
        if (safetyMultiplier < 1) {
            throw new IllegalArgumentException("safetyMultiplier must be >= 1");
        }
        if (minCacheFraction <= 0 || minCacheFraction >= 1) {
            throw new IllegalArgumentException("minCacheFraction must be in (0, 1)");
        }
        if (minHeadroomFraction <= 0 || minHeadroomFraction >= 1) {
            throw new IllegalArgumentException("minHeadroomFraction must be in (0, 1)");
        }
        if (gcHintCooldownMs < 0) {
            throw new IllegalArgumentException("gcHintCooldownMs must be >= 0");
        }
        if (shrinkCooldownMs < 0) {
            throw new IllegalArgumentException("shrinkCooldownMs must be >= 0");
        }
        if (maxSampleIntervalMs < 0) {
            throw new IllegalArgumentException("maxSampleIntervalMs must be >= 0");
        }
        if (stallRatioThreshold <= 0 || stallRatioThreshold >= 1) {
            throw new IllegalArgumentException("stallRatioThreshold must be in (0, 1)");
        }

        this.maxDirectMemoryBytes = maxDirectMemoryBytes;
        this.blockSize = blockSize;
        this.sampleInterval = sampleInterval;
        this.sampleMask = sampleInterval - 1;
        this.emaAlpha = emaAlpha;
        this.safetyMultiplier = safetyMultiplier;
        this.minCacheFraction = minCacheFraction;
        this.minHeadroomFraction = minHeadroomFraction;
        this.gcHintCooldownNanos = gcHintCooldownMs * 1_000_000L;
        this.shrinkCooldownNanos = shrinkCooldownMs * 1_000_000L;
        this.maxSampleIntervalNanos = maxSampleIntervalMs * 1_000_000L;
        this.stallRatioThreshold = stallRatioThreshold;
        this.stallThresholdNanos = DEFAULT_STALL_THRESHOLD_NANOS;
        this.systemGcDisabled = detectSystemGcDisabled();
        this.directPool = findDirectPool();
        this.cleaner = Cleaner.create();

        // Initialize lastHeadroomBytes to maxDirect (no allocations yet)
        this.lastHeadroomBytes = maxDirectMemoryBytes;

        // Sample baseline external usage before any cache allocations
        this.baselineExternalUsage = sampleBaselineExternalUsage();

        if (systemGcDisabled) {
            LOG.warning("System.gc() is disabled (-XX:+DisableExplicitGC detected). "
                    + "All pressure relief will use cache shrink.");
        }

        LOG.info(String.format(
                "DirectMemoryAllocator initialized: maxDirect=%d, blockSize=%d, "
                        + "sampleInterval=%d, emaAlpha=%.2f, safetyMultiplier=%.1f, "
                        + "minCacheFraction=%.2f, minHeadroomFraction=%.2f, "
                        + "baselineExternalUsage=%d, systemGcDisabled=%s",
                maxDirectMemoryBytes, blockSize, sampleInterval, emaAlpha,
                safetyMultiplier, minCacheFraction, minHeadroomFraction,
                baselineExternalUsage, systemGcDisabled));
    }

    /**
     * Convenience constructor with default tuning parameters.
     *
     * @param maxDirectMemoryBytes upper bound on native direct memory
     * @param blockSize            cache block size in bytes
     */
    public DirectMemoryAllocator(long maxDirectMemoryBytes, int blockSize) {
        this(maxDirectMemoryBytes, blockSize, DEFAULT_SAMPLE_INTERVAL, DEFAULT_EMA_ALPHA,
                DEFAULT_SAFETY_MULTIPLIER, DEFAULT_MIN_CACHE_FRACTION,
                DEFAULT_GC_HINT_COOLDOWN_MS, DEFAULT_SHRINK_COOLDOWN_MS,
                DEFAULT_MAX_SAMPLE_INTERVAL_MS, DEFAULT_MIN_HEADROOM_FRACTION,
                DEFAULT_STALL_RATIO_THRESHOLD);
    }

    /**
     * No-arg constructor: auto-detects maxDirectMemoryBytes from
     * {@code sun.misc.VM.maxDirectMemory()} and uses default block size (8192).
     */
    public DirectMemoryAllocator() {
        this(resolveMaxDirectMemory(), DEFAULT_BLOCK_SIZE);
    }

    // ======================== Lifecycle ========================

    /**
     * Registers the cache controller. Called by the block cache during init.
     * Only one controller may be registered at a time.
     *
     * @param controller the cache controller to register
     */
    public void registerCacheController(CacheController controller) {
        this.cacheController = controller;
    }

    /**
     * Deregisters the cache controller, setting the reference to null.
     * After this call, diagnostic actions that depend on the CacheController
     * will be skipped gracefully (no NPE).
     */
    public void deregisterCacheController() {
        this.cacheController = null;
    }

    // ======================== Core API ========================

    /**
     * Allocates a direct ByteBuffer. Every {@code sampleInterval} allocations
     * (or when {@code maxSampleIntervalMs} elapses), runs the adaptive
     * diagnostic and takes corrective action if needed.
     *
     * <p>Implements the full allocation flow:
     * <ol>
     *   <li>Atomic counter increments (allocCounter, totalBytesRequested, windowedAllocations)</li>
     *   <li>Hard floor fast-fail with adaptive safety margin (Req 1.5)</li>
     *   <li>Periodic diagnostic trigger (count-based or time-based) with tryLock (Req 3.1, 3.2, 3.3)</li>
     *   <li>Pre-check fast-fail: cache at floor + headroom below target + no improvement (Req 1.3)</li>
     *   <li>Tightly-scoped OOM catch around {@code ByteBuffer.allocateDirect()} only (Req 1.1, 1.2)</li>
     *   <li>Accounting: totalBytesAllocated → allocatedByUs → Cleaner registration (Req 4.2)</li>
     *   <li>Stall detection on every allocation (Req 3.4)</li>
     * </ol>
     *
     * @param size the number of bytes to allocate
     * @return a newly allocated direct ByteBuffer
     * @throws MemoryBackPressureException if allocation cannot be satisfied
     */
    public ByteBuffer allocate(int size) throws MemoryBackPressureException {
        long count = allocCounter.getAndIncrement();
        totalBytesRequested.add(size);
        windowedAllocations.increment();

        // --- Hard floor fast-fail with adaptive safety margin (Req 1.5) ---
        // HIGHEST PRIORITY — evaluated before all other checks.
        // Margin scales with allocation rate under high concurrency.
        // Skip until first diagnostic has run (lastHeadroomBytes defaults to maxDirect at init).
        long windowedAllocs = windowedAllocations.sum();
        long hardFloorMargin = Math.min(
                Math.max((long) blockSize * 32,
                        windowedAllocs * blockSize / Math.max(sampleInterval / 16, 1)),
                (long) (maxDirectMemoryBytes * 0.05));
        if (lastDiagnosticNanos > 0 && lastHeadroomBytes < size + hardFloorMargin) {
            throw new MemoryBackPressureException(
                    "Hard floor: headroom " + lastHeadroomBytes
                            + " < requested " + size + " + margin " + hardFloorMargin,
                    lastPressureLevel, gcDebtEma, lastHeadroomBytes,
                    allocatedByUs.get(), lastNativeUsedBytes, maxDirectMemoryBytes);
        }

        // --- Periodic diagnostic (count-based or time-based) ---
        // Both paths use tryLock — never bypass concurrency guard (Req 3.3, 9.2)
        boolean timeBased = timeSinceLastDiagnosticNanos() > maxSampleIntervalNanos;
        if ((count & sampleMask) == 0 || timeBased) {
            diagnoseAndRespondWithTryLock();
        }

        // --- Pre-check fast-fail (Req 1.3) ---
        // SECOND PRIORITY — only evaluated if hard floor did not trigger.
        // cache at floor + headroom below target + no improvement + GC not catching up
        // + external usage not spiking (don't punish ourselves for Netty/NIO)
        // Grace window: allow sampleInterval/4 allocations after first pre-check trigger
        // before actually rejecting, giving GC/Cleaner time to catch up.
        boolean cacheAtFloor = isCacheAtFloor();
        if ((cacheAtFloor || forcePreCheck) && lastHeadroomBytes < targetHeadroomBytes) {
            if (headroomNotImproved(size) && gcDebtGrowthRate >= 0
                    && !(externalUsageGrowthRate / maxDirectMemoryBytes > 0.01)) {
                preCheckFailCount++;
                if (preCheckFailCount >= sampleInterval / 4) {
                    throw new MemoryBackPressureException(
                            "Pre-check: cache at floor, no headroom improvement, GC not catching up"
                                    + " (preCheckFailCount=" + preCheckFailCount + ")",
                            lastPressureLevel, gcDebtEma, lastHeadroomBytes,
                            allocatedByUs.get(), lastNativeUsedBytes, maxDirectMemoryBytes);
                }
            } else {
                preCheckFailCount = 0;
            }
        } else {
            preCheckFailCount = 0;
        }

        // --- Allocate with OOM catch (Req 1.1, 1.2) ---
        // Tightly scoped: only ByteBuffer.allocateDirect() is inside the try block.
        ByteBuffer buf;
        long t0 = System.nanoTime();
        try {
            buf = ByteBuffer.allocateDirect(size);
        } catch (OutOfMemoryError oome) {
            throw new MemoryBackPressureException(
                    "OOM from allocateDirect(" + size + ")", oome,
                    lastPressureLevel, gcDebtEma, lastHeadroomBytes,
                    allocatedByUs.get(), lastNativeUsedBytes, maxDirectMemoryBytes);
        }
        long elapsed = System.nanoTime() - t0;

        // --- Accounting order: totalBytesAllocated FIRST, then allocatedByUs, then Cleaner ---
        // This ensures that if Cleaner fires immediately (rare edge case under GC pressure),
        // allocatedByUs + reclaimedByGc never transiently exceeds totalBytesAllocated.
        totalBytesAllocated.add(size);
        totalAllocations.increment();
        allocatedByUs.addAndGet(size);

        // Cleaner registration: fail-fast, no rollback. Native memory exists and must be tracked.
        // Catch Exception only — let Error (e.g., OutOfMemoryError) propagate naturally.
        try {
            cleaner.register(buf, new CleanerAction(size, allocatedByUs, reclaimedByGc));
        } catch (Exception e) {
            throw new IllegalStateException("Cleaner registration failed; "
                    + "AllocatedByUs will overcount by " + size + " bytes", e);
        }

        // --- Stall detection (every allocation, outside diagnostic lock) (Req 3.4) ---
        if (elapsed > stallThresholdNanos) {
            stallCount.increment();
            stallNanosTotal.add(elapsed);
            windowedStalls.increment();
        }

        return buf;
    }

    /**
     * Returns nanoseconds since the last diagnostic run, or {@code Long.MAX_VALUE}
     * if no diagnostic has run yet.
     */
    private long timeSinceLastDiagnosticNanos() {
        long last = lastDiagnosticNanos;
        if (last == 0) {
            return Long.MAX_VALUE;
        }
        return System.nanoTime() - last;
    }

    /**
     * Attempts to run {@link #diagnoseAndRespond()} under the diagnostic tryLock.
     * If the lock is already held by another thread, skips without blocking (Req 9.3).
     */
    private void diagnoseAndRespondWithTryLock() {
        if (diagnosticLock.compareAndSet(false, true)) {
            try {
                diagnoseAndRespond();
            } finally {
                diagnosticLock.set(false);
            }
        }
    }

    /**
     * Checks whether the cache is currently at its minimum capacity floor.
     * Returns {@code false} if no CacheController is registered.
     */
    private boolean isCacheAtFloor() {
        CacheController cc = cacheController;
        if (cc == null) {
            return false;
        }
        long currentMax = cc.currentMaxBlocks();
        long originalMax = cc.originalMaxBlocks();
        return originalMax > 0 && currentMax <= (long) (originalMax * minCacheFraction);
    }

    /**
     * Checks whether headroom has NOT improved by at least
     * {@code max(blockSize, targetHeadroomBytes * 0.01)} since the previous
     * diagnostic sample. Used by the pre-check fast-fail path.
     *
     * @param requestedSize the size of the current allocation request
     * @return true if headroom has not meaningfully improved
     */
    private boolean headroomNotImproved(int requestedSize) {
        long improvementThreshold = Math.max(blockSize, (long) (targetHeadroomBytes * 0.01));
        // lastHeadroomBytes is the most recent sampled headroom.
        // If headroom hasn't grown by at least the threshold, it hasn't improved.
        // We compare against targetHeadroomBytes as the baseline — if we're still
        // below target by more than the threshold, headroom hasn't improved enough.
        return lastHeadroomBytes < targetHeadroomBytes - improvementThreshold;
    }

    // ======================== Diagnostic core (stubs) ========================

    /**
     * Runs the adaptive diagnostic routine. Samples MXBean state, computes
     * GC debt and pressure, and takes tiered corrective action.
     *
     * <p>Called from within {@link #diagnoseAndRespondWithTryLock()} — the
     * diagnostic lock is already held by the caller. This method does NOT
     * acquire the lock itself.
     *
     * <h3>Steps</h3>
     * <ol>
     *   <li>Sample raw state from MXBean and CacheController</li>
     *   <li>Compute derived signals (GC debt, external usage)</li>
     *   <li>Update EMAs (gcDebtEma, externalUsageEma, growth rates, deficitEma)</li>
     *   <li>Compute adaptive headroom target and deficit</li>
     *   <li>Reset windowed stall/allocation counters</li>
     *   <li>Store all computed values to volatile fields</li>
     *   <li>Execute tiered decision logic (Tier 1/2/3 or restore)</li>
     * </ol>
     */
    void diagnoseAndRespond() {
        // NOTE: Lock is already held by caller (diagnoseAndRespondWithTryLock)

        // 1. Sample raw state
        if (directPool == null) {
            return; // MXBean unavailable
        }
        long nativeUsed = directPool.getMemoryUsed();
        if (nativeUsed < 0) {
            return; // MXBean unavailable
        }
        CacheController cc = cacheController;
        long cacheHeld = (cc != null) ? cc.cacheHeldBytes() : 0;
        long allocByUs = allocatedByUs.get();

        // 2. Compute derived signals
        long gcDebt = Math.max(0, allocByUs - cacheHeld);
        long externalUsage = nativeUsed - allocByUs;
        if (externalUsage < 0) {
            LOG.warning("Counter drift: external=" + externalUsage);
            driftDetectedCount.increment();
            externalUsage = 0;
        }

        // 3. Update EMAs
        double alpha = emaAlpha;
        gcDebtEma = alpha * gcDebt + (1 - alpha) * gcDebtEma;
        externalUsageEma = alpha * externalUsage + (1 - alpha) * externalUsageEma;
        gcDebtGrowthRate = alpha * (gcDebt - prevGcDebt) + (1 - alpha) * gcDebtGrowthRate;
        prevGcDebt = gcDebt;
        externalUsageGrowthRate = alpha * (externalUsage - prevExternalUsage) + (1 - alpha) * externalUsageGrowthRate;
        prevExternalUsage = externalUsage;

        // 4. Compute adaptive headroom target
        long safetyMargin = Math.max((long) blockSize * 32, (long) (nativeUsed * 0.02));
        long targetHeadroom = Math.max(
                (long) (gcDebtEma * safetyMultiplier) + safetyMargin,
                (long) (maxDirectMemoryBytes * minHeadroomFraction));
        long headroom = maxDirectMemoryBytes - nativeUsed;

        // 5. Compute deficit EMA
        long rawDeficit = Math.max(0, targetHeadroom - headroom);
        deficitEma = alpha * rawDeficit + (1 - alpha) * deficitEma;

        // 6. Reset windowed counters (atomically read and reset)
        long wStalls = windowedStalls.sumThenReset();
        long wAllocs = windowedAllocations.sumThenReset();

        // 6b. Compute stall ratio and update stall window tracking (Req 10.1, 10.2)
        double windowedStallRatio = (double) wStalls / Math.max(wAllocs, 1);
        if (windowedStallRatio > stallRatioThreshold) {
            consecutiveStallWindows++;
        } else {
            consecutiveStallWindows = 0;
        }

        // Stall override: require consecutive windows to filter noise (Req 10.2)
        // All three conditions must hold to prevent false escalation
        boolean stallOverride = consecutiveStallWindows >= 2
                && gcDebtGrowthRate > 0
                && gcDebtEma > (long) blockSize * 32;

        // 7. Cleaner lag approximation (observability metric only) (Req 4.4, 4.5)
        cleanerLagApprox = Math.max(0, allocByUs - nativeUsed);
        nativeGap = Math.max(0, nativeUsed - cacheHeld);

        // 7b. Lyapunov monitoring (observability, not control) (Req 12.1)
        // prev* values are the state BEFORE this sample's EMA updates
        // V uses squared terms for formal positive-definiteness
        // Relaxed check: allow small noise (1 + 0.05), track rolling violations
        {
            double a = 1.0, b = 0.5, c = 0.1;
            long cacheSize = (cc != null) ? cc.currentMaxBlocks() : 0;
            long original = (cc != null) ? cc.originalMaxBlocks() : 0;
            double cacheDeviation = Math.abs(cacheSize - original);

            double vPrev = a * prevDeficitEma * prevDeficitEma
                    + b * prevGcDebtEma * prevGcDebtEma
                    + c * prevCacheDeviation * prevCacheDeviation;
            double vNext = a * deficitEma * deficitEma
                    + b * gcDebtEma * gcDebtEma
                    + c * cacheDeviation * cacheDeviation;

            if (vNext > vPrev * 1.05) {
                lyapunovViolationCount++;
            } else {
                lyapunovViolationCount = Math.max(0, lyapunovViolationCount - 1);
            }

            // Save current state as prev for next sample
            prevDeficitEma = deficitEma;
            prevGcDebtEma = gcDebtEma;
            prevCacheDeviation = cacheDeviation;
        }

        // 8. Update observable state (store to volatile fields)
        lastDiagnosticNanos = System.nanoTime();
        lastGcDebtBytes = gcDebt;
        lastNativeUsedBytes = nativeUsed;
        lastCacheHeldBytes = cacheHeld;
        lastHeadroomBytes = headroom;
        targetHeadroomBytes = targetHeadroom;
        lastExternalUsage = externalUsage;

        // 9. Decision — SINGLE CONTROL LAW based on pressure only
        //    stallOverride prevents taking the restore/no-op path when stalls indicate real pressure
        if (headroom >= targetHeadroom && !stallOverride) {
            if (isCacheShrunk && cc != null) {
                gradualRestore(cc, targetHeadroom);
            } else {
                tier1NoActionCount.increment();
                lastAction = LastAction.NONE;
            }
            lastPressureLevel = 0.0;
            return;
        }

        double pressure = deficitEma / Math.max(targetHeadroom, 1);

        // Stall-based escalation: elevate effective pressure when MXBean may be stale (Req 10.4)
        if (stallOverride && headroom >= targetHeadroom) {
            LOG.warning("Stall-based escalation: MXBean may be stale — "
                    + "consecutiveStallWindows=" + consecutiveStallWindows
                    + ", gcDebtGrowthRate=" + String.format("%.2f", gcDebtGrowthRate)
                    + ", gcDebtEma=" + String.format("%.0f", gcDebtEma));
        }
        if (stallOverride) {
            pressure = Math.max(pressure, 0.3);
        }

        lastPressureLevel = pressure;

        // Tier 1: Low pressure (stallOverride already elevated pressure above 0.3)
        if (pressure < 0.3) {
            tier1NoActionCount.increment();
            lastAction = LastAction.NONE;
        }
        // Tier 2: Moderate pressure — GC hint (if effective and enabled)
        else if (pressure < 0.8) {
            tier2ModeratePressureCount.increment();
            // Skip GC hint if GC is already catching up AND debt is small (Req 2.3)
            if (gcDebtGrowthRate < 0 && gcDebtEma < (long) blockSize * 32) {
                lastAction = LastAction.NONE; // GC working, debt tiny — no action needed
            } else if (tryGcHint()) {
                gcHintModeratePressure.increment();
                lastAction = LastAction.GC_HINT;
            } else {
                lastAction = LastAction.NONE; // GC disabled or cooldown
            }
        }
        // Tier 3: High pressure — proportional cache shrink
        else {
            tier3LastResortCount.increment();
            // GC disabled + cache at floor → early backpressure (Req 2.4)
            boolean cacheAtFloor = isCacheAtFloor();
            if (systemGcDisabled && cacheAtFloor) {
                forcePreCheck = true;
                lastAction = LastAction.NONE;
            }
            // Two-phase lag detection: distinguish real pressure from Cleaner lag (Req 4.4, 4.5)
            // gcDebt = allocatedByUs - cacheHeld (our counter says memory is outstanding)
            // nativeGap = nativeUsed - cacheHeld (OS says memory is outstanding)
            // If gcDebt high but nativeGap low → Cleaner lag (native already freed, counter lagging)
            // If both high → real pressure (native memory genuinely held)
            else if (gcDebtGrowthRate <= 0 && gcDebt > nativeGap * 1.5) {
                // gcDebt >> nativeGap → Cleaner lag is dominant, not real pressure
                boolean hintFired = tryGcHint(); // may help process Cleaner queue
                lastAction = hintFired ? LastAction.GC_HINT : LastAction.NONE;
            } else if (cc != null && canShrink()) {
                // Delay shrink if external usage is spiking OR already large (Req 5.1, 5.4)
                // External pressure — shrinking our cache won't help
                if (externalUsageGrowthRate / maxDirectMemoryBytes > 0.01
                        || externalUsageEma / maxDirectMemoryBytes > 0.3) {
                    lastAction = LastAction.NONE;
                } else {
                    boolean hintFired = tryGcHint();
                    if (hintFired) {
                        gcHintWithShrink.increment();
                    }
                    double shrinkFactor = Math.min(0.3, 0.5 * (deficitEma / Math.max(targetHeadroom, 1)));
                    long currentMax = cc.currentMaxBlocks();
                    long floor = (long) (cc.originalMaxBlocks() * minCacheFraction);
                    long newMax = Math.max((long) (currentMax * (1 - shrinkFactor)), floor);
                    if (newMax < currentMax) {
                        totalBlocksEvicted.add(currentMax - newMax);
                        cacheShrinkBlocksTotal.add(currentMax - newMax);
                        cc.setMaxBlocks(newMax);
                        cc.cleanUp();
                        cacheShrinkCount.increment();
                        isCacheShrunk = true;
                        lastShrinkNanos = System.nanoTime();
                        gcDebtAtLastShrink = gcDebt;
                        gcDebtEmaAtLastShrink = gcDebtEma;
                        deficitAtLastShrink = (long) deficitEma;
                    }
                    lastAction = LastAction.CACHE_SHRINK;
                }
            } else {
                forcePreCheck = true;
                lastAction = LastAction.NONE;
            }
        }
    }

    /**
     * Attempts to call {@code System.gc()} if cooldown has elapsed and
     * GC hints are not disabled.
     *
     * <p>Skips the hint if:
     * <ul>
     *   <li>{@code -XX:+DisableExplicitGC} was detected at construction (Req 11.2)</li>
     *   <li>GC is already catching up effectively: {@code gcDebtEma > 0 AND
     *       gcDebtGrowthRate < -gcDebtEma × 0.01} (Req 16.3)</li>
     *   <li>Cooldown has not elapsed since the last hint (Req 16.1, 16.2)</li>
     * </ul>
     *
     * @return true if the hint was fired, false if skipped
     */
    boolean tryGcHint() {
        // Req 11.2: System.gc() disabled — no hint possible
        if (systemGcDisabled) {
            return false;
        }
        // Req 16.3: Skip if GC is already catching up effectively
        // (relative to current debt level)
        if (gcDebtEma > 0 && gcDebtGrowthRate < -gcDebtEma * 0.01) {
            return false;
        }
        // Req 16.1, 16.2: Enforce cooldown between GC hints
        long now = System.nanoTime();
        if (now - lastGcHintNanos < gcHintCooldownNanos) {
            gcHintSkippedCooldown.increment();
            return false;
        }
        System.gc();
        lastGcHintNanos = now;
        gcHintCount.increment();
        return true;
    }

    /**
     * Checks whether a cache shrink is allowed based on cooldown,
     * spike detection, and hysteresis.
     *
     * <p>Three guards prevent over-shrinking:
     * <ol>
     *   <li><b>Spike check:</b> If GC debt EMA has spiked significantly since
     *       the last shrink (more than 1.5× the EMA at last shrink plus a
     *       tolerance of {@code blockSize × 32}), the previous shrink's
     *       evictions have not yet been reclaimed — block further shrinks.</li>
     *   <li><b>Cooldown:</b> Enforce a minimum interval ({@code shrinkCooldownNanos})
     *       between consecutive shrinks to let GC process evicted buffers.</li>
     *   <li><b>Hysteresis:</b> If the deficit EMA has improved to below 80% of
     *       its value at the last shrink, the previous action is already working
     *       — skip the shrink to prevent over-correction.</li>
     * </ol>
     *
     * @return true if shrink is allowed (Req 17.4)
     */
    boolean canShrink() {
        long now = System.nanoTime();

        // Spike check: skip if no prior shrink has occurred (gcDebtAtLastShrink == 0)
        if (gcDebtAtLastShrink > 0) {
            if (gcDebtEma > gcDebtEmaAtLastShrink * 1.5 + (long) blockSize * 32) {
                return false;
            }
        }

        // Cooldown check
        if (now - lastShrinkNanos < shrinkCooldownNanos) {
            return false;
        }

        // Hysteresis: if deficitEma is improving (< 80% of previous), skip shrink
        if (deficitAtLastShrink > 0 && deficitEma < deficitAtLastShrink * 0.8) {
            return false;
        }

        // Cooldown elapsed and deficit not improving — allow shrink
        return true;
    }

    /**
     * Gradually restores cache capacity toward {@code originalMaxBlocks}
     * using proportional restore. The restore fraction is proportional to
     * how much headroom exceeds the target, capped at 20% per step to
     * prevent asymmetric oscillation with the shrink dynamics.
     *
     * <p>When the cache is fully restored ({@code newMax >= original}),
     * clears the {@code isCacheShrunk} flag so future diagnostics skip
     * the restore path.
     *
     * @param cc              the cache controller
     * @param targetHeadroom  the current target headroom in bytes
     */
    void gradualRestore(CacheController cc, long targetHeadroom) {
        long current = cc.currentMaxBlocks();
        long original = cc.originalMaxBlocks();
        if (current >= original) {
            isCacheShrunk = false;
            lastAction = LastAction.NONE;
            return;
        }
        // Proportional restore: matches shrink dynamics to prevent asymmetric oscillation
        double restoreFraction = Math.min(0.2, (double) lastHeadroomBytes / Math.max(targetHeadroom, 1));
        long restoreBlocks = Math.max(1, (long) (restoreFraction * (original - current)));
        long newMax = Math.min(original, current + restoreBlocks);
        cc.setMaxBlocks(newMax);
        cacheRestoreCount.increment();
        cacheRestoreBlocksTotal.add(newMax - current);
        lastAction = LastAction.CACHE_RESTORE;
        if (newMax >= original) {
            isCacheShrunk = false;
        }
    }

    // ======================== Recommended sizing ========================

    /**
     * Returns the recommended max blocks for the given block size, accounting
     * for minimum headroom and external usage EMA.
     *
     * <p>STUB: Task 2.7 fills in the full implementation.
     *
     * @param blockSizeParam the block size in bytes
     * @return recommended max blocks
     */
    public long getRecommendedMaxBlocks(int blockSizeParam) {
        long minHeadroom = (long) (maxDirectMemoryBytes * minHeadroomFraction);
        long externalBudget = Math.max((long) externalUsageEma, baselineExternalUsage);
        long available = maxDirectMemoryBytes - minHeadroom - externalBudget;
        return Math.max(1, available / blockSizeParam);
    }

    // ======================== Diagnostic state getters ========================

    public double getGcDebtEma() {
        return gcDebtEma;
    }

    public long getTargetHeadroomBytes() {
        return targetHeadroomBytes;
    }

    public long getLastHeadroomBytes() {
        return lastHeadroomBytes;
    }

    public double getLastPressureLevel() {
        return lastPressureLevel;
    }

    public long getAllocatedByUs() {
        return allocatedByUs.get();
    }

    public long getReclaimedByGc() {
        return reclaimedByGc.sum();
    }

    public long getLastNativeUsedBytes() {
        return lastNativeUsedBytes;
    }

    public long getLastExternalUsage() {
        return lastExternalUsage;
    }

    public double getExternalUsageEma() {
        return externalUsageEma;
    }

    public double getGcDebtGrowthRate() {
        return gcDebtGrowthRate;
    }

    public long getTimeSinceLastGcHintMs() {
        long last = lastGcHintNanos;
        return last == 0 ? -1 : (System.nanoTime() - last) / 1_000_000L;
    }

    public LastAction getLastAction() {
        return lastAction;
    }

    public boolean isCacheShrunk() {
        return isCacheShrunk;
    }

    public long getLastGcDebtBytes() {
        return lastGcDebtBytes;
    }

    public long getLastCacheHeldBytes() {
        return lastCacheHeldBytes;
    }

    public double getDeficitEma() {
        return deficitEma;
    }

    public double getExternalUsageGrowthRate() {
        return externalUsageGrowthRate;
    }

    public long getCleanerLagApprox() {
        return cleanerLagApprox;
    }

    public long getNativeGap() {
        return nativeGap;
    }

    public int getConsecutiveStallWindows() {
        return consecutiveStallWindows;
    }

    public int getLyapunovViolationCount() {
        return lyapunovViolationCount;
    }

    // ======================== Metric counter getters ========================

    public long getTotalAllocations() {
        return totalAllocations.sum();
    }

    public long getTotalBytesAllocated() {
        return totalBytesAllocated.sum();
    }

    public long getTotalBytesRequested() {
        return totalBytesRequested.sum();
    }

    public long getStallCount() {
        return stallCount.sum();
    }

    public long getStallNanosTotal() {
        return stallNanosTotal.sum();
    }

    public long getGcHintCount() {
        return gcHintCount.sum();
    }

    public long getGcHintBecauseDebt() {
        return gcHintModeratePressure.sum();
    }

    public long getGcHintWithShrink() {
        return gcHintWithShrink.sum();
    }

    public long getGcHintSkippedCooldown() {
        return gcHintSkippedCooldown.sum();
    }

    public long getCacheShrinkCount() {
        return cacheShrinkCount.sum();
    }

    public long getCacheRestoreCount() {
        return cacheRestoreCount.sum();
    }

    public long getTotalBlocksEvicted() {
        return totalBlocksEvicted.sum();
    }

    public long getTier1NoActionCount() {
        return tier1NoActionCount.sum();
    }

    public long getTier3LastResortCount() {
        return tier3LastResortCount.sum();
    }

    public long getDriftDetectedCount() {
        return driftDetectedCount.sum();
    }

    public long getTier2ModeratePressureCount() {
        return tier2ModeratePressureCount.sum();
    }

    public long getCacheShrinkBlocksTotal() {
        return cacheShrinkBlocksTotal.sum();
    }

    public long getCacheRestoreBlocksTotal() {
        return cacheRestoreBlocksTotal.sum();
    }

    // ======================== Snapshot ========================

    /** Captures a point-in-time snapshot of all diagnostic and metric state. */
    public DiagnosticSnapshot snapshot() {
        return new DiagnosticSnapshot(
            System.currentTimeMillis(),
            getTotalAllocations(), getTotalBytesAllocated(), getAllocatedByUs(), getReclaimedByGc(),
            getLastPressureLevel(), getGcDebtEma(), getDeficitEma(), getCleanerLagApprox(),
            getTargetHeadroomBytes(), getLastHeadroomBytes(),
            getStallCount(), getStallNanosTotal(),
            getGcHintCount(), getGcHintSkippedCooldown(),
            getCacheShrinkCount(), getCacheShrinkBlocksTotal(),
            getCacheRestoreCount(), getCacheRestoreBlocksTotal(),
            getTotalBlocksEvicted(),
            getTier1NoActionCount(), getTier2ModeratePressureCount(), getTier3LastResortCount(),
            getLastNativeUsedBytes(), getLastExternalUsage(), getExternalUsageEma(),
            getConsecutiveStallWindows(), getLyapunovViolationCount(),
            getLastAction()
        );
    }

    // ======================== Configuration getters ========================

    public long getMaxDirectMemoryBytes() {
        return maxDirectMemoryBytes;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getSampleInterval() {
        return sampleInterval;
    }

    public double getEmaAlpha() {
        return emaAlpha;
    }

    public double getSafetyMultiplier() {
        return safetyMultiplier;
    }

    public double getMinCacheFraction() {
        return minCacheFraction;
    }

    public double getMinHeadroomFraction() {
        return minHeadroomFraction;
    }

    public boolean isSystemGcDisabled() {
        return systemGcDisabled;
    }

    public long getStallThresholdNanos() {
        return stallThresholdNanos;
    }

    public double getStallRatioThreshold() {
        return stallRatioThreshold;
    }

    // ======================== Reset ========================

    /**
     * Resets all LongAdder-based operational counters to zero.
     * Does NOT reset live accounting state ({@code allocatedByUs},
     * {@code reclaimedByGc}, {@code allocCounter}) as these track
     * real-time memory ownership and must remain accurate.
     */
    public void resetMetrics() {
        totalAllocations.reset();
        totalBytesAllocated.reset();
        totalBytesRequested.reset();
        stallCount.reset();
        stallNanosTotal.reset();
        windowedStalls.reset();
        windowedAllocations.reset();
        gcHintCount.reset();
        gcHintModeratePressure.reset();
        gcHintWithShrink.reset();
        gcHintSkippedCooldown.reset();
        cacheShrinkCount.reset();
        cacheRestoreCount.reset();
        totalBlocksEvicted.reset();
        tier1NoActionCount.reset();
        tier2ModeratePressureCount.reset();
        tier3LastResortCount.reset();
        driftDetectedCount.reset();
        cacheShrinkBlocksTotal.reset();
        cacheRestoreBlocksTotal.reset();
    }

    // ======================== Internal helpers ========================

    /**
     * Finds the "direct" BufferPoolMXBean. Returns null if not found
     * (diagnostic will be disabled).
     */
    private static BufferPoolMXBean findDirectPool() {
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if ("direct".equals(pool.getName())) {
                return pool;
            }
        }
        LOG.warning("BufferPoolMXBean 'direct' not found; adaptive diagnostics disabled");
        return null;
    }

    /**
     * Best-effort detection of {@code -XX:+DisableExplicitGC} via
     * {@link java.lang.management.RuntimeMXBean#getInputArguments()}.
     *
     * @return true if the flag is detected, false otherwise (safe default)
     */
    private static boolean detectSystemGcDisabled() {
        try {
            List<String> args = ManagementFactory.getRuntimeMXBean().getInputArguments();
            return args.stream().anyMatch(a -> a.contains("-XX:+DisableExplicitGC"));
        } catch (Exception e) {
            // Safe default: assume gc hints are enabled
            return false;
        }
    }

    /**
     * Samples the current native direct memory usage as the baseline
     * external usage before any cache allocations.
     */
    private long sampleBaselineExternalUsage() {
        if (directPool != null) {
            long used = directPool.getMemoryUsed();
            return Math.max(0, used);
        }
        return 0;
    }

    /**
     * Resolves the JVM's max direct memory size via {@code sun.misc.VM.maxDirectMemory()}.
     * Falls back to parsing {@code -XX:MaxDirectMemorySize} from JVM args (JDK 25+ removed
     * {@code sun.misc.VM}), then to {@code Runtime.getRuntime().maxMemory()}.
     */
    static long resolveMaxDirectMemory() {
        try {
            Class<?> vmClass = Class.forName("sun.misc.VM");
            java.lang.reflect.Method method = vmClass.getDeclaredMethod("maxDirectMemory");
            long result = (long) method.invoke(null);
            if (result > 0) {
                return result;
            }
        } catch (Exception ignored) {
            // JDK 25+ removed sun.misc.VM — parse from JVM input arguments
        }
        for (String arg : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            if (arg.startsWith("-XX:MaxDirectMemorySize=")) {
                String val = arg.substring("-XX:MaxDirectMemorySize=".length()).trim().toLowerCase();
                long multiplier = 1;
                if (val.endsWith("g")) { multiplier = 1024L * 1024 * 1024; val = val.substring(0, val.length() - 1); }
                else if (val.endsWith("m")) { multiplier = 1024L * 1024; val = val.substring(0, val.length() - 1); }
                else if (val.endsWith("k")) { multiplier = 1024L; val = val.substring(0, val.length() - 1); }
                return Long.parseLong(val) * multiplier;
            }
        }
        return Runtime.getRuntime().maxMemory();
    }
}
