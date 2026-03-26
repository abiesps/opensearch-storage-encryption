/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Gates {@code ByteBuffer.allocateDirect()} calls with soft/hard thresholds
 * to prevent {@code OutOfMemoryError} under heavy allocation churn.
 *
 * <p>Tracks outstanding direct memory via a self-maintained {@link AtomicLong}
 * counter (no syscall on the hot path). Periodically cross-checks against
 * {@link BufferPoolMXBean} to correct drift.
 *
 * <ul>
 *   <li>Soft threshold (default 0.85): triggers async {@code System.gc()}, allocation proceeds.</li>
 *   <li>Hard threshold (default 0.95): blocks caller, triggers {@code System.gc()},
 *       waits up to {@code hardTimeoutMs} for utilization to drop. Throws
 *       {@link DirectMemoryExhaustedException} on timeout.</li>
 * </ul>
 *
 * @deprecated Use {@link DirectMemoryAllocator} instead. The new allocator provides
 *             Cleaner-based tracking, adaptive tiered pressure response (GC hint,
 *             proportional cache shrink, gradual restore), and richer diagnostics.
 *             This class is retained for backward compatibility during migration.
 */
@Deprecated(forRemoval = true)
public final class DirectMemoryAdmissionController {

    private static final Logger LOG = Logger.getLogger(DirectMemoryAdmissionController.class.getName());

    /** How often (in allocations) to cross-check against BufferPoolMXBean. */
    static final long CROSS_CHECK_INTERVAL = 1000;

    /** Tolerance for drift correction — only correct if drift exceeds 10%. */
    private static final double DRIFT_TOLERANCE = 0.10;

    /** Polling interval while blocked at the hard threshold. */
    private static final long HARD_POLL_INTERVAL_MS = 50;

    private final AtomicLong outstandingBytes = new AtomicLong(0);
    private final AtomicLong crossCheckCounter = new AtomicLong(0);
    private final long maxDirectMemory;
    private final double softThreshold;
    private final double hardThreshold;
    private final long hardTimeoutMs;

    /**
     * Creates a controller with the specified thresholds.
     *
     * @param maxDirectMemory  max direct memory in bytes (from {@code -XX:MaxDirectMemorySize})
     * @param softThreshold    utilization ratio triggering async GC (default 0.85)
     * @param hardThreshold    utilization ratio triggering blocking GC (default 0.95)
     * @param hardTimeoutMs    max ms to block at hard threshold before throwing (default 5000)
     */
    public DirectMemoryAdmissionController(long maxDirectMemory, double softThreshold, double hardThreshold, long hardTimeoutMs) {
        if (maxDirectMemory <= 0) {
            throw new IllegalArgumentException("maxDirectMemory must be positive, got: " + maxDirectMemory);
        }
        if (softThreshold <= 0 || softThreshold >= 1.0) {
            throw new IllegalArgumentException("softThreshold must be in (0, 1), got: " + softThreshold);
        }
        if (hardThreshold <= softThreshold || hardThreshold > 1.0) {
            throw new IllegalArgumentException("hardThreshold must be in (softThreshold, 1], got: " + hardThreshold);
        }
        if (hardTimeoutMs <= 0) {
            throw new IllegalArgumentException("hardTimeoutMs must be positive, got: " + hardTimeoutMs);
        }
        this.maxDirectMemory = maxDirectMemory;
        this.softThreshold = softThreshold;
        this.hardThreshold = hardThreshold;
        this.hardTimeoutMs = hardTimeoutMs;
    }

    /**
     * Creates a controller with default thresholds using the JVM's max direct memory.
     */
    public DirectMemoryAdmissionController() {
        this(resolveMaxDirectMemory(), 0.85, 0.95, 5000);
    }

    /**
     * Acquires permission to allocate {@code bytes} of direct memory.
     *
     * <ol>
     *   <li>Increments outstandingBytes by {@code bytes}.</li>
     *   <li>Computes utilization = outstandingBytes / maxDirectMemory.</li>
     *   <li>If utilization &gt; hardThreshold: blocks, calls {@code System.gc()},
     *       waits up to hardTimeoutMs. Throws {@link DirectMemoryExhaustedException} on timeout.</li>
     *   <li>If utilization &gt; softThreshold: calls {@code System.gc()} asynchronously, proceeds.</li>
     *   <li>Periodically cross-checks against BufferPoolMXBean and corrects drift.</li>
     * </ol>
     *
     * @param bytes number of bytes to acquire
     */
    public void acquire(int bytes) {
        long current = outstandingBytes.addAndGet(bytes);
        long count = crossCheckCounter.incrementAndGet();

        // Periodic cross-check against BufferPoolMXBean
        if (count % CROSS_CHECK_INTERVAL == 0) {
            crossCheckWithMXBean();
            current = outstandingBytes.get();
        }

        double utilization = (double) current / maxDirectMemory;

        if (utilization > hardThreshold) {
            blockUntilBelowHardThreshold(bytes);
        } else if (utilization > softThreshold) {
            // Async GC hint — non-blocking
            System.gc();
        }
    }

    /**
     * Releases {@code bytes} of direct memory (called on eviction or load failure).
     *
     * @param bytes number of bytes to release
     */
    public void release(int bytes) {
        long result = outstandingBytes.addAndGet(-bytes);
        if (result < 0) {
            LOG.warning("outstandingBytes went negative (" + result + "); will be corrected on next cross-check");
        }
    }

    /**
     * Returns the current outstanding bytes tracked by this controller.
     */
    public long getOutstandingBytes() {
        return outstandingBytes.get();
    }

    /**
     * Returns the configured max direct memory.
     */
    public long getMaxDirectMemory() {
        return maxDirectMemory;
    }

    /**
     * Returns the current utilization ratio.
     */
    public double getUtilization() {
        return (double) outstandingBytes.get() / maxDirectMemory;
    }

    /**
     * Returns the configured soft threshold.
     */
    public double getSoftThreshold() {
        return softThreshold;
    }

    /**
     * Returns the configured hard threshold.
     */
    public double getHardThreshold() {
        return hardThreshold;
    }

    /**
     * Returns the configured hard timeout in milliseconds.
     */
    public long getHardTimeoutMs() {
        return hardTimeoutMs;
    }

    // ---- internal ----

    /**
     * Blocks the calling thread, triggers GC, and polls until utilization drops
     * below the hard threshold or the timeout expires.
     */
    private void blockUntilBelowHardThreshold(int requestedBytes) {
        System.gc();
        long deadline = System.nanoTime() + hardTimeoutMs * 1_000_000L;

        while (true) {
            long current = outstandingBytes.get();
            double utilization = (double) current / maxDirectMemory;
            if (utilization <= hardThreshold) {
                return; // utilization dropped — proceed
            }

            if (System.nanoTime() >= deadline) {
                // Timeout — roll back the allocation and throw
                outstandingBytes.addAndGet(-requestedBytes);
                throw new DirectMemoryExhaustedException(
                    "Direct memory exhausted: utilization="
                        + String.format("%.2f", utilization)
                        + " exceeds hard threshold="
                        + hardThreshold
                        + " after "
                        + hardTimeoutMs
                        + "ms timeout (outstanding="
                        + current
                        + ", max="
                        + maxDirectMemory
                        + ")",
                    utilization,
                    current,
                    maxDirectMemory
                );
            }

            try {
                Thread.sleep(HARD_POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outstandingBytes.addAndGet(-requestedBytes);
                throw new DirectMemoryExhaustedException(
                    "Interrupted while waiting for direct memory to drop below hard threshold",
                    utilization,
                    current,
                    maxDirectMemory
                );
            }
        }
    }

    /**
     * Cross-checks the self-tracked counter against BufferPoolMXBean and corrects
     * drift if it exceeds the tolerance.
     */
    private void crossCheckWithMXBean() {
        try {
            List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
            for (BufferPoolMXBean pool : pools) {
                if ("direct".equals(pool.getName())) {
                    long mxBeanUsed = pool.getMemoryUsed();
                    if (mxBeanUsed < 0) {
                        return; // MXBean not available
                    }
                    long tracked = outstandingBytes.get();
                    long drift = Math.abs(tracked - mxBeanUsed);
                    double driftRatio = (double) drift / maxDirectMemory;
                    if (driftRatio > DRIFT_TOLERANCE) {
                        LOG.warning(
                            "Direct memory drift detected: tracked="
                                + tracked
                                + ", mxBean="
                                + mxBeanUsed
                                + ", drift="
                                + String.format("%.1f%%", driftRatio * 100)
                        );
                        outstandingBytes.set(mxBeanUsed);
                    }
                    return;
                }
            }
        } catch (Exception e) {
            LOG.fine("BufferPoolMXBean cross-check failed: " + e.getMessage());
        }
    }

    /**
     * Resolves the JVM's max direct memory size.
     * Tries {@code sun.misc.VM.maxDirectMemory()} first, then falls back to
     * BufferPoolMXBean, then to {@code Runtime.getRuntime().maxMemory()}.
     */
    static long resolveMaxDirectMemory() {
        // Try sun.misc.VM.maxDirectMemory() via reflection
        try {
            Class<?> vmClass = Class.forName("sun.misc.VM");
            java.lang.reflect.Method method = vmClass.getDeclaredMethod("maxDirectMemory");
            long result = (long) method.invoke(null);
            if (result > 0) {
                return result;
            }
        } catch (Exception ignored) {
            // Not available — fall through
        }

        // Fallback: use Runtime.maxMemory() as a reasonable approximation
        // (JVM defaults MaxDirectMemorySize to -Xmx if not explicitly set)
        return Runtime.getRuntime().maxMemory();
    }
}
