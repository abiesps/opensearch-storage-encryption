/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

/**
 * Checked exception thrown when the allocator cannot satisfy a direct memory
 * allocation after all corrective actions. Callers handle based on operation
 * criticality.
 *
 * <p>Diagnostic fields capture the allocator's state at the time of failure,
 * enabling callers to make informed degradation decisions (skip read-ahead,
 * reduce batch size, serve from disk).
 *
 * @see DirectMemoryAllocator
 */
public class MemoryBackPressureException extends Exception {

    private final double pressureLevel;
    private final double gcDebtEma;
    private final long headroomBytes;
    private final long allocatedByUs;
    private final long nativeUsedBytes;
    private final long maxDirectMemoryBytes;

    /**
     * Constructs a new exception with a cause, typically wrapping an
     * {@link OutOfMemoryError} caught from {@code ByteBuffer.allocateDirect()}.
     *
     * @param message             human-readable description
     * @param cause               the underlying cause (usually OOM), may be {@code null}
     * @param pressureLevel       current pressure ratio in [0.0, 1.0+]
     * @param gcDebtEma           smoothed GC debt in bytes
     * @param headroomBytes       available headroom at time of failure
     * @param allocatedByUs       bytes currently outstanding from this allocator
     * @param nativeUsedBytes     total native direct memory in use (from MXBean)
     * @param maxDirectMemoryBytes JVM max direct memory limit
     */
    public MemoryBackPressureException(String message, Throwable cause,
            double pressureLevel, double gcDebtEma, long headroomBytes,
            long allocatedByUs, long nativeUsedBytes, long maxDirectMemoryBytes) {
        super(message, cause);
        this.pressureLevel = pressureLevel;
        this.gcDebtEma = gcDebtEma;
        this.headroomBytes = headroomBytes;
        this.allocatedByUs = allocatedByUs;
        this.nativeUsedBytes = nativeUsedBytes;
        this.maxDirectMemoryBytes = maxDirectMemoryBytes;
    }

    /**
     * Constructs a new exception without a cause, used for pre-check fast-fail
     * when the allocator determines allocation would fail based on current
     * pressure signals.
     *
     * @param message             human-readable description
     * @param pressureLevel       current pressure ratio in [0.0, 1.0+]
     * @param gcDebtEma           smoothed GC debt in bytes
     * @param headroomBytes       available headroom at time of failure
     * @param allocatedByUs       bytes currently outstanding from this allocator
     * @param nativeUsedBytes     total native direct memory in use (from MXBean)
     * @param maxDirectMemoryBytes JVM max direct memory limit
     */
    public MemoryBackPressureException(String message,
            double pressureLevel, double gcDebtEma, long headroomBytes,
            long allocatedByUs, long nativeUsedBytes, long maxDirectMemoryBytes) {
        this(message, null, pressureLevel, gcDebtEma, headroomBytes,
             allocatedByUs, nativeUsedBytes, maxDirectMemoryBytes);
    }

    /** Returns the pressure ratio at the time of failure. */
    public double pressureLevel()       { return pressureLevel; }

    /** Returns the smoothed GC debt in bytes. */
    public double gcDebtEma()           { return gcDebtEma; }

    /** Returns the available headroom in bytes at the time of failure. */
    public long headroomBytes()         { return headroomBytes; }

    /** Returns the bytes currently outstanding from this allocator. */
    public long allocatedByUs()         { return allocatedByUs; }

    /** Returns the total native direct memory in use (from MXBean). */
    public long nativeUsedBytes()       { return nativeUsedBytes; }

    /** Returns the JVM max direct memory limit. */
    public long maxDirectMemoryBytes()  { return maxDirectMemoryBytes; }
}
