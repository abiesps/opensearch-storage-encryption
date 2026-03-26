/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.LongRange;

/**
 * Property-based tests for {@link MemoryBackPressureException} field round-trip.
 *
 * <p>Validates that for any set of diagnostic values, constructing the exception
 * and reading back via getters returns the original values unchanged.
 */
class MemoryBackPressureExceptionPropertyTests {

    /**
     * Property 17: MemoryBackPressureException field round-trip (with cause).
     *
     * <p>For any set of diagnostic values, constructing the exception with a
     * cause (OOM wrap constructor) and asserting all getters return the original
     * values.
     *
     * <p><b>Validates: Requirements 13.2</b>
     *
     * @param pressureLevel    pressure ratio
     * @param gcDebtEma        smoothed GC debt
     * @param headroomBytes    available headroom
     * @param allocatedByUs    bytes outstanding from allocator
     * @param nativeUsedBytes  total native direct memory in use
     * @param maxDirectMemoryBytes JVM max direct memory limit
     */
    @Property(tries = 200)
    void withCauseConstructorPreservesAllFields(
        @ForAll @DoubleRange(min = 0.0, max = 10.0) double pressureLevel,
        @ForAll @DoubleRange(min = 0.0, max = 1e15) double gcDebtEma,
        @ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long headroomBytes,
        @ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long allocatedByUs,
        @ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long nativeUsedBytes,
        @ForAll @LongRange(min = 1, max = Long.MAX_VALUE) long maxDirectMemoryBytes
    ) {
        OutOfMemoryError cause = new OutOfMemoryError("simulated OOM");
        String message = "test back-pressure";

        MemoryBackPressureException ex = new MemoryBackPressureException(
            message, cause,
            pressureLevel, gcDebtEma, headroomBytes,
            allocatedByUs, nativeUsedBytes, maxDirectMemoryBytes
        );

        assertEquals(pressureLevel, ex.pressureLevel(),
            "pressureLevel round-trip mismatch");
        assertEquals(gcDebtEma, ex.gcDebtEma(),
            "gcDebtEma round-trip mismatch");
        assertEquals(headroomBytes, ex.headroomBytes(),
            "headroomBytes round-trip mismatch");
        assertEquals(allocatedByUs, ex.allocatedByUs(),
            "allocatedByUs round-trip mismatch");
        assertEquals(nativeUsedBytes, ex.nativeUsedBytes(),
            "nativeUsedBytes round-trip mismatch");
        assertEquals(maxDirectMemoryBytes, ex.maxDirectMemoryBytes(),
            "maxDirectMemoryBytes round-trip mismatch");
        assertEquals(message, ex.getMessage(),
            "message round-trip mismatch");
        assertSame(cause, ex.getCause(),
            "cause must be the exact OOM instance passed to constructor");
    }

    /**
     * Property 17: MemoryBackPressureException field round-trip (without cause).
     *
     * <p>For any set of diagnostic values, constructing the exception without a
     * cause (pre-check fast-fail constructor) and asserting all getters return
     * the original values, with cause being null.
     *
     * <p><b>Validates: Requirements 13.2</b>
     *
     * @param pressureLevel    pressure ratio
     * @param gcDebtEma        smoothed GC debt
     * @param headroomBytes    available headroom
     * @param allocatedByUs    bytes outstanding from allocator
     * @param nativeUsedBytes  total native direct memory in use
     * @param maxDirectMemoryBytes JVM max direct memory limit
     */
    @Property(tries = 200)
    void withoutCauseConstructorPreservesAllFieldsAndCauseIsNull(
        @ForAll @DoubleRange(min = 0.0, max = 10.0) double pressureLevel,
        @ForAll @DoubleRange(min = 0.0, max = 1e15) double gcDebtEma,
        @ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long headroomBytes,
        @ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long allocatedByUs,
        @ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long nativeUsedBytes,
        @ForAll @LongRange(min = 1, max = Long.MAX_VALUE) long maxDirectMemoryBytes
    ) {
        String message = "pre-check fast-fail";

        MemoryBackPressureException ex = new MemoryBackPressureException(
            message,
            pressureLevel, gcDebtEma, headroomBytes,
            allocatedByUs, nativeUsedBytes, maxDirectMemoryBytes
        );

        assertEquals(pressureLevel, ex.pressureLevel(),
            "pressureLevel round-trip mismatch");
        assertEquals(gcDebtEma, ex.gcDebtEma(),
            "gcDebtEma round-trip mismatch");
        assertEquals(headroomBytes, ex.headroomBytes(),
            "headroomBytes round-trip mismatch");
        assertEquals(allocatedByUs, ex.allocatedByUs(),
            "allocatedByUs round-trip mismatch");
        assertEquals(nativeUsedBytes, ex.nativeUsedBytes(),
            "nativeUsedBytes round-trip mismatch");
        assertEquals(maxDirectMemoryBytes, ex.maxDirectMemoryBytes(),
            "maxDirectMemoryBytes round-trip mismatch");
        assertEquals(message, ex.getMessage(),
            "message round-trip mismatch");
        assertNull(ex.getCause(),
            "cause must be null for the no-cause constructor");
    }
}
