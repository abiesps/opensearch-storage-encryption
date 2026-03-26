/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * Vector API (SIMD) bulk copy helpers for benchmarking.
 * Separated into its own class so the JMH bytecode generator
 * does not need to resolve Vector API types at annotation-processing time.
 */
final class VectorBulkCopy {

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final int LANE_COUNT = SPECIES.length();

    private VectorBulkCopy() {}

    /** Copy {@code len} bytes from a MemorySegment into a byte array using Vector API. */
    static void copyToArray(MemorySegment src, byte[] dst, int len) {
        int i = 0;
        for (; i + LANE_COUNT <= len; i += LANE_COUNT) {
            ByteVector.fromMemorySegment(SPECIES, src, i, java.nio.ByteOrder.nativeOrder())
                    .intoArray(dst, i);
        }
        // scalar tail
        for (; i < len; i++) {
            dst[i] = src.get(ValueLayout.JAVA_BYTE, i);
        }
    }

    /** Copy {@code len} bytes from one MemorySegment to another using Vector API. */
    static void copySegToSeg(MemorySegment src, MemorySegment dst, int len) {
        int i = 0;
        for (; i + LANE_COUNT <= len; i += LANE_COUNT) {
            ByteVector.fromMemorySegment(SPECIES, src, i, java.nio.ByteOrder.nativeOrder())
                    .intoMemorySegment(dst, i, java.nio.ByteOrder.nativeOrder());
        }
        // scalar tail
        for (; i < len; i++) {
            dst.set(ValueLayout.JAVA_BYTE, i, src.get(ValueLayout.JAVA_BYTE, i));
        }
    }
}
