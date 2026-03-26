/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

/**
 * Encodes a (vfdId, blockId) pair into a single {@code long} for use as a
 * primitive cache key. Avoids GC pressure from object-based keys on the hot
 * read path.
 *
 * <h2>Bit layout</h2>
 * <pre>
 * ┌──────────────────────────────────┬────────────────────┐
 * │         VFD_ID (42 bits)         │  Block_ID (22 bits)│
 * │  bits 63..22                     │  bits 21..0        │
 * └──────────────────────────────────┴────────────────────┘
 * </pre>
 *
 * <ul>
 *   <li>VFD_ID: 2^42 ≈ 4.4 trillion unique files (monotonic AtomicLong)</li>
 *   <li>Block_ID: 2^22 ≈ 4.2 million blocks per file</li>
 *   <li>At 8 KB blocks: 4.2M × 8 KB = 32 GB max file size</li>
 *   <li>At 4 KB blocks: 4.2M × 4 KB = 16 GB max file size</li>
 * </ul>
 *
 * <p>VFD_ID starts at 1 (0 is reserved as "invalid"). Exhaustion of the
 * 42-bit space is fatal and requires a node restart.
 */
public final class VirtualPage {

    /** Number of bits allocated to the block ID in the lower portion. */
    static final int BLOCK_ID_BITS = 22;

    /** Bitmask for extracting the block ID from an encoded key. */
    static final long BLOCK_ID_MASK = (1L << BLOCK_ID_BITS) - 1; // 0x3FFFFF

    /** Maximum valid VFD ID (2^42 - 1). */
    public static final long MAX_VFD_ID = (1L << 42) - 1;

    /** Maximum valid block ID (2^22 - 1). */
    public static final int MAX_BLOCK_ID = (int) BLOCK_ID_MASK;

    private VirtualPage() {} // utility class — no instances

    /**
     * Encode a (vfdId, blockId) pair into a single {@code long} key.
     *
     * @param vfdId  the VFD identifier (1 to {@link #MAX_VFD_ID})
     * @param blockId the block identifier (0 to {@link #MAX_BLOCK_ID})
     * @return the encoded key
     */
    public static long encode(long vfdId, int blockId) {
        assert vfdId > 0 && vfdId <= MAX_VFD_ID : "VFD_ID out of range: " + vfdId;
        assert blockId >= 0 && blockId <= MAX_BLOCK_ID : "Block_ID out of range: " + blockId;
        return (vfdId << BLOCK_ID_BITS) | (blockId & BLOCK_ID_MASK);
    }

    /**
     * Extract the VFD ID from an encoded key.
     *
     * @param key the encoded key from {@link #encode}
     * @return the VFD ID
     */
    public static long extractVfdId(long key) {
        return key >>> BLOCK_ID_BITS;
    }

    /**
     * Extract the block ID from an encoded key.
     *
     * @param key the encoded key from {@link #encode}
     * @return the block ID
     */
    public static int extractBlockId(long key) {
        return (int) (key & BLOCK_ID_MASK);
    }
}
