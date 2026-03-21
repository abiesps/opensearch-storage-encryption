/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

/**
 * Encodes a {@code (vfdId, blockId)} pair into a single primitive {@code long}.
 *
 * <p>Bit layout:
 * <pre>
 *  63                              22 21                 0
 *  ┌──────────────────────────────────┬───────────────────┐
 *  │         VFD_ID (42 bits)         │  Block_ID (22 b)  │
 *  └──────────────────────────────────┴───────────────────┘
 * </pre>
 *
 * <ul>
 *   <li><b>Bits 63–22 (42 bits):</b> VFD_ID — monotonically increasing file descriptor
 *       identifier. Valid range: {@code [1, MAX_VFD_ID]}. Value 0 is reserved as "invalid".</li>
 *   <li><b>Bits 21–0 (22 bits):</b> Block_ID — cache block index within a file.
 *       Valid range: {@code [0, MAX_BLOCK_ID]}.</li>
 * </ul>
 *
 * <p>All methods are static; this class cannot be instantiated.
 */
public final class VirtualPage {

    // Prevent instantiation
    private VirtualPage() {
        throw new AssertionError("Utility class - do not instantiate");
    }

    // ── Constants ───────────────────────────────────────────────────

    /** Number of bits allocated to Block_ID in the encoded key. */
    public static final int BLOCK_ID_BITS = 22;

    /** Bitmask for extracting Block_ID: {@code (1L << 22) - 1 = 0x3FFFFF}. */
    public static final long BLOCK_ID_MASK = (1L << BLOCK_ID_BITS) - 1;

    /** Maximum valid VFD_ID: {@code (1L << 42) - 1 = 4,398,046,511,103}. */
    public static final long MAX_VFD_ID = (1L << 42) - 1;

    /** Maximum valid Block_ID: {@code (1L << 22) - 1 = 4,194,303}. */
    public static final int MAX_BLOCK_ID = (int) BLOCK_ID_MASK;

    // ── Encoding ────────────────────────────────────────────────────

    /**
     * Encodes a {@code (vfdId, blockId)} pair into a single {@code long}.
     *
     * @param vfdId   the virtual file descriptor ID; must be in {@code [1, MAX_VFD_ID]}
     * @param blockId the block index within the file; must be in {@code [0, MAX_BLOCK_ID]}
     * @return the encoded key
     * @throws AssertionError if assertions are enabled and inputs are out of range
     */
    public static long encode(long vfdId, int blockId) {
        assert vfdId > 0 : "vfdId must be positive, got: " + vfdId;
        assert vfdId <= MAX_VFD_ID : "vfdId exceeds MAX_VFD_ID, got: " + vfdId;
        assert blockId >= 0 : "blockId must be non-negative, got: " + blockId;
        assert blockId <= MAX_BLOCK_ID : "blockId exceeds MAX_BLOCK_ID, got: " + blockId;
        return (vfdId << BLOCK_ID_BITS) | (blockId & BLOCK_ID_MASK);
    }

    // ── Decoding ────────────────────────────────────────────────────

    /**
     * Extracts the VFD_ID from an encoded key.
     *
     * @param key the encoded key produced by {@link #encode}
     * @return the VFD_ID (upper 42 bits)
     */
    public static long extractVfdId(long key) {
        return key >>> BLOCK_ID_BITS;
    }

    /**
     * Extracts the Block_ID from an encoded key.
     *
     * @param key the encoded key produced by {@link #encode}
     * @return the Block_ID (lower 22 bits)
     */
    public static int extractBlockId(long key) {
        return (int) (key & BLOCK_ID_MASK);
    }

    // ── Diagnostics ─────────────────────────────────────────────────

    /**
     * Returns a human-readable representation of an encoded key.
     *
     * <p>Format: {@code "VirtualPage[vfdId=<id>, blockId=<id>]"}
     *
     * @param key the encoded key
     * @return diagnostic string
     */
    public static String toString(long key) {
        return "VirtualPage[vfdId=" + extractVfdId(key) + ", blockId=" + extractBlockId(key) + "]";
    }
}
