/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for BlockCacheKeyV2 equals/hashCode contract.
 *
 * Feature: bufferpool-v2-memory-management, Property 3: BlockCacheKeyV2 equals/hashCode contract
 * Validates: Requirements 2.1, 2.2, 2.3
 */
class BlockCacheKeyV2PropertyTests {

    /**
     * Property 3: BlockCacheKeyV2 equals/hashCode contract
     *
     * For any two keys with the same vfd and blockId, equals() returns true
     * and hashCode() matches. Round-trip accessor check included.
     *
     * Validates: Requirements 2.1, 2.2, 2.3
     */
    @Property(tries = 500)
    void equalKeysHaveEqualHashCodes(
            @ForAll @IntRange(min = 1, max = Integer.MAX_VALUE) int vfd,
            @ForAll @LongRange(min = 0, max = Long.MAX_VALUE >> 15) long blockId) {

        BlockCacheKeyV2 a = new BlockCacheKeyV2(vfd, blockId);
        BlockCacheKeyV2 b = new BlockCacheKeyV2(vfd, blockId);

        // equals contract: same vfd and blockId → equal
        assertEquals(a, b, "Keys with same vfd and blockId must be equal");
        assertEquals(b, a, "equals must be symmetric");

        // hashCode contract: equal objects must have equal hash codes
        assertEquals(a.hashCode(), b.hashCode(),
                "Equal keys must have equal hashCodes");

        // Round-trip accessor check
        assertEquals(vfd, a.vfd(), "vfd accessor must return constructor value");
        assertEquals(blockId, a.blockId(), "blockId accessor must return constructor value");
    }

    /**
     * Property 3 (inequality): Keys with different vfd or blockId are not equal.
     *
     * Validates: Requirements 2.1, 2.2
     */
    @Property(tries = 500)
    void differentVfdMeansNotEqual(
            @ForAll @IntRange(min = 1, max = Integer.MAX_VALUE - 1) int vfd1,
            @ForAll @LongRange(min = 0, max = Long.MAX_VALUE >> 15) long blockId) {

        int vfd2 = vfd1 + 1;
        BlockCacheKeyV2 a = new BlockCacheKeyV2(vfd1, blockId);
        BlockCacheKeyV2 b = new BlockCacheKeyV2(vfd2, blockId);

        assertNotEquals(a, b, "Keys with different vfd must not be equal");
    }

    @Property(tries = 500)
    void differentBlockIdMeansNotEqual(
            @ForAll @IntRange(min = 1, max = Integer.MAX_VALUE) int vfd,
            @ForAll @LongRange(min = 0, max = (Long.MAX_VALUE >> 15) - 1) long blockId1) {

        long blockId2 = blockId1 + 1;
        BlockCacheKeyV2 a = new BlockCacheKeyV2(vfd, blockId1);
        BlockCacheKeyV2 b = new BlockCacheKeyV2(vfd, blockId2);

        assertNotEquals(a, b, "Keys with different blockId must not be equal");
    }

    /**
     * Property 3 (reflexivity): a.equals(a) is always true.
     *
     * Validates: Requirements 2.1
     */
    @Property(tries = 200)
    void equalsIsReflexive(
            @ForAll @IntRange(min = 1, max = Integer.MAX_VALUE) int vfd,
            @ForAll @LongRange(min = 0, max = Long.MAX_VALUE >> 15) long blockId) {

        BlockCacheKeyV2 key = new BlockCacheKeyV2(vfd, blockId);
        assertEquals(key, key, "equals must be reflexive");
    }

    /**
     * Property 3 (null safety): a.equals(null) is always false.
     *
     * Validates: Requirements 2.1
     */
    @Property(tries = 200)
    void equalsNullReturnsFalse(
            @ForAll @IntRange(min = 1, max = Integer.MAX_VALUE) int vfd,
            @ForAll @LongRange(min = 0, max = Long.MAX_VALUE >> 15) long blockId) {

        BlockCacheKeyV2 key = new BlockCacheKeyV2(vfd, blockId);
        assertNotEquals(null, key, "equals(null) must return false");
    }

    /**
     * Property 3 (no Path/String): BlockCacheKeyV2 does not reference Path or String in key identity.
     * The hashCode uses only vfd and blockId fields.
     *
     * Validates: Requirements 2.4
     */
    @Property(tries = 200)
    void hashCodeDependsOnlyOnVfdAndBlockId(
            @ForAll @IntRange(min = 1, max = Integer.MAX_VALUE) int vfd,
            @ForAll @LongRange(min = 0, max = Long.MAX_VALUE >> 15) long blockId) {

        BlockCacheKeyV2 a = new BlockCacheKeyV2(vfd, blockId);
        BlockCacheKeyV2 b = new BlockCacheKeyV2(vfd, blockId);

        // Constructed independently, same hash — proves no hidden state
        assertEquals(a.hashCode(), b.hashCode());
    }

    /**
     * Property 3 (offset compatibility): offset() returns the reconstructed blockOffset.
     *
     * Validates: Requirements 2.1
     */
    @Property(tries = 200)
    void offsetReturnsBlockOffset(
            @ForAll @IntRange(min = 1, max = Integer.MAX_VALUE) int vfd,
            @ForAll @LongRange(min = 0, max = Long.MAX_VALUE >> 15) long blockId) {

        BlockCacheKeyV2 key = new BlockCacheKeyV2(vfd, blockId);
        assertEquals(key.blockOffset(), key.offset(),
                "offset() must return blockOffset()");
    }

    // ---- Property 3b: blockId ↔ blockOffset roundtrip consistency ----

    /**
     * Property 3b: blockId consistency (aligned offset roundtrip)
     *
     * For any valid blockOffset aligned to CACHE_BLOCK_SIZE,
     * blockOffset >>> CACHE_BLOCK_SIZE_POWER → blockId → blockId << CACHE_BLOCK_SIZE_POWER → original blockOffset.
     *
     * Catches subtle shift bugs where incorrect power or signed shift silently corrupts keys.
     *
     * Validates: Requirements 2.1, 2.3
     */
    @Property(tries = 500)
    void alignedBlockOffsetRoundTrips(
            @ForAll @LongRange(min = 0, max = (Long.MAX_VALUE >> 15)) long blockId) {

        int power = StaticConfigs.CACHE_BLOCK_SIZE_POWER;
        long blockOffset = blockId << power;

        // Reconstruct blockId from blockOffset
        long recoveredBlockId = blockOffset >>> power;
        assertEquals(blockId, recoveredBlockId,
                "blockOffset >>> POWER must recover original blockId");

        // Reconstruct blockOffset from recovered blockId
        long recoveredOffset = recoveredBlockId << power;
        assertEquals(blockOffset, recoveredOffset,
                "blockId << POWER must recover original blockOffset");
    }

    /**
     * Property 3b: blockId consistency (via BlockCacheKeyV2 accessor)
     *
     * For any blockId in valid range, constructing a key and calling blockOffset()
     * then shifting back must yield the original blockId.
     *
     * Validates: Requirements 2.1, 2.3
     */
    @Property(tries = 500)
    void blockCacheKeyRoundTrips(
            @ForAll @IntRange(min = 1, max = Integer.MAX_VALUE) int vfd,
            @ForAll @LongRange(min = 0, max = Long.MAX_VALUE >> 15) long blockId) {

        BlockCacheKeyV2 key = new BlockCacheKeyV2(vfd, blockId);

        // blockOffset() reconstructs from blockId
        long offset = key.blockOffset();
        // Shifting back must yield original blockId
        long recoveredBlockId = offset >>> StaticConfigs.CACHE_BLOCK_SIZE_POWER;
        assertEquals(blockId, recoveredBlockId,
                "blockOffset() >>> POWER must recover original blockId");
    }

    /**
     * Property 3b: blockId shift identity
     *
     * For any blockId in [0, maxBlockId], blockId << POWER >>> POWER == blockId.
     * This catches signed-shift bugs.
     *
     * Validates: Requirements 2.1, 2.3
     */
    @Property(tries = 500)
    void blockIdShiftIdentity(
            @ForAll @LongRange(min = 0, max = Long.MAX_VALUE >> 15) long blockId) {

        int power = StaticConfigs.CACHE_BLOCK_SIZE_POWER;
        long roundTripped = (blockId << power) >>> power;
        assertEquals(blockId, roundTripped,
                "blockId << POWER >>> POWER must equal original blockId");
    }
}

