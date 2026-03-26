/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Property-based tests for {@link VirtualPage}.
 * Uses randomized iteration (100 trials per property) via OpenSearchTestCase.
 */
public class VirtualPagePropertyTests extends OpenSearchTestCase {

    /** Property 1: encode/decode round-trip for random (vfdId, blockId) pairs. */
    public void testEncodeDecodeRoundTrip() {
        for (int trial = 0; trial < 100; trial++) {
            long vfdId = randomLongBetween(1L, VirtualPage.MAX_VFD_ID);
            int blockId = randomIntBetween(0, VirtualPage.MAX_BLOCK_ID);

            long key = VirtualPage.encode(vfdId, blockId);
            assertEquals("vfdId round-trip failed for (" + vfdId + ", " + blockId + ")",
                vfdId, VirtualPage.extractVfdId(key));
            assertEquals("blockId round-trip failed for (" + vfdId + ", " + blockId + ")",
                blockId, VirtualPage.extractBlockId(key));
        }
    }

    /** Property 2: distinct inputs produce distinct keys. */
    public void testDistinctInputsProduceDistinctKeys() {
        for (int trial = 0; trial < 100; trial++) {
            long vfdId = randomLongBetween(1L, VirtualPage.MAX_VFD_ID);
            int blockId = randomIntBetween(0, VirtualPage.MAX_BLOCK_ID);
            long key = VirtualPage.encode(vfdId, blockId);
            // Key should be unique for unique (vfdId, blockId) — collisions
            // are possible with random draws but astronomically unlikely
            // given the 64-bit key space. We just verify the mapping is injective
            // by re-extracting.
            assertEquals(vfdId, VirtualPage.extractVfdId(key));
            assertEquals(blockId, VirtualPage.extractBlockId(key));
        }
    }

    /** Property 3: encoded key is always non-negative (vfdId >= 1 means bit 63 is 0 for reasonable vfdIds). */
    public void testEncodedKeyNonNegativeForSmallVfdIds() {
        for (int trial = 0; trial < 100; trial++) {
            // VFD IDs up to 2^41 - 1 produce non-negative keys (bit 63 = 0)
            long vfdId = randomLongBetween(1L, (1L << 41) - 1);
            int blockId = randomIntBetween(0, VirtualPage.MAX_BLOCK_ID);
            long key = VirtualPage.encode(vfdId, blockId);
            assertTrue("key should be non-negative for vfdId=" + vfdId, key >= 0);
        }
    }

    /** Property 4: blockId bits are independent of vfdId bits. */
    public void testBlockIdBitsIndependentOfVfdId() {
        for (int trial = 0; trial < 100; trial++) {
            long vfdId = randomLongBetween(1L, VirtualPage.MAX_VFD_ID);
            int blockId1 = randomIntBetween(0, VirtualPage.MAX_BLOCK_ID);
            int blockId2 = randomIntBetween(0, VirtualPage.MAX_BLOCK_ID);

            long key1 = VirtualPage.encode(vfdId, blockId1);
            long key2 = VirtualPage.encode(vfdId, blockId2);

            // Same vfdId → same upper bits
            assertEquals(VirtualPage.extractVfdId(key1), VirtualPage.extractVfdId(key2));
            // Different blockIds → different lower bits (unless blockId1 == blockId2)
            if (blockId1 != blockId2) {
                assertNotEquals(key1, key2);
            }
        }
    }

    /** Property 5: vfdId bits are independent of blockId bits. */
    public void testVfdIdBitsIndependentOfBlockId() {
        for (int trial = 0; trial < 100; trial++) {
            long vfdId1 = randomLongBetween(1L, VirtualPage.MAX_VFD_ID);
            long vfdId2 = randomLongBetween(1L, VirtualPage.MAX_VFD_ID);
            int blockId = randomIntBetween(0, VirtualPage.MAX_BLOCK_ID);

            long key1 = VirtualPage.encode(vfdId1, blockId);
            long key2 = VirtualPage.encode(vfdId2, blockId);

            // Same blockId → same lower bits
            assertEquals(VirtualPage.extractBlockId(key1), VirtualPage.extractBlockId(key2));
            // Different vfdIds → different upper bits (unless vfdId1 == vfdId2)
            if (vfdId1 != vfdId2) {
                assertNotEquals(key1, key2);
            }
        }
    }
}
