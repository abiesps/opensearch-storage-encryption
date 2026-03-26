/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link VirtualPage} — encode/decode round-trip,
 * boundary values, and overflow assertion guards.
 */
public class VirtualPageTests extends OpenSearchTestCase {

    public void testEncodeDecodeRoundTrip() {
        long vfdId = 42L;
        int blockId = 1023;
        long key = VirtualPage.encode(vfdId, blockId);
        assertEquals(vfdId, VirtualPage.extractVfdId(key));
        assertEquals(blockId, VirtualPage.extractBlockId(key));
    }

    public void testMinimumValues() {
        // vfdId=1 (minimum valid), blockId=0
        long key = VirtualPage.encode(1L, 0);
        assertEquals(1L, VirtualPage.extractVfdId(key));
        assertEquals(0, VirtualPage.extractBlockId(key));
    }

    public void testMaximumVfdId() {
        long key = VirtualPage.encode(VirtualPage.MAX_VFD_ID, 0);
        assertEquals(VirtualPage.MAX_VFD_ID, VirtualPage.extractVfdId(key));
        assertEquals(0, VirtualPage.extractBlockId(key));
    }

    public void testMaximumBlockId() {
        long key = VirtualPage.encode(1L, VirtualPage.MAX_BLOCK_ID);
        assertEquals(1L, VirtualPage.extractVfdId(key));
        assertEquals(VirtualPage.MAX_BLOCK_ID, VirtualPage.extractBlockId(key));
    }

    public void testBothMaximum() {
        long key = VirtualPage.encode(VirtualPage.MAX_VFD_ID, VirtualPage.MAX_BLOCK_ID);
        assertEquals(VirtualPage.MAX_VFD_ID, VirtualPage.extractVfdId(key));
        assertEquals(VirtualPage.MAX_BLOCK_ID, VirtualPage.extractBlockId(key));
    }

    public void testDistinctKeysForDifferentInputs() {
        long key1 = VirtualPage.encode(1L, 0);
        long key2 = VirtualPage.encode(1L, 1);
        long key3 = VirtualPage.encode(2L, 0);
        assertNotEquals(key1, key2);
        assertNotEquals(key1, key3);
        assertNotEquals(key2, key3);
    }

    public void testVfdIdZeroAsserts() {
        AssertionError e = expectThrows(AssertionError.class, () -> VirtualPage.encode(0L, 0));
        assertTrue(e.getMessage().contains("VFD_ID out of range"));
    }

    public void testVfdIdOverflowAsserts() {
        AssertionError e = expectThrows(AssertionError.class, () -> VirtualPage.encode(VirtualPage.MAX_VFD_ID + 1, 0));
        assertTrue(e.getMessage().contains("VFD_ID out of range"));
    }

    public void testNegativeVfdIdAsserts() {
        AssertionError e = expectThrows(AssertionError.class, () -> VirtualPage.encode(-1L, 0));
        assertTrue(e.getMessage().contains("VFD_ID out of range"));
    }

    public void testBlockIdOverflowAsserts() {
        AssertionError e = expectThrows(AssertionError.class, () -> VirtualPage.encode(1L, VirtualPage.MAX_BLOCK_ID + 1));
        assertTrue(e.getMessage().contains("Block_ID out of range"));
    }

    public void testNegativeBlockIdAsserts() {
        AssertionError e = expectThrows(AssertionError.class, () -> VirtualPage.encode(1L, -1));
        assertTrue(e.getMessage().contains("Block_ID out of range"));
    }

    public void testBlockIdBitsDoNotLeakIntoVfdId() {
        // Encode with max blockId, verify vfdId extraction is clean
        long key = VirtualPage.encode(1L, VirtualPage.MAX_BLOCK_ID);
        assertEquals(1L, VirtualPage.extractVfdId(key));
        // Encode with vfdId that has low bits set, verify blockId extraction is clean
        long key2 = VirtualPage.encode(0xFFL, 0);
        assertEquals(0, VirtualPage.extractBlockId(key2));
    }

    public void testConstants() {
        assertEquals(22, VirtualPage.BLOCK_ID_BITS);
        assertEquals((1L << 22) - 1, VirtualPage.BLOCK_ID_MASK);
        assertEquals((1L << 42) - 1, VirtualPage.MAX_VFD_ID);
        assertEquals((1 << 22) - 1, VirtualPage.MAX_BLOCK_ID);
    }
}
