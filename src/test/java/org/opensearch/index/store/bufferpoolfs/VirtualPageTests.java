/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.bufferpoolfs.VirtualPage.BLOCK_ID_BITS;
import static org.opensearch.index.store.bufferpoolfs.VirtualPage.BLOCK_ID_MASK;
import static org.opensearch.index.store.bufferpoolfs.VirtualPage.MAX_BLOCK_ID;
import static org.opensearch.index.store.bufferpoolfs.VirtualPage.MAX_VFD_ID;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import org.opensearch.test.OpenSearchTestCase;

public class VirtualPageTests extends OpenSearchTestCase {

    // ---- constant value tests ----

    public void testBlockIdBitsIs22() {
        assertEquals(22, BLOCK_ID_BITS);
    }

    public void testBlockIdMaskIs0x3FFFFF() {
        assertEquals(0x3FFFFFL, BLOCK_ID_MASK);
    }

    public void testMaxVfdIdEquals2Pow42Minus1() {
        assertEquals((1L << 42) - 1, MAX_VFD_ID);
    }

    public void testMaxBlockIdEquals2Pow22Minus1() {
        assertEquals((1L << 22) - 1, (long) MAX_BLOCK_ID);
    }

    // ---- capacity tests ----

    /** Req 2.1: 2^22 distinct Block_ID values per file. */
    public void testBlockIdCapacity() {
        assertEquals(4_194_304, MAX_BLOCK_ID + 1);
    }

    /** Req 2.2: With 8KB blocks, supports files up to 32 GB. */
    public void testMaxFileSizeWith8KBlocks() {
        assertEquals(34_359_738_368L, (MAX_BLOCK_ID + 1L) * 8192);
    }

    /** Req 3.1: Up to 2^42 unique VFD_ID values. */
    public void testVfdIdCapacity() {
        assertEquals(4_398_046_511_103L, MAX_VFD_ID);
    }

    // ---- encode/decode round-trip for known values ----

    public void testEncodeDecodeVfdId1BlockId0() {
        long key = VirtualPage.encode(1, 0);
        assertEquals(1L, VirtualPage.extractVfdId(key));
        assertEquals(0, VirtualPage.extractBlockId(key));
    }

    public void testEncodeDecodeVfdId1BlockIdMax() {
        long key = VirtualPage.encode(1, MAX_BLOCK_ID);
        assertEquals(1L, VirtualPage.extractVfdId(key));
        assertEquals(MAX_BLOCK_ID, VirtualPage.extractBlockId(key));
    }

    public void testEncodeDecodeMaxVfdIdBlockId0() {
        long key = VirtualPage.encode(MAX_VFD_ID, 0);
        assertEquals(MAX_VFD_ID, VirtualPage.extractVfdId(key));
        assertEquals(0, VirtualPage.extractBlockId(key));
    }

    public void testEncodeDecodeMaxVfdIdMaxBlockId() {
        long key = VirtualPage.encode(MAX_VFD_ID, MAX_BLOCK_ID);
        assertEquals(MAX_VFD_ID, VirtualPage.extractVfdId(key));
        assertEquals(MAX_BLOCK_ID, VirtualPage.extractBlockId(key));
    }

    // ---- toString format ----

    public void testToStringFormat() {
        long key = VirtualPage.encode(42, 7);
        assertEquals("VirtualPage[vfdId=42, blockId=7]", VirtualPage.toString(key));
    }

    // ---- utility class design (Req 9.1, 9.2) ----

    public void testClassIsFinal() {
        assertTrue(Modifier.isFinal(VirtualPage.class.getModifiers()));
    }

    public void testConstructorIsPrivateAndThrowsAssertionError() throws Exception {
        Constructor<VirtualPage> ctor = VirtualPage.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(ctor.getModifiers()));
        ctor.setAccessible(true);
        try {
            ctor.newInstance();
            fail("Expected AssertionError from private constructor");
        } catch (java.lang.reflect.InvocationTargetException e) {
            assertTrue(e.getCause() instanceof AssertionError);
        }
    }

    // ---- VFD_ID 0 triggers AssertionError (Req 3.2) ----

    public void testVfdIdZeroThrowsAssertionError() {
        AssertionError e = expectThrows(AssertionError.class, () -> VirtualPage.encode(0, 0));
        assertTrue(e.getMessage().contains("0"));
    }
}
