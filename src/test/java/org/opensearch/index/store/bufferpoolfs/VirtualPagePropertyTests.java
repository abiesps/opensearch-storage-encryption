/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Property-based tests for {@link VirtualPage}.
 * Uses randomized iteration (100 tries per property) via OpenSearchTestCase
 * random utilities.
 *
 * Feature: vfd-virtual-page
 */
public class VirtualPagePropertyTests extends OpenSearchTestCase {

    /**
     * Feature: vfd-virtual-page, Property 1: Encode/Decode Round-Trip
     *
     * Validates: Requirements 1.1, 4.1, 4.2, 5.1, 5.2
     */
    public void testEncodeDecodeRoundTrip() {
        for (int trial = 0; trial < 100; trial++) {
            long vfdId = randomLongBetween(1, VirtualPage.MAX_VFD_ID);
            int blockId = randomIntBetween(0, VirtualPage.MAX_BLOCK_ID);

            long key = VirtualPage.encode(vfdId, blockId);

            assertEquals("vfdId round-trip failed on trial " + trial, vfdId, VirtualPage.extractVfdId(key));
            assertEquals("blockId round-trip failed on trial " + trial, blockId, VirtualPage.extractBlockId(key));
        }
    }

    /**
     * Feature: vfd-virtual-page, Property 2: Uniqueness (Injectivity)
     *
     * Validates: Requirements 6.1
     */
    public void testUniqueness() {
        for (int trial = 0; trial < 100; trial++) {
            long v1 = randomLongBetween(1, VirtualPage.MAX_VFD_ID);
            int b1 = randomIntBetween(0, VirtualPage.MAX_BLOCK_ID);
            long v2 = randomLongBetween(1, VirtualPage.MAX_VFD_ID);
            int b2 = randomIntBetween(0, VirtualPage.MAX_BLOCK_ID);

            // Ensure the two pairs are distinct; regenerate if equal
            if (v1 == v2 && b1 == b2) {
                b2 = (b2 == VirtualPage.MAX_BLOCK_ID) ? 0 : b2 + 1;
            }

            long key1 = VirtualPage.encode(v1, b1);
            long key2 = VirtualPage.encode(v2, b2);

            assertNotEquals(
                "Distinct pairs (" + v1 + "," + b1 + ") and (" + v2 + "," + b2 + ") produced same key on trial " + trial,
                key1,
                key2
            );
        }
    }

    /**
     * Feature: vfd-virtual-page, Property 3: Overflow Guards Reject Invalid Inputs
     *
     * Validates: Requirements 3.2, 7.1, 7.2, 7.3, 7.4, 8.1
     */
    public void testOverflowGuards() {
        for (int trial = 0; trial < 100; trial++) {
            // Valid counterparts for pairing with invalid values
            long validVfdId = randomLongBetween(1, VirtualPage.MAX_VFD_ID);
            int validBlockId = randomIntBetween(0, VirtualPage.MAX_BLOCK_ID);

            // Case 1: vfdId <= 0
            long negativeVfdId = randomLongBetween(Long.MIN_VALUE, 0);
            AssertionError e1 = expectThrows(AssertionError.class, () -> VirtualPage.encode(negativeVfdId, validBlockId));
            assertTrue(
                "Error message should contain invalid vfdId " + negativeVfdId + ", got: " + e1.getMessage(),
                e1.getMessage().contains(String.valueOf(negativeVfdId))
            );

            // Case 2: vfdId > MAX_VFD_ID
            long overflowVfdId = randomLongBetween(VirtualPage.MAX_VFD_ID + 1, Long.MAX_VALUE);
            AssertionError e2 = expectThrows(AssertionError.class, () -> VirtualPage.encode(overflowVfdId, validBlockId));
            assertTrue(
                "Error message should contain invalid vfdId " + overflowVfdId + ", got: " + e2.getMessage(),
                e2.getMessage().contains(String.valueOf(overflowVfdId))
            );

            // Case 3: blockId < 0
            int negativeBlockId = randomIntBetween(Integer.MIN_VALUE, -1);
            AssertionError e3 = expectThrows(AssertionError.class, () -> VirtualPage.encode(validVfdId, negativeBlockId));
            assertTrue(
                "Error message should contain invalid blockId " + negativeBlockId + ", got: " + e3.getMessage(),
                e3.getMessage().contains(String.valueOf(negativeBlockId))
            );

            // Case 4: blockId > MAX_BLOCK_ID
            int overflowBlockId = randomIntBetween(VirtualPage.MAX_BLOCK_ID + 1, Integer.MAX_VALUE);
            AssertionError e4 = expectThrows(AssertionError.class, () -> VirtualPage.encode(validVfdId, overflowBlockId));
            assertTrue(
                "Error message should contain invalid blockId " + overflowBlockId + ", got: " + e4.getMessage(),
                e4.getMessage().contains(String.valueOf(overflowBlockId))
            );
        }
    }

    /**
     * Feature: vfd-virtual-page, Property 4: toString Round-Trip
     *
     * Validates: Requirements 10.1, 10.2
     */
    public void testToStringRoundTrip() {
        Pattern pattern = Pattern.compile("VirtualPage\\[vfdId=(\\d+), blockId=(\\d+)]");

        for (int trial = 0; trial < 100; trial++) {
            long vfdId = randomLongBetween(1, VirtualPage.MAX_VFD_ID);
            int blockId = randomIntBetween(0, VirtualPage.MAX_BLOCK_ID);

            long key = VirtualPage.encode(vfdId, blockId);
            String str = VirtualPage.toString(key);

            Matcher matcher = pattern.matcher(str);
            assertTrue("toString output did not match expected format: " + str, matcher.matches());

            long parsedVfdId = Long.parseLong(matcher.group(1));
            int parsedBlockId = Integer.parseInt(matcher.group(2));

            long reEncoded = VirtualPage.encode(parsedVfdId, parsedBlockId);
            assertEquals("Re-encoded key from toString output should equal original on trial " + trial, key, reEncoded);
        }
    }
}
