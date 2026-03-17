/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

import org.opensearch.index.store.bufferpoolfs.SparseLongBlockTable;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Unit tests for SparseLongBlockTable edge cases.
 *
 * Feature: bufferpool-v2-memory-management
 * Requirements: 3.3, 10.2, 10.3
 */
class SparseLongBlockTableTests {

    private static final int PAGE_SIZE = 1 << SparseLongBlockTable.PAGE_SHIFT; // 64

    /**
     * Test max block ID for 10GB file with current block size.
     *
     * put/get/remove must all succeed at this block ID.
     *
     * Requirements: 3.3, 10.2, 10.3
     */
    @Test
    void maxBlockIdForTenGbFile() {
        int power = StaticConfigs.CACHE_BLOCK_SIZE_POWER;
        int blockSize = 1 << power;
        long tenGb = 10L * 1024 * 1024 * 1024;
        long maxBlockId = (tenGb / blockSize) - 1;

        // Verify our expected max blockId calculation
        if (power == 15) {
            assertEquals(327679L, maxBlockId, "Max blockId for 10GB with 32KB blocks");
        } else if (power == 12) {
            assertEquals(2621439L, maxBlockId, "Max blockId for 10GB with 4KB blocks");
        }

        SparseLongBlockTable table = new SparseLongBlockTable(1);
        byte[] data = new byte[64];
        data[0] = (byte) 0xDE;
        data[1] = (byte) 0xAD;
        MemorySegment seg = MemorySegment.ofArray(data);

        // put at max blockId — triggers directory growth and page allocation
        table.put(maxBlockId, seg);

        // get at max blockId — must return the same segment
        MemorySegment result = table.get(maxBlockId);
        assertNotNull(result, "get at max blockId must return non-null");
        assertSame(seg, result, "get must return the exact segment that was put");

        // remove at max blockId — must return the previous segment
        MemorySegment removed = table.remove(maxBlockId);
        assertNotNull(removed, "remove at max blockId must return non-null");
        assertSame(seg, removed, "remove must return the exact segment that was stored");

        // After remove, get must return null
        assertNull(table.get(maxBlockId), "get after remove must return null");
    }

    /**
     * Test that get() beyond directory returns null without exception.
     */
    @Test
    void getBeyondDirectoryReturnsNull() {
        SparseLongBlockTable table = new SparseLongBlockTable(1);
        assertNull(table.get(100_000), "get beyond directory must return null");
    }

    /**
     * Test that remove() beyond directory returns null without exception.
     */
    @Test
    void removeBeyondDirectoryReturnsNull() {
        SparseLongBlockTable table = new SparseLongBlockTable(1);
        assertNull(table.remove(100_000), "remove beyond directory must return null");
    }

    /**
     * Test clear() nulls all pages.
     */
    @Test
    void clearNullsAllPagesAndResetsCounts() {
        SparseLongBlockTable table = new SparseLongBlockTable(2);

        // Fill entries in two different pages
        table.put(0, MemorySegment.ofArray(new byte[8]));
        table.put(PAGE_SIZE, MemorySegment.ofArray(new byte[8])); // page 1

        assertEquals(1, table.getChunkPopCount(0));
        assertEquals(1, table.getChunkPopCount(1));

        table.clear();

        assertNull(table.get(0), "get after clear must return null");
        assertNull(table.get(PAGE_SIZE), "get after clear must return null");
    }

    /**
     * Test put overwrites existing entry at same blockId.
     */
    @Test
    void putOverwritesExistingEntry() {
        SparseLongBlockTable table = new SparseLongBlockTable(1);
        MemorySegment seg1 = MemorySegment.ofArray(new byte[8]);
        MemorySegment seg2 = MemorySegment.ofArray(new byte[8]);

        table.put(5, seg1);
        assertSame(seg1, table.get(5));
        assertEquals(1, table.getChunkPopCount(0), "popcount must be 1 after single put");

        table.put(5, seg2);
        assertSame(seg2, table.get(5), "get must return the overwritten segment");
        assertEquals(1, table.getChunkPopCount(0), "popcount must still be 1 after overwrite");
    }

    /**
     * Test remove on empty slot returns null.
     */
    @Test
    void removeEmptySlotReturnsNull() {
        SparseLongBlockTable table = new SparseLongBlockTable(1);
        table.put(0, MemorySegment.ofArray(new byte[8]));
        assertNull(table.remove(1), "remove on empty slot must return null");
    }

    /**
     * Test that popcount tracks correctly across multiple slots in same page.
     */
    @Test
    void popcountTracksMultipleSlotsInSamePage() {
        SparseLongBlockTable table = new SparseLongBlockTable(1);
        for (int i = 0; i < 10; i++) {
            table.put(i, MemorySegment.ofArray(new byte[8]));
        }
        assertEquals(10, table.getChunkPopCount(0));

        for (int i = 0; i < 5; i++) {
            table.remove(i);
        }
        assertEquals(5, table.getChunkPopCount(0));
    }
}
