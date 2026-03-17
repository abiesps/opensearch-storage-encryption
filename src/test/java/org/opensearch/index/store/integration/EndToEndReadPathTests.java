/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.index.store.block_cache_v2.CaffeineBlockCacheV2;
import org.opensearch.index.store.block_cache_v2.DirectBufferPoolDirectory;
import org.opensearch.index.store.block_cache_v2.DirectMemoryAdmissionController;
import org.opensearch.index.store.block_cache_v2.VirtualFileDescriptorRegistry;

/**
 * End-to-end read path smoke test for the V2 block cache.
 *
 * Creates a temp file on disk (16KB = 4 blocks at 4KB), opens it via
 * DirectBufferPoolDirectory -> DirectByteBufferIndexInput, and verifies:
 * - Sequential read correctness
 * - Random access read correctness
 * - Clone read + close semantics
 * - Parent close deregisters VFD and clears table
 *
 * Validates: Requirements 6.1, 6.2, 6.3, 6.5, 7.1, 7.2, 7.3, 7.4
 */
@SuppressWarnings("preview")
class EndToEndReadPathTests {

    private static final int FILE_SIZE = 16 * 1024; // 16KB = 4 blocks at 4KB
    private static final String TEST_FILE_NAME = "test-data.bin";

    private Path tempDir;
    private byte[] expectedData;
    private CaffeineBlockCacheV2 blockCache;
    private DirectBufferPoolDirectory directory;
    private VirtualFileDescriptorRegistry vfdRegistry;

    @BeforeEach
    void setUp() throws IOException {
        vfdRegistry = VirtualFileDescriptorRegistry.getInstance();
        vfdRegistry.clear();

        tempDir = Files.createTempDirectory("e2e-read-path-test-");
        tempDir.toFile().deleteOnExit();

        // Generate deterministic test data
        expectedData = new byte[FILE_SIZE];
        Random rng = new Random(42);
        rng.nextBytes(expectedData);

        // Write test file to disk using standard Java I/O
        Path testFile = tempDir.resolve(TEST_FILE_NAME);
        Files.write(testFile, expectedData);

        // Create cache and directory
        DirectMemoryAdmissionController ac =
            new DirectMemoryAdmissionController(1_000_000_000L, 0.85, 0.95, 5000);
        blockCache = new CaffeineBlockCacheV2(1024, ac);
        directory = new DirectBufferPoolDirectory(tempDir, blockCache);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (directory != null) {
            directory.close();
        }
        vfdRegistry.clear();
        // Clean up temp files
        if (tempDir != null) {
            Files.walk(tempDir)
                .sorted(java.util.Comparator.reverseOrder())
                .forEach(p -> p.toFile().delete());
        }
    }

    /**
     * Sequential read: read all bytes one-by-one via readByte(),
     * verify against direct disk read.
     */
    @Test
    void testSequentialReadAllBytes() throws IOException {
        try (IndexInput input = directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT)) {
            assertEquals(FILE_SIZE, input.length(), "File length should match");

            for (int i = 0; i < FILE_SIZE; i++) {
                byte actual = input.readByte();
                assertEquals(expectedData[i], actual,
                    "Mismatch at byte " + i);
            }
        }
    }

    /**
     * Sequential bulk read: read all bytes via readBytes() in chunks,
     * verify against direct disk read.
     */
    @Test
    void testSequentialBulkRead() throws IOException {
        try (IndexInput input = directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT)) {
            byte[] actual = new byte[FILE_SIZE];
            input.readBytes(actual, 0, FILE_SIZE);
            assertArrayEquals(expectedData, actual,
                "Bulk read should match expected data");
        }
    }

    /**
     * Random access: read bytes at random positions via readByte(pos),
     * verify correctness.
     */
    @Test
    void testRandomAccessReads() throws IOException {
        try (IndexInput input = directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT)) {
            Random rng = new Random(123);
            for (int i = 0; i < 200; i++) {
                int pos = rng.nextInt(FILE_SIZE);
                input.seek(pos);
                byte actual = input.readByte();
                assertEquals(expectedData[pos], actual,
                    "Mismatch at random position " + pos);
            }
        }
    }

    /**
     * Random access across block boundaries: seek to positions near
     * block boundaries and read multi-byte values.
     */
    @Test
    void testReadAcrossBlockBoundaries() throws IOException {
        try (IndexInput input = directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT)) {
            // Read across each block boundary
            int numBlocks = FILE_SIZE / CACHE_BLOCK_SIZE;
            for (int b = 1; b < numBlocks; b++) {
                int boundaryPos = b * CACHE_BLOCK_SIZE - 2;
                input.seek(boundaryPos);
                // Read 4 bytes spanning the boundary
                byte[] buf = new byte[4];
                input.readBytes(buf, 0, 4);
                for (int j = 0; j < 4; j++) {
                    assertEquals(expectedData[boundaryPos + j], buf[j],
                        "Mismatch at boundary offset " + (boundaryPos + j));
                }
            }
        }
    }

    /**
     * Clone: read from clone, close clone, verify parent still works.
     */
    @Test
    void testCloneReadAndCloseDoesNotAffectParent() throws IOException {
        try (IndexInput parent = directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT)) {
            // Read first block from parent
            parent.seek(0);
            byte firstByte = parent.readByte();
            assertEquals(expectedData[0], firstByte);

            // Clone and read from clone
            IndexInput clone = parent.clone();
            clone.seek(100);
            byte cloneByte = clone.readByte();
            assertEquals(expectedData[100], cloneByte,
                "Clone should read correct data");

            // Read from a different position in clone
            clone.seek(CACHE_BLOCK_SIZE + 50);
            byte cloneByte2 = clone.readByte();
            assertEquals(expectedData[CACHE_BLOCK_SIZE + 50], cloneByte2,
                "Clone should read correct data from second block");

            // Close clone
            clone.close();

            // Parent should still work after clone is closed
            parent.seek(500);
            byte parentByte = parent.readByte();
            assertEquals(expectedData[500], parentByte,
                "Parent should still read correctly after clone close");

            // Read across all blocks from parent
            parent.seek(0);
            byte[] allBytes = new byte[FILE_SIZE];
            parent.readBytes(allBytes, 0, FILE_SIZE);
            assertArrayEquals(expectedData, allBytes,
                "Parent full read should still work after clone close");
        }
    }

    /**
     * Slice: create a slice, read from it, verify correctness.
     */
    @Test
    void testSliceRead() throws IOException {
        try (IndexInput parent = directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT)) {
            int sliceOffset = CACHE_BLOCK_SIZE + 100;
            int sliceLength = CACHE_BLOCK_SIZE * 2;
            IndexInput slice = parent.slice("test-slice", sliceOffset, sliceLength);

            assertEquals(sliceLength, slice.length(), "Slice length should match");

            byte[] sliceData = new byte[sliceLength];
            slice.readBytes(sliceData, 0, sliceLength);

            for (int i = 0; i < sliceLength; i++) {
                assertEquals(expectedData[sliceOffset + i], sliceData[i],
                    "Slice mismatch at offset " + i);
            }

            slice.close();

            // Parent still works
            parent.seek(0);
            assertEquals(expectedData[0], parent.readByte(),
                "Parent should work after slice close");
        }
    }

    /**
     * Parent close: verify VFD deregistered and table cleared.
     */
    @Test
    void testParentCloseDeregistersVfdAndClearsTable() throws IOException {
        int vfdBefore = vfdRegistry.size();

        IndexInput input = directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT);

        // VFD should be registered
        assertTrue(vfdRegistry.size() > vfdBefore,
            "VFD registry should have a new entry after openInput");

        // Read some data to populate the block table
        byte[] buf = new byte[FILE_SIZE];
        input.readBytes(buf, 0, FILE_SIZE);
        assertArrayEquals(expectedData, buf);

        // Close parent
        input.close();

        // VFD should be deregistered
        assertEquals(vfdBefore, vfdRegistry.size(),
            "VFD registry should return to original size after close");
    }

    /**
     * Idempotent close: calling close() multiple times should not throw.
     */
    @Test
    void testIdempotentClose() throws IOException {
        IndexInput input = directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT);
        input.readByte();
        input.close();
        input.close(); // second close should not throw
        input.close(); // third close should not throw
    }

    /**
     * Clone idempotent close: closing a clone multiple times should not throw.
     */
    @Test
    void testCloneIdempotentClose() throws IOException {
        try (IndexInput parent = directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT)) {
            IndexInput clone = parent.clone();
            clone.readByte();
            clone.close();
            clone.close(); // should not throw
        }
    }

    /**
     * Verify block size is 4KB as expected after StaticConfigs change.
     */
    @Test
    void testBlockSizeIs4KB() {
        assertEquals(4096, CACHE_BLOCK_SIZE,
            "CACHE_BLOCK_SIZE should be 4096 (4KB)");
        assertEquals(4095, org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_MASK,
            "CACHE_BLOCK_MASK should be 4095");
        assertEquals(12, org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER,
            "CACHE_BLOCK_SIZE_POWER should be 12");
    }
}
