/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.block_cache_v2.CaffeineBlockCacheV2;
import org.opensearch.index.store.block_cache_v2.DirectBufferPoolDirectory;
import org.opensearch.index.store.block_cache_v2.DirectMemoryAdmissionController;
import org.opensearch.index.store.block_cache_v2.VirtualFileDescriptorRegistry;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

/**
 * Property-based tests for IndexInput read correctness, clone/close semantics,
 * and idempotent close behavior via DirectBufferPoolDirectory.
 *
 * Feature: bufferpool-v2-memory-management
 */
@SuppressWarnings("preview")
class IndexInputPropertyTests {

    private static final String TEST_FILE_NAME = "prop-test-data.bin";

    // ---- Helpers ----

    /**
     * Creates a temp directory with a test file of the given size filled with
     * deterministic random data seeded by the file size.
     */
    private TestFixture createFixture(int fileSize) throws IOException {
        Path tempDir = Files.createTempDirectory("idx-input-prop-");
        tempDir.toFile().deleteOnExit();

        byte[] data = new byte[fileSize];
        new Random(fileSize).nextBytes(data);
        Path testFile = tempDir.resolve(TEST_FILE_NAME);
        Files.write(testFile, data);

        DirectMemoryAdmissionController ac =
            new DirectMemoryAdmissionController(1_000_000_000L, 0.85, 0.95, 5000);
        CaffeineBlockCacheV2 cache = new CaffeineBlockCacheV2(1024, ac);
        DirectBufferPoolDirectory directory = new DirectBufferPoolDirectory(tempDir, cache);

        return new TestFixture(tempDir, data, cache, directory);
    }

    private void cleanUp(TestFixture f) {
        try {
            if (f.directory != null) f.directory.close();
        } catch (IOException ignored) {}
        VirtualFileDescriptorRegistry.getInstance().clear();
        if (f.tempDir != null) {
            try {
                Files.walk(f.tempDir)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> p.toFile().delete());
            } catch (IOException ignored) {}
        }
    }

    private record TestFixture(
        Path tempDir,
        byte[] expectedData,
        CaffeineBlockCacheV2 cache,
        DirectBufferPoolDirectory directory
    ) {}

    // ======================== Property 18 ========================

    /**
     * Property 18: Sequential read correctness across block boundaries.
     * For any file size, readByte() loop produces same bytes as direct disk read;
     * block boundary crossings handled transparently.
     *
     * Feature: bufferpool-v2-memory-management, Property 18: Sequential read correctness across block boundaries
     * Validates: Requirements 6.1, 6.2, 6.3
     */
    @Property(tries = 20)
    void sequentialReadCorrectnessAcrossBlockBoundaries(
        @ForAll @IntRange(min = 1, max = 65536) int fileSize
    ) throws IOException {
        VirtualFileDescriptorRegistry.getInstance().clear();
        TestFixture f = createFixture(fileSize);
        try (IndexInput input = f.directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT)) {
            assertEquals(fileSize, input.length(), "File length should match");

            for (int i = 0; i < fileSize; i++) {
                byte actual = input.readByte();
                assertEquals(f.expectedData[i], actual,
                    "Mismatch at byte " + i + " (fileSize=" + fileSize + ")");
            }
        } finally {
            cleanUp(f);
        }
    }

    // ======================== Property 19 ========================

    /**
     * Property 19: Random access read correctness.
     * For any valid position, readByte(pos) returns same byte as disk;
     * cache misses trigger loads and populate table.
     *
     * Feature: bufferpool-v2-memory-management, Property 19: Random access read correctness
     * Validates: Requirements 6.5, 6.6, 9.3
     */
    @Property(tries = 20)
    void randomAccessReadCorrectness(
        @ForAll @IntRange(min = 1, max = 65536) int fileSize,
        @ForAll @IntRange(min = 0, max = 65535) int rawPosition
    ) throws IOException {
        int position = rawPosition % fileSize; // ensure valid position
        VirtualFileDescriptorRegistry.getInstance().clear();
        TestFixture f = createFixture(fileSize);
        try (IndexInput input = f.directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT)) {
            // Seek to the random position and read
            input.seek(position);
            byte actual = input.readByte();
            assertEquals(f.expectedData[position], actual,
                "Mismatch at position " + position + " (fileSize=" + fileSize + ")");

            // Also verify a few positions across different blocks
            int numProbes = Math.min(10, fileSize);
            Random rng = new Random(fileSize ^ position);
            for (int i = 0; i < numProbes; i++) {
                int pos = rng.nextInt(fileSize);
                input.seek(pos);
                byte b = input.readByte();
                assertEquals(f.expectedData[pos], b,
                    "Mismatch at probe position " + pos);
            }
        } finally {
            cleanUp(f);
        }
    }

    // ======================== Property 20 ========================

    /**
     * Property 20: Clone close does not affect parent.
     * Closing a clone sets its currentSegment to null but parent continues
     * reading correctly.
     *
     * Feature: bufferpool-v2-memory-management, Property 20: Clone close does not affect parent
     * Validates: Requirements 7.3
     */
    @Property(tries = 20)
    void cloneCloseDoesNotAffectParent(
        @ForAll @IntRange(min = 1, max = 65536) int fileSize
    ) throws IOException {
        VirtualFileDescriptorRegistry.getInstance().clear();
        TestFixture f = createFixture(fileSize);
        try (IndexInput parent = f.directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT)) {
            // Read first byte from parent
            byte firstByte = parent.readByte();
            assertEquals(f.expectedData[0], firstByte, "Parent first byte should match");

            // Clone and read from clone at a position in a different block if possible
            IndexInput clone = parent.clone();
            int clonePos = Math.min(CACHE_BLOCK_SIZE + 1, fileSize - 1);
            clone.seek(clonePos);
            byte cloneByte = clone.readByte();
            assertEquals(f.expectedData[clonePos], cloneByte, "Clone should read correct data");

            // Close clone
            clone.close();

            // Parent should still work — read all bytes sequentially
            parent.seek(0);
            byte[] allBytes = new byte[fileSize];
            parent.readBytes(allBytes, 0, fileSize);
            assertArrayEquals(f.expectedData, allBytes,
                "Parent full read should work after clone close (fileSize=" + fileSize + ")");
        } finally {
            cleanUp(f);
        }
    }

    // ======================== Property 21 ========================

    /**
     * Property 21: Idempotent close.
     * Calling close() multiple times on parent, clone, or slice does not throw.
     *
     * Feature: bufferpool-v2-memory-management, Property 21: Idempotent close
     * Validates: Requirements 7.4
     */
    @Property(tries = 20)
    void idempotentClose(
        @ForAll @IntRange(min = 1, max = 65536) int fileSize
    ) throws IOException {
        VirtualFileDescriptorRegistry.getInstance().clear();
        TestFixture f = createFixture(fileSize);
        try {
            // Test parent idempotent close
            IndexInput parent = f.directory.openInput(TEST_FILE_NAME, IOContext.DEFAULT);
            parent.seek(0);
            parent.readByte();

            // Create clone — reset to position 0 so readByte() doesn't hit EOF
            IndexInput clone = parent.clone();
            clone.seek(0);
            clone.readByte();

            int sliceLen = Math.min(CACHE_BLOCK_SIZE, fileSize);
            IndexInput slice = parent.slice("test-slice", 0, sliceLen);
            slice.readByte();

            // Close clone multiple times
            assertDoesNotThrow(() -> clone.close(), "First clone close should not throw");
            assertDoesNotThrow(() -> clone.close(), "Second clone close should not throw");

            // Close slice multiple times
            assertDoesNotThrow(() -> slice.close(), "First slice close should not throw");
            assertDoesNotThrow(() -> slice.close(), "Second slice close should not throw");

            // Close parent multiple times
            assertDoesNotThrow(() -> parent.close(), "First parent close should not throw");
            assertDoesNotThrow(() -> parent.close(), "Second parent close should not throw");
            assertDoesNotThrow(() -> parent.close(), "Third parent close should not throw");
        } finally {
            cleanUp(f);
        }
    }
}
