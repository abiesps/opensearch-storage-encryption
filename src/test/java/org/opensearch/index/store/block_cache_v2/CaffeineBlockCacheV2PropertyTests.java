/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.opensearch.index.store.bufferpoolfs.RadixBlockTable;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

/**
 * Property-based tests for CaffeineBlockCacheV2.
 *
 * Feature: bufferpool-v2-memory-management
 */
@SuppressWarnings({"removal", "preview"})
class CaffeineBlockCacheV2PropertyTests {

    @BeforeProperty
    void setUp() {
    }

    @AfterProperty
    void tearDown() {
    }

    // ---- Helpers ----

    private Path createTempFile(int numBlocks) throws IOException {
        Path tempFile = Files.createTempFile("cache-prop-test-", ".dat");
        tempFile.toFile().deleteOnExit();
        int fileSize = numBlocks * CACHE_BLOCK_SIZE;
        byte[] data = new byte[fileSize];
        for (int i = 0; i < fileSize; i++) {
            data[i] = (byte) (i % 251);
        }
        Files.write(tempFile, data);
        return tempFile;
    }

    private Path createTempFileWithSize(int fileSize) throws IOException {
        Path tempFile = Files.createTempFile("cache-prop-test-", ".dat");
        tempFile.toFile().deleteOnExit();
        byte[] data = new byte[fileSize];
        for (int i = 0; i < fileSize; i++) {
            data[i] = (byte) (i % 251);
        }
        Files.write(tempFile, data);
        return tempFile;
    }

    // ======================== Property 14 ========================

    /**
     * Property 14: Cache returns read-only direct ByteBuffer slices.
     * For any loaded block, returned ByteBuffer is direct, read-only,
     * limit = block size (or remaining for last block).
     *
     * Feature: bufferpool-v2-memory-management, Property 14: Cache returns read-only ByteBuffer slices
     * Validates: Requirements 4.1, 4.3
     */
    @Property(tries = 50)
    void cacheReturnsReadOnlyDirectByteBuffer(
        @ForAll @IntRange(min = 1, max = 8) int numBlocks
    ) throws IOException {
        DirectMemoryAdmissionController ac = new DirectMemoryAdmissionController(1_000_000_000L, 0.85, 0.95, 5000);
        CaffeineBlockCacheV2 cache = new CaffeineBlockCacheV2(1024, ac);
        try {
            Path tempFile = createTempFile(numBlocks);
            String filePath = tempFile.toAbsolutePath().toString();

            for (int blockId = 0; blockId < numBlocks; blockId++) {
                BlockCacheKeyV2 key = new BlockCacheKeyV2(filePath, blockId);
                ByteBuffer buf = cache.getOrLoad(key);

                assertNotNull(buf, "Returned buffer should not be null");
                assertTrue(buf.isDirect(), "Returned buffer should be direct");
                assertTrue(buf.isReadOnly(), "Returned buffer should be read-only");
                assertEquals(CACHE_BLOCK_SIZE, buf.limit(),
                    "Full block should have limit = CACHE_BLOCK_SIZE");
            }
        } finally {
            cache.close();
        }
    }

    /**
     * Property 14 (last block variant): For the last block of a file that doesn't
     * align to block size, the limit should equal the remaining bytes.
     *
     * Feature: bufferpool-v2-memory-management, Property 14: Cache returns read-only ByteBuffer slices
     * Validates: Requirements 4.1, 4.3
     */
    @Property(tries = 50)
    void cacheReturnsCorrectLimitForLastBlock(
        @ForAll @IntRange(min = 1, max = 7) int fullBlocks,
        @ForAll @IntRange(min = 1, max = 32767) int rawRemainder
    ) throws IOException {
        int remainder = (rawRemainder % (CACHE_BLOCK_SIZE - 1)) + 1;
        DirectMemoryAdmissionController ac = new DirectMemoryAdmissionController(1_000_000_000L, 0.85, 0.95, 5000);
        CaffeineBlockCacheV2 cache = new CaffeineBlockCacheV2(1024, ac);
        try {
            int fileSize = fullBlocks * CACHE_BLOCK_SIZE + remainder;
            Path tempFile = createTempFileWithSize(fileSize);
            String filePath = tempFile.toAbsolutePath().toString();

            long lastBlockId = fullBlocks;
            BlockCacheKeyV2 key = new BlockCacheKeyV2(filePath, lastBlockId);
            ByteBuffer buf = cache.getOrLoad(key);

            assertNotNull(buf, "Returned buffer should not be null");
            assertTrue(buf.isDirect(), "Returned buffer should be direct");
            assertTrue(buf.isReadOnly(), "Returned buffer should be read-only");
            assertEquals(remainder, buf.limit(),
                "Last partial block should have limit = remaining bytes (" + remainder + ")");
        } finally {
            cache.close();
        }
    }

    // ======================== Property 15 ========================

    /**
     * Property 15: Eviction nulls table entry and triggers fallback.
     * After eviction, RadixBlockTable.get(blockId) returns null;
     * next access reloads from Caffeine.
     *
     * Feature: bufferpool-v2-memory-management, Property 15: Eviction nulls table entry and triggers fallback
     * Validates: Requirements 5.1, 5.2, 5.7, 11.2
     */
    @Property(tries = 30)
    void evictionNullsTableEntry(
        @ForAll @IntRange(min = 1, max = 4) int numBlocks
    ) throws IOException {
        DirectMemoryAdmissionController ac = new DirectMemoryAdmissionController(1_000_000_000L, 0.85, 0.95, 5000);
        RadixBlockTableCache tableCache = new RadixBlockTableCache();
        CaffeineBlockCacheV2 cache = new CaffeineBlockCacheV2(1024, ac, new DirectMemoryAllocator(), tableCache);
        try {
            Path tempFile = createTempFile(numBlocks);
            String filePath = tempFile.toAbsolutePath().toString();

            // Acquire a shared table via the cache
            RadixBlockTable<MemorySegment> table = tableCache.acquire(filePath);

            // Load all blocks and populate table
            for (int blockId = 0; blockId < numBlocks; blockId++) {
                BlockCacheKeyV2 key = new BlockCacheKeyV2(filePath, blockId);
                ByteBuffer buf = cache.getOrLoad(key);
                MemorySegment seg = MemorySegment.ofBuffer(buf);
                table.put(blockId, seg);
                assertNotNull(table.get(blockId), "Table should have entry for block " + blockId);
            }

            // Invalidate all entries for this file — triggers eviction listener
            cache.invalidate(filePath);
            cache.cleanUp();

            boolean allNull = false;
            for (int attempt = 0; attempt < 20; attempt++) {
                allNull = true;
                for (int blockId = 0; blockId < numBlocks; blockId++) {
                    if (table.get(blockId) != null) {
                        allNull = false;
                        break;
                    }
                }
                if (allNull) break;
                try { Thread.sleep(50); } catch (InterruptedException e) { break; }
                cache.cleanUp();
            }

            for (int blockId = 0; blockId < numBlocks; blockId++) {
                assertNull(table.get(blockId),
                    "Table entry for block " + blockId + " should be null after eviction");
            }

            // Verify fallback: next getOrLoad reloads from disk
            BlockCacheKeyV2 key0 = new BlockCacheKeyV2(filePath, 0);
            ByteBuffer reloaded = cache.getOrLoad(key0);
            assertNotNull(reloaded, "Reloaded block should not be null");
            assertTrue(reloaded.isDirect(), "Reloaded block should be direct");

            tableCache.release(filePath);
        } finally {
            cache.close();
        }
    }

    // ======================== Property 16 ========================

    /**
     * Property 16: Eviction after table removed is graceful.
     * When the table has been released and removed, eviction listener
     * doesn't throw.
     *
     * Feature: bufferpool-v2-memory-management, Property 16: Graceful eviction after table removal
     * Validates: Requirements 6.3
     */
    @Property(tries = 30)
    void evictionAfterTableRemovedIsGraceful(
        @ForAll @IntRange(min = 1, max = 4) int numBlocks
    ) throws IOException {
        DirectMemoryAdmissionController ac = new DirectMemoryAdmissionController(1_000_000_000L, 0.85, 0.95, 5000);
        RadixBlockTableCache tableCache = new RadixBlockTableCache();
        CaffeineBlockCacheV2 cache = new CaffeineBlockCacheV2(1024, ac, new DirectMemoryAllocator(), tableCache);
        try {
            Path tempFile = createTempFile(numBlocks);
            String filePath = tempFile.toAbsolutePath().toString();

            // Acquire and immediately release so the table is removed
            tableCache.acquire(filePath);
            BlockCacheKeyV2 key = new BlockCacheKeyV2(filePath, 0);
            cache.getOrLoad(key);
            tableCache.release(filePath);

            // Invalidate triggers eviction listener — should not throw
            cache.invalidate(filePath);
            cache.cleanUp();
            // If we get here without exception, the test passes
        } finally {
            cache.close();
        }
    }

    // ======================== Property 17 ========================

    /**
     * Property 17: Reader safety after eviction.
     * Reader holding MemorySegment reference before eviction safely
     * completes read with correct data.
     *
     * Feature: bufferpool-v2-memory-management, Property 17: Reader safety after eviction
     * Validates: Requirements 5.6
     */
    @Property(tries = 30)
    void readerSafetyAfterEviction(
        @ForAll @IntRange(min = 2, max = 6) int numBlocks
    ) throws IOException {
        DirectMemoryAdmissionController ac = new DirectMemoryAdmissionController(1_000_000_000L, 0.85, 0.95, 5000);
        RadixBlockTableCache tableCache = new RadixBlockTableCache();
        // maxBlocks=1 so loading a new block evicts the old one
        CaffeineBlockCacheV2 cache = new CaffeineBlockCacheV2(1, ac, new DirectMemoryAllocator(), tableCache);
        try {
            Path tempFile = createTempFile(numBlocks);
            String filePath = tempFile.toAbsolutePath().toString();

            RadixBlockTable<MemorySegment> table = tableCache.acquire(filePath);

            // Load block 0 and get a MemorySegment reference
            BlockCacheKeyV2 key0 = new BlockCacheKeyV2(filePath, 0);
            ByteBuffer buf0 = cache.getOrLoad(key0);
            MemorySegment readerSegment = MemorySegment.ofBuffer(buf0);

            // Read expected data from disk for block 0
            byte[] expectedBlock0 = new byte[CACHE_BLOCK_SIZE];
            try (FileChannel ch = FileChannel.open(tempFile, StandardOpenOption.READ)) {
                ByteBuffer diskBuf = ByteBuffer.wrap(expectedBlock0);
                ch.position(0);
                while (diskBuf.hasRemaining()) {
                    if (ch.read(diskBuf) < 0) break;
                }
            }

            // Now load block 1 — this evicts block 0 from Caffeine
            BlockCacheKeyV2 key1 = new BlockCacheKeyV2(filePath, 1);
            cache.getOrLoad(key1);

            // The reader still holds readerSegment from block 0.
            for (int i = 0; i < CACHE_BLOCK_SIZE; i++) {
                byte actual = readerSegment.get(
                    java.lang.foreign.ValueLayout.JAVA_BYTE, i);
                assertEquals(expectedBlock0[i], actual,
                    "Reader should see correct data at offset " + i + " even after eviction");
            }

            tableCache.release(filePath);
        } finally {
            cache.close();
        }
    }
}
