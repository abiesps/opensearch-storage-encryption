/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import org.opensearch.index.store.bufferpoolfs.SparseLongBlockTable;

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
class CaffeineBlockCacheV2PropertyTests {

    private VirtualFileDescriptorRegistry vfdRegistry;

    @BeforeProperty
    void setUp() {
        vfdRegistry = VirtualFileDescriptorRegistry.getInstance();
        vfdRegistry.clear();
    }

    @AfterProperty
    void tearDown() {
        vfdRegistry.clear();
    }

    // ---- Helpers ----

    private Path createTempFile(int numBlocks) throws IOException {
        Path tempFile = Files.createTempFile("cache-prop-test-", ".dat");
        tempFile.toFile().deleteOnExit();
        int fileSize = numBlocks * CACHE_BLOCK_SIZE;
        byte[] data = new byte[fileSize];
        // Fill with deterministic pattern: byte at position i = (byte)(i % 251)
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
        // Use large maxDirectMemory so admission controller doesn't interfere
        DirectMemoryAdmissionController ac = new DirectMemoryAdmissionController(1_000_000_000L, 0.85, 0.95, 5000);
        CaffeineBlockCacheV2 cache = new CaffeineBlockCacheV2(1024, ac);
        try {
            Path tempFile = createTempFile(numBlocks);
            int vfd = vfdRegistry.register(tempFile.toAbsolutePath());

            for (int blockId = 0; blockId < numBlocks; blockId++) {
                BlockCacheKeyV2 key = new BlockCacheKeyV2(vfd, blockId);
                ByteBuffer buf = cache.getOrLoad(key);

                assertNotNull(buf, "Returned buffer should not be null");
                assertTrue(buf.isDirect(), "Returned buffer should be direct");
                assertTrue(buf.isReadOnly(), "Returned buffer should be read-only");
                assertEquals(CACHE_BLOCK_SIZE, buf.limit(),
                    "Full block should have limit = CACHE_BLOCK_SIZE");
            }

            vfdRegistry.deregister(vfd);
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
        // Clamp remainder to [1, CACHE_BLOCK_SIZE - 1] since annotation can't use non-constant
        int remainder = (rawRemainder % (CACHE_BLOCK_SIZE - 1)) + 1;
        DirectMemoryAdmissionController ac = new DirectMemoryAdmissionController(1_000_000_000L, 0.85, 0.95, 5000);
        CaffeineBlockCacheV2 cache = new CaffeineBlockCacheV2(1024, ac);
        try {
            int fileSize = fullBlocks * CACHE_BLOCK_SIZE + remainder;
            Path tempFile = createTempFileWithSize(fileSize);
            int vfd = vfdRegistry.register(tempFile.toAbsolutePath());

            // Load the last block
            long lastBlockId = fullBlocks; // 0-indexed, so fullBlocks is the last partial block
            BlockCacheKeyV2 key = new BlockCacheKeyV2(vfd, lastBlockId);
            ByteBuffer buf = cache.getOrLoad(key);

            assertNotNull(buf, "Returned buffer should not be null");
            assertTrue(buf.isDirect(), "Returned buffer should be direct");
            assertTrue(buf.isReadOnly(), "Returned buffer should be read-only");
            assertEquals(remainder, buf.limit(),
                "Last partial block should have limit = remaining bytes (" + remainder + ")");

            vfdRegistry.deregister(vfd);
        } finally {
            cache.close();
        }
    }

    // ======================== Property 15 ========================

    /**
     * Property 15: Eviction nulls table entry and triggers fallback.
     * After eviction, SparseLongBlockTable.get(blockId) returns null;
     * next access reloads from Caffeine.
     *
     * We test this by loading blocks, populating the table, then invalidating
     * the cache. After invalidation + cleanUp, the eviction listener should
     * have nulled the table entries.
     *
     * Feature: bufferpool-v2-memory-management, Property 15: Eviction nulls table entry and triggers fallback
     * Validates: Requirements 5.1, 5.2, 5.7, 11.2
     */
    @Property(tries = 30)
    void evictionNullsTableEntry(
        @ForAll @IntRange(min = 1, max = 4) int numBlocks
    ) throws IOException {
        DirectMemoryAdmissionController ac = new DirectMemoryAdmissionController(1_000_000_000L, 0.85, 0.95, 5000);
        CaffeineBlockCacheV2 cache = new CaffeineBlockCacheV2(1024, ac);
        try {
            Path tempFile = createTempFile(numBlocks);
            int vfd = vfdRegistry.register(tempFile.toAbsolutePath());

            SparseLongBlockTable table = new SparseLongBlockTable(4);
            cache.registerTable(vfd, table);

            // Load all blocks and populate table
            for (int blockId = 0; blockId < numBlocks; blockId++) {
                BlockCacheKeyV2 key = new BlockCacheKeyV2(vfd, blockId);
                ByteBuffer buf = cache.getOrLoad(key);
                MemorySegment seg = MemorySegment.ofBuffer(buf);
                table.put(blockId, seg);
                assertNotNull(table.get(blockId), "Table should have entry for block " + blockId);
            }

            // Invalidate all entries for this vfd — triggers eviction listener
            cache.invalidate(vfd);
            cache.cleanUp();

            // Give Caffeine a moment to process async eviction listeners
            // and retry a few times since eviction is asynchronous
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
                cache.cleanUp(); // re-trigger pending listener processing
            }

            for (int blockId = 0; blockId < numBlocks; blockId++) {
                assertNull(table.get(blockId),
                    "Table entry for block " + blockId + " should be null after eviction");
            }

            // Verify fallback: next getOrLoad reloads from disk
            BlockCacheKeyV2 key0 = new BlockCacheKeyV2(vfd, 0);
            ByteBuffer reloaded = cache.getOrLoad(key0);
            assertNotNull(reloaded, "Reloaded block should not be null");
            assertTrue(reloaded.isDirect(), "Reloaded block should be direct");

            vfdRegistry.deregister(vfd);
        } finally {
            cache.close();
        }
    }

    // ======================== Property 16 ========================

    /**
     * Property 16: Stale WeakReference cleanup.
     * When WeakRef is cleared, eviction listener doesn't throw and
     * opportunistically removes stale entry.
     *
     * Feature: bufferpool-v2-memory-management, Property 16: Stale WeakReference cleanup
     * Validates: Requirements 5.4, 5.5
     */
    @Property(tries = 30)
    void staleWeakReferenceCleanup(
        @ForAll @IntRange(min = 1, max = 4) int numBlocks
    ) throws IOException {
        DirectMemoryAdmissionController ac = new DirectMemoryAdmissionController(1_000_000_000L, 0.85, 0.95, 5000);
        CaffeineBlockCacheV2 cache = new CaffeineBlockCacheV2(1024, ac);
        try {
            Path tempFile = createTempFile(numBlocks);
            int vfd = vfdRegistry.register(tempFile.toAbsolutePath());

            // Register a table, then clear the WeakReference to simulate GC
            SparseLongBlockTable table = new SparseLongBlockTable(4);
            cache.registerTable(vfd, table);

            // Load a block so there's something to evict
            BlockCacheKeyV2 key = new BlockCacheKeyV2(vfd, 0);
            cache.getOrLoad(key);

            // Now null out the table reference and force GC to clear the WeakReference
            table = null;
            for (int i = 0; i < 10; i++) {
                System.gc();
                Thread.sleep(50);
            }

            // Invalidate the cache entry — this triggers the eviction listener
            // which should encounter a cleared WeakReference and NOT throw
            cache.invalidate(vfd);
            cache.close();

            // If we get here without exception, the test passes
            vfdRegistry.deregister(vfd);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            // cache already closed
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
        // maxBlocks=1 so loading a new block evicts the old one
        CaffeineBlockCacheV2 cache = new CaffeineBlockCacheV2(1, ac);
        try {
            Path tempFile = createTempFile(numBlocks);
            int vfd = vfdRegistry.register(tempFile.toAbsolutePath());

            SparseLongBlockTable table = new SparseLongBlockTable(4);
            cache.registerTable(vfd, table);

            // Load block 0 and get a MemorySegment reference
            BlockCacheKeyV2 key0 = new BlockCacheKeyV2(vfd, 0);
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
            BlockCacheKeyV2 key1 = new BlockCacheKeyV2(vfd, 1);
            cache.getOrLoad(key1);

            // The reader still holds readerSegment from block 0.
            // It should still be readable with correct data because the
            // MemorySegment keeps the underlying ByteBuffer reachable.
            for (int i = 0; i < CACHE_BLOCK_SIZE; i++) {
                byte actual = readerSegment.get(
                    java.lang.foreign.ValueLayout.JAVA_BYTE, i);
                assertEquals(expectedBlock0[i], actual,
                    "Reader should see correct data at offset " + i + " even after eviction");
            }

            vfdRegistry.deregister(vfd);
        } finally {
            cache.close();
        }
    }
}
