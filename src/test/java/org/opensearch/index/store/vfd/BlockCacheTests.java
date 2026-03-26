/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;

import org.opensearch.index.store.bufferpoolfs.RadixBlockTable;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link BlockCache} — put/get round-trip, eviction triggers
 * L1 cascade, metrics accuracy, invalidate, estimatedSize, and close.
 *
 * <p><b>Validates: Requirements 23, 24</b></p>
 */
@SuppressWarnings("preview")
public class BlockCacheTests extends OpenSearchTestCase {

    private static final int FD_LIMIT = 10;
    private static final int FD_BURST = 5;

    private Path tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir("block-cache-test");
    }

    private Path createTestFile(String name) throws IOException {
        Path file = tempDir.resolve(name);
        Files.write(file, new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
        return file;
    }

    private static MemorySegment segment(int size) {
        return MemorySegment.ofArray(new byte[size]);
    }

    /**
     * Polls a condition in a tight loop with short sleeps until it returns true
     * or the timeout expires.
     */
    private static void awaitCondition(java.util.function.BooleanSupplier condition, long timeoutMs)
            throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        // Final attempt
        condition.getAsBoolean();
    }

    // =========================================================================
    // put/get round-trip
    // =========================================================================

    public void testPutGetRoundTrip() throws Exception {
        try (VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST);
             BlockCache cache = new BlockCache(100, registry)) {
            MemorySegment seg = segment(8192);
            long key = VirtualPage.encode(1, 0);

            cache.put(key, seg);
            MemorySegment result = cache.get(key);

            assertSame("get should return the same MemorySegment reference", seg, result);
        }
    }

    public void testPutGetMultipleKeys() throws Exception {
        try (VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST);
             BlockCache cache = new BlockCache(100, registry)) {
            MemorySegment seg1 = segment(8192);
            MemorySegment seg2 = segment(8192);
            long key1 = VirtualPage.encode(1, 0);
            long key2 = VirtualPage.encode(1, 1);

            cache.put(key1, seg1);
            cache.put(key2, seg2);

            assertSame(seg1, cache.get(key1));
            assertSame(seg2, cache.get(key2));
        }
    }

    // =========================================================================
    // get miss returns null and increments missCount
    // =========================================================================

    public void testGetMissReturnsNullAndIncrementsMissCount() throws Exception {
        try (VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST);
             BlockCache cache = new BlockCache(100, registry)) {
            long key = VirtualPage.encode(1, 0);

            MemorySegment result = cache.get(key);

            assertNull("get on missing key should return null", result);
            assertEquals(1, cache.metrics().missCount.sum());
            assertEquals(0, cache.metrics().hitCount.sum());
        }
    }

    // =========================================================================
    // get hit increments hitCount
    // =========================================================================

    public void testGetHitIncrementsHitCount() throws Exception {
        try (VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST);
             BlockCache cache = new BlockCache(100, registry)) {
            long key = VirtualPage.encode(1, 0);
            cache.put(key, segment(8192));

            cache.get(key);

            assertEquals(1, cache.metrics().hitCount.sum());
            assertEquals(0, cache.metrics().missCount.sum());
        }
    }

    // =========================================================================
    // Eviction triggers L1 cascade
    // =========================================================================

    public void testEvictionTriggersL1CascadeRemoval() throws Exception {
        try (VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST)) {
            // Acquire a VFD so the registry creates a RadixBlockTable for it
            Path file = createTestFile("cascade.dat");
            VirtualFileDescriptor vfd = registry.acquire(file);
            long vfdId = vfd.id();

            RadixBlockTable<MemorySegment> rbt = registry.getRadixBlockTable(vfdId);
            assertNotNull("RadixBlockTable should exist for acquired VFD", rbt);

            // Use maxBlocks=2 to force eviction when we insert a 3rd entry
            try (BlockCache cache = new BlockCache(2, registry)) {
                // Put entries in both L1 (RadixBlockTable) and L2 (BlockCache)
                for (int blockId = 0; blockId < 3; blockId++) {
                    long key = VirtualPage.encode(vfdId, blockId);
                    MemorySegment seg = segment(8192);
                    rbt.put(blockId, seg);
                    cache.put(key, seg);
                }

                // Force Caffeine maintenance to trigger eviction
                cache.cleanUp();

                // Poll for eviction to complete
                awaitCondition(() -> {
                    cache.cleanUp();
                    return cache.metrics().evictionCount.sum() > 0;
                }, 2000);

                // At least one entry should have been evicted from L2
                assertTrue("evictionCount should be > 0", cache.metrics().evictionCount.sum() > 0);

                // The evicted entry's block should have been removed from L1
                assertTrue("l1CascadeRemovalCount should be > 0",
                    cache.metrics().l1CascadeRemovalCount.sum() > 0);

                // Verify at least one L1 entry was actually removed
                int nullCount = 0;
                for (int blockId = 0; blockId < 3; blockId++) {
                    if (rbt.get(blockId) == null) {
                        nullCount++;
                    }
                }
                assertTrue("At least one L1 entry should have been removed by cascade", nullCount > 0);
            }

            registry.release(vfd);
        }
    }

    public void testEvictionCascadeSkipsClosedVfd() throws Exception {
        try (VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST)) {
            // Use a VFD id that has no RadixBlockTable registered
            // (simulates VFD already closed)
            try (BlockCache cache = new BlockCache(1, registry)) {
                // Use a vfdId that doesn't exist in the registry
                long fakeVfdId = 999;
                long key1 = VirtualPage.encode(fakeVfdId, 0);
                long key2 = VirtualPage.encode(fakeVfdId, 1);

                cache.put(key1, segment(8192));
                cache.put(key2, segment(8192));
                cache.cleanUp();

                // Poll for eviction to complete
                awaitCondition(() -> {
                    cache.cleanUp();
                    return cache.metrics().evictionCount.sum() > 0;
                }, 2000);

                // Eviction should have happened but L1 cascade should be 0
                // (no RadixBlockTable found for the fake VFD id)
                assertTrue("evictionCount should be > 0", cache.metrics().evictionCount.sum() > 0);
                assertEquals("l1CascadeRemovalCount should be 0 when VFD is closed",
                    0, cache.metrics().l1CascadeRemovalCount.sum());
            }
        }
    }

    // =========================================================================
    // Metrics accuracy
    // =========================================================================

    public void testMetricsAccuracyAfterVariousOperations() throws Exception {
        try (VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST);
             BlockCache cache = new BlockCache(100, registry)) {
            long key1 = VirtualPage.encode(1, 0);
            long key2 = VirtualPage.encode(1, 1);
            long key3 = VirtualPage.encode(1, 2);

            // 3 misses
            cache.get(key1);
            cache.get(key2);
            cache.get(key3);
            assertEquals(3, cache.metrics().missCount.sum());
            assertEquals(0, cache.metrics().hitCount.sum());

            // Put 2 entries, then 2 hits
            cache.put(key1, segment(8192));
            cache.put(key2, segment(8192));
            cache.get(key1);
            cache.get(key2);
            assertEquals(2, cache.metrics().hitCount.sum());
            assertEquals(3, cache.metrics().missCount.sum());

            // 1 more miss
            cache.get(key3);
            assertEquals(2, cache.metrics().hitCount.sum());
            assertEquals(4, cache.metrics().missCount.sum());
        }
    }

    public void testMetricsEvictionCount() throws Exception {
        try (VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST);
             BlockCache cache = new BlockCache(2, registry)) {
            // Insert 3 entries into a cache with maxBlocks=2
            cache.put(VirtualPage.encode(1, 0), segment(8192));
            cache.put(VirtualPage.encode(1, 1), segment(8192));
            cache.put(VirtualPage.encode(1, 2), segment(8192));

            cache.cleanUp();
            awaitCondition(() -> {
                cache.cleanUp();
                return cache.metrics().evictionCount.sum() > 0;
            }, 2000);

            assertTrue("evictionCount should be > 0 after exceeding maxBlocks",
                cache.metrics().evictionCount.sum() > 0);
        }
    }

    public void testCacheMetricsToString() throws Exception {
        try (VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST);
             BlockCache cache = new BlockCache(100, registry)) {
            String str = cache.metrics().toString();
            assertTrue(str.contains("hits="));
            assertTrue(str.contains("misses="));
            assertTrue(str.contains("evictions="));
            assertTrue(str.contains("l1Cascades="));
        }
    }

    // =========================================================================
    // invalidate removes entry
    // =========================================================================

    public void testInvalidateRemovesEntry() throws Exception {
        try (VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST);
             BlockCache cache = new BlockCache(100, registry)) {
            long key = VirtualPage.encode(1, 0);
            cache.put(key, segment(8192));

            assertNotNull(cache.get(key));

            cache.invalidate(key);
            cache.cleanUp();
            awaitCondition(() -> {
                cache.cleanUp();
                return cache.get(key) == null;
            }, 2000);

            assertNull("Entry should be removed after invalidate", cache.get(key));
        }
    }

    // =========================================================================
    // estimatedSize reflects cache contents
    // =========================================================================

    public void testEstimatedSizeReflectsCacheContents() throws Exception {
        try (VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST);
             BlockCache cache = new BlockCache(100, registry)) {
            assertEquals(0, cache.estimatedSize());

            cache.put(VirtualPage.encode(1, 0), segment(8192));
            cache.put(VirtualPage.encode(1, 1), segment(8192));
            cache.cleanUp();

            assertEquals(2, cache.estimatedSize());

            cache.put(VirtualPage.encode(1, 2), segment(8192));
            cache.cleanUp();

            assertEquals(3, cache.estimatedSize());
        }
    }

    // =========================================================================
    // close() cleans up properly
    // =========================================================================

    public void testCloseInvalidatesAllEntries() throws Exception {
        VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST);
        BlockCache cache = new BlockCache(100, registry);

        cache.put(VirtualPage.encode(1, 0), segment(8192));
        cache.put(VirtualPage.encode(1, 1), segment(8192));
        cache.cleanUp();
        assertEquals(2, cache.estimatedSize());

        cache.close();

        assertEquals(0, cache.estimatedSize());

        registry.close();
    }

    public void testCloseIsIdempotent() throws Exception {
        VfdRegistry registry = new VfdRegistry(FD_LIMIT, FD_BURST);
        BlockCache cache = new BlockCache(100, registry);

        cache.put(VirtualPage.encode(1, 0), segment(8192));
        cache.close();
        cache.close(); // should not throw

        assertEquals(0, cache.estimatedSize());

        registry.close();
    }
}
