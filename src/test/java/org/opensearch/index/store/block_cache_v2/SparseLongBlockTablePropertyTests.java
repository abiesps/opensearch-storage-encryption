/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

import org.opensearch.index.store.bufferpoolfs.SparseLongBlockTable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for SparseLongBlockTable (bitmap+popcount).
 *
 * PAGE_SIZE = 1 &lt;&lt; PAGE_SHIFT = 64 entries per page.
 * All single-page tests must keep blockIds in [0, PAGE_SIZE).
 *
 * Feature: bufferpool-v2-memory-management
 */
class SparseLongBlockTablePropertyTests {

    private static final int PAGE_SIZE = 1 << SparseLongBlockTable.PAGE_SHIFT; // 64

    // ---- Property 4: put/get round trip ----

    @Property(tries = 500)
    void putGetRoundTrip(
            @ForAll @LongRange(min = 0, max = 100_000) long blockId) {

        SparseLongBlockTable table = new SparseLongBlockTable(1);
        byte[] data = new byte[64];
        data[0] = (byte) (blockId & 0xFF);
        MemorySegment seg = MemorySegment.ofArray(data);

        assertNull(table.get(blockId), "get before put must return null");
        table.put(blockId, seg);
        MemorySegment result = table.get(blockId);
        assertNotNull(result, "get after put must return non-null");
        assertSame(seg, result, "get must return the exact segment that was put");
    }

    @Property(tries = 200)
    void multipleEntriesRoundTrip(
            @ForAll @IntRange(min = 1, max = 20) int count) {

        SparseLongBlockTable table = new SparseLongBlockTable(1);
        MemorySegment[] segments = new MemorySegment[count];

        for (int i = 0; i < count; i++) {
            long blockId = (long) i * PAGE_SIZE + i; // different pages
            byte[] data = new byte[32];
            data[0] = (byte) i;
            segments[i] = MemorySegment.ofArray(data);
            table.put(blockId, segments[i]);
        }

        for (int i = 0; i < count; i++) {
            long blockId = (long) i * PAGE_SIZE + i;
            assertSame(segments[i], table.get(blockId),
                    "Entry " + i + " must round-trip");
        }
    }

    // ---- Property 5: chunkPopCount accuracy ----

    @Property(tries = 300)
    void chunkPopCountAccuracy(
            @ForAll @IntRange(min = 1, max = 500) int numOps) {

        SparseLongBlockTable table = new SparseLongBlockTable(1);
        Set<Long> presentIds = new HashSet<>();

        for (int i = 0; i < numOps; i++) {
            long blockId = i % PAGE_SIZE; // stays in page 0 (0..63)
            if (presentIds.contains(blockId)) {
                table.remove(blockId);
                presentIds.remove(blockId);
            } else {
                table.put(blockId, MemorySegment.ofArray(new byte[16]));
                presentIds.add(blockId);
            }
        }

        int expectedCount = presentIds.size();
        int actualCount = table.getChunkPopCount(0);
        if (expectedCount == 0) {
            assertTrue(actualCount == 0 || actualCount == -1,
                    "popcount must be 0 or -1 (reclaimed) when empty, got " + actualCount);
        } else {
            assertEquals(expectedCount, actualCount,
                    "chunkPopCount must equal number of non-null entries");
        }
    }

    @Property(tries = 100)
    void chunkPopCountFillAndDrain(
            @ForAll @IntRange(min = 1, max = 64) int fillCount) {

        int fc = Math.min(fillCount, PAGE_SIZE);
        SparseLongBlockTable table = new SparseLongBlockTable(1);

        for (int i = 0; i < fc; i++) {
            table.put(i, MemorySegment.ofArray(new byte[8]));
            assertEquals(i + 1, table.getChunkPopCount(0),
                    "PopCount after " + (i + 1) + " puts");
        }

        for (int i = 0; i < fc; i++) {
            table.remove(i);
            int expected = fc - i - 1;
            int actual = table.getChunkPopCount(0);
            if (expected == 0) {
                assertTrue(actual == 0 || actual == -1,
                        "PopCount after full drain must be 0 or -1, got " + actual);
            } else {
                assertEquals(expected, actual,
                        "PopCount after " + (i + 1) + " removes");
            }
        }
    }

    // ---- Property 6: Chunk deallocation after full eviction ----

    @Property(tries = 200)
    void chunkDeallocatedAfterFullEviction(
            @ForAll @IntRange(min = 1, max = 64) int fillCount) {

        int fc = Math.min(fillCount, PAGE_SIZE);
        SparseLongBlockTable table = new SparseLongBlockTable(1);

        for (int i = 0; i < fc; i++) {
            table.put(i, MemorySegment.ofArray(new byte[8]));
        }
        assertTrue(table.isChunkAllocated(0), "Chunk must be allocated after puts");

        for (int i = 0; i < fc; i++) {
            table.remove(i);
        }
        assertFalse(table.isChunkAllocated(0),
                "Chunk must be deallocated after all entries removed");
    }

    @Property(tries = 200)
    void chunkNotDeallocatedAfterPartialEviction(
            @ForAll @IntRange(min = 2, max = 64) int fillCount) {

        int fc = Math.min(fillCount, PAGE_SIZE);
        SparseLongBlockTable table = new SparseLongBlockTable(1);

        for (int i = 0; i < fc; i++) {
            table.put(i, MemorySegment.ofArray(new byte[8]));
        }

        for (int i = 0; i < fc - 1; i++) {
            table.remove(i);
        }

        assertTrue(table.isChunkAllocated(0),
                "Chunk must remain allocated when entries still present");
        assertEquals(1, table.getChunkPopCount(0),
                "PopCount must be 1 with one entry remaining");
    }

    // ---- Property 7: VarHandle ordering ----

    @Property(tries = 20)
    void varHandleOrderingNoPartialReferences(
            @ForAll @IntRange(min = 2, max = 8) int writerCount,
            @ForAll @IntRange(min = 2, max = 8) int readerCount) throws Exception {

        int wc = Math.min(writerCount, 8);
        int rc = Math.min(readerCount, 8);

        SparseLongBlockTable table = new SparseLongBlockTable(4);
        int entriesPerWriter = 50;
        AtomicBoolean sawPartial = new AtomicBoolean(false);
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch startLatch = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(wc + rc);
        List<Future<?>> futures = new ArrayList<>();

        for (int w = 0; w < wc; w++) {
            final int writerId = w;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < entriesPerWriter; i++) {
                        long blockId = (long) writerId * PAGE_SIZE + (i % PAGE_SIZE);
                        byte[] data = new byte[32];
                        data[0] = (byte) writerId;
                        data[1] = (byte) i;
                        for (int j = 2; j < data.length; j++) {
                            data[j] = (byte) 0xAB;
                        }
                        table.put(blockId, MemorySegment.ofArray(data));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        final int finalWc = wc;
        for (int r = 0; r < rc; r++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    while (running.get() && !sawPartial.get()) {
                        for (int w = 0; w < finalWc; w++) {
                            for (int i = 0; i < entriesPerWriter; i++) {
                                long blockId = (long) w * PAGE_SIZE + (i % PAGE_SIZE);
                                MemorySegment seg = table.get(blockId);
                                if (seg != null) {
                                    byte sentinel = seg.get(
                                            java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
                                    if (sentinel < 0) sawPartial.set(true);
                                    byte fill = seg.get(
                                            java.lang.foreign.ValueLayout.JAVA_BYTE, 2);
                                    if (fill != (byte) 0xAB) sawPartial.set(true);
                                }
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        startLatch.countDown();

        for (int i = 0; i < wc; i++) {
            futures.get(i).get();
        }
        running.set(false);

        for (int i = wc; i < futures.size(); i++) {
            futures.get(i).get();
        }

        executor.shutdown();

        assertFalse(sawPartial.get(),
                "Readers must never see partially initialized MemorySegment data");
    }
}
