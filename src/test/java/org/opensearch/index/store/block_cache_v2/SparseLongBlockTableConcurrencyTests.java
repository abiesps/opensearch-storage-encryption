/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.opensearch.index.store.bufferpoolfs.SparseLongBlockTable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Aggressive concurrency tests for SparseLongBlockTable.
 *
 * These tests exercise the full concurrency model: VarHandle ordering,
 * page allocation races, directory growth, and sustained stress.
 *
 * Feature: bufferpool-v2-memory-management
 * Requirements: 3.15a, 3.15b, 3.15c, 3.15d, 3.15e, 3.15f
 */
class SparseLongBlockTableConcurrencyTests {

    private static final int THREAD_COUNT = 32;
    private static final int BLOCK_ID_RANGE = 2048;
    private static final int PAGE_SIZE = 1 << SparseLongBlockTable.PAGE_SHIFT; // 64

    private static MemorySegment createSentinelSegment(long blockId) {
        byte[] data = new byte[32];
        for (int i = 0; i < 8; i++) {
            data[i] = (byte) ((blockId >>> (56 - i * 8)) & 0xFF);
        }
        for (int i = 8; i < data.length; i++) {
            data[i] = (byte) 0xAB;
        }
        return MemorySegment.ofArray(data);
    }

    private static long readSentinel(MemorySegment seg) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (seg.get(ValueLayout.JAVA_BYTE, i) & 0xFFL);
        }
        return value;
    }

    private static boolean hasSentinelFill(MemorySegment seg) {
        for (int i = 8; i < 32; i++) {
            if (seg.get(ValueLayout.JAVA_BYTE, i) != (byte) 0xAB) {
                return false;
            }
        }
        return true;
    }

    // ---- Property 8: Concurrent put/get safety ----

    @Test
    void concurrentPutGetSafety() throws Exception {
        int writerCount = 16;
        int readerCount = 16;
        int totalThreads = writerCount + readerCount;
        int opsPerWriter = 500;

        SparseLongBlockTable table = new SparseLongBlockTable(4);
        AtomicBoolean corruptionDetected = new AtomicBoolean(false);
        AtomicReference<String> corruptionDetail = new AtomicReference<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch writersDone = new CountDownLatch(writerCount);
        AtomicBoolean readersStop = new AtomicBoolean(false);

        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        List<Future<?>> futures = new ArrayList<>();

        for (int w = 0; w < writerCount; w++) {
            final int writerId = w;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerWriter; i++) {
                        long blockId = (long) writerId * 128 + (i % 128);
                        MemorySegment seg = createSentinelSegment(blockId);
                        table.put(blockId, seg);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    writersDone.countDown();
                }
            }));
        }

        for (int r = 0; r < readerCount; r++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    while (!readersStop.get() && !corruptionDetected.get()) {
                        long blockId = ThreadLocalRandom.current().nextLong(0, BLOCK_ID_RANGE);
                        MemorySegment seg = table.get(blockId);
                        if (seg != null) {
                            if (!hasSentinelFill(seg)) {
                                corruptionDetected.set(true);
                                corruptionDetail.set("Corrupted fill at blockId=" + blockId);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    corruptionDetected.set(true);
                    corruptionDetail.set("Exception in reader: " + e.getMessage());
                }
            }));
        }

        startLatch.countDown();
        writersDone.await(30, TimeUnit.SECONDS);
        readersStop.set(true);

        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertFalse(corruptionDetected.get(),
                "Readers must never see corrupted references: " + corruptionDetail.get());
    }

    // ---- Property 9: Concurrent put/remove safety ----

    @Test
    void concurrentPutRemoveSafety() throws Exception {
        int writerCount = 16;
        int removerCount = 16;
        int totalThreads = writerCount + removerCount;
        int opsPerThread = 500;

        SparseLongBlockTable table = new SparseLongBlockTable(4);
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        AtomicReference<String> exceptionDetail = new AtomicReference<>();
        CountDownLatch startLatch = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        List<Future<?>> futures = new ArrayList<>();

        for (int w = 0; w < writerCount; w++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        long blockId = ThreadLocalRandom.current().nextLong(0, 512);
                        MemorySegment seg = createSentinelSegment(blockId);
                        table.put(blockId, seg);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    exceptionThrown.set(true);
                    exceptionDetail.set("Writer exception: " + e.getMessage());
                }
            }));
        }

        for (int r = 0; r < removerCount; r++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        long blockId = ThreadLocalRandom.current().nextLong(0, 512);
                        MemorySegment removed = table.remove(blockId);
                        if (removed != null && !hasSentinelFill(removed)) {
                            exceptionThrown.set(true);
                            exceptionDetail.set("Corrupted segment returned from remove at blockId=" + blockId);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    exceptionThrown.set(true);
                    exceptionDetail.set("Remover exception: " + e.getMessage());
                }
            }));
        }

        startLatch.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertFalse(exceptionThrown.get(),
                "No exceptions or corruption during concurrent put/remove: " + exceptionDetail.get());

        for (long blockId = 0; blockId < 512; blockId++) {
            MemorySegment seg = table.get(blockId);
            if (seg != null) {
                assertTrue(hasSentinelFill(seg),
                        "Remaining entry at blockId=" + blockId + " must have valid sentinel fill");
            }
        }
    }

    // ---- Property 10: Concurrent clear safety ----

    @Test
    void concurrentClearSafety() throws Exception {
        int writerCount = 12;
        int readerCount = 12;
        int clearCount = 8;
        int totalThreads = writerCount + readerCount + clearCount;
        int opsPerThread = 300;

        SparseLongBlockTable table = new SparseLongBlockTable(4);
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        AtomicReference<String> exceptionDetail = new AtomicReference<>();
        CountDownLatch startLatch = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        List<Future<?>> futures = new ArrayList<>();

        // Writers — put() is synchronized so no NPE from clear() races
        for (int w = 0; w < writerCount; w++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        long blockId = ThreadLocalRandom.current().nextLong(0, 512);
                        table.put(blockId, createSentinelSegment(blockId));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    exceptionThrown.set(true);
                    exceptionDetail.set("Writer exception: " + e);
                }
            }));
        }

        // Readers
        for (int r = 0; r < readerCount; r++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        long blockId = ThreadLocalRandom.current().nextLong(0, 512);
                        MemorySegment seg = table.get(blockId);
                        if (seg != null && !hasSentinelFill(seg)) {
                            exceptionThrown.set(true);
                            exceptionDetail.set("Corrupted segment at blockId=" + blockId);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    exceptionThrown.set(true);
                    exceptionDetail.set("Reader exception: " + e);
                }
            }));
        }

        // Clear threads
        for (int c = 0; c < clearCount; c++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread / 10; i++) {
                        table.clear();
                        Thread.yield();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    exceptionThrown.set(true);
                    exceptionDetail.set("Clear exception: " + e);
                }
            }));
        }

        startLatch.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertFalse(exceptionThrown.get(),
                "No exceptions during concurrent clear with readers/writers: " + exceptionDetail.get());
    }

    // ---- Property 11: Concurrent page allocation ----

    @Test
    void concurrentChunkAllocation() throws Exception {
        SparseLongBlockTable table = new SparseLongBlockTable(1);
        int threadCount = THREAD_COUNT;
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        AtomicReference<String> exceptionDetail = new AtomicReference<>();

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        // All threads try to put into the same page (blockIds 0 to PAGE_SIZE-1)
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    long blockId = threadId % PAGE_SIZE;
                    MemorySegment seg = createSentinelSegment(blockId);
                    table.put(blockId, seg);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    exceptionThrown.set(true);
                    exceptionDetail.set("Thread " + threadId + " exception: " + e);
                }
            }));
        }

        startLatch.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertFalse(exceptionThrown.get(),
                "No exceptions during concurrent page allocation: " + exceptionDetail.get());

        assertTrue(table.isChunkAllocated(0), "Page 0 must be allocated");
        for (int t = 0; t < Math.min(threadCount, PAGE_SIZE); t++) {
            MemorySegment seg = table.get(t);
            if (seg != null) {
                assertTrue(hasSentinelFill(seg),
                        "Entry at blockId=" + t + " must have valid sentinel fill");
            }
        }
    }

    // ---- Property 12: Concurrent directory growth ----

    @Test
    void concurrentDirectoryGrowth() throws Exception {
        SparseLongBlockTable table = new SparseLongBlockTable(1);
        int threadCount = THREAD_COUNT;
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        AtomicReference<String> exceptionDetail = new AtomicReference<>();

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        // Each thread targets a different page (blockId = threadId * PAGE_SIZE)
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    long blockId = (long) threadId * PAGE_SIZE;
                    MemorySegment seg = createSentinelSegment(blockId);
                    table.put(blockId, seg);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    exceptionThrown.set(true);
                    exceptionDetail.set("Thread " + threadId + " exception: " + e);
                }
            }));
        }

        startLatch.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertFalse(exceptionThrown.get(),
                "No exceptions during concurrent directory growth: " + exceptionDetail.get());

        for (int t = 0; t < threadCount; t++) {
            long blockId = (long) t * PAGE_SIZE;
            MemorySegment seg = table.get(blockId);
            if (seg == null) {
                fail("Entry at blockId=" + blockId + " (page " + t + ") lost during directory growth");
            }
            assertTrue(hasSentinelFill(seg),
                    "Entry at blockId=" + blockId + " must have valid sentinel fill");
            long storedId = readSentinel(seg);
            assertEquals(blockId, storedId,
                    "Sentinel blockId must match for page " + t);
        }
    }

    // ---- Property 13: Sustained stress ----

    @Test
    void sustainedStress() throws Exception {
        int threadCount = THREAD_COUNT;
        long durationMs = 10_000;

        SparseLongBlockTable table = new SparseLongBlockTable(4);
        AtomicBoolean corruptionDetected = new AtomicBoolean(false);
        AtomicReference<String> corruptionDetail = new AtomicReference<>();
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger totalOps = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    ThreadLocalRandom rng = ThreadLocalRandom.current();
                    int localOps = 0;

                    while (running.get() && !corruptionDetected.get()) {
                        long blockId = rng.nextLong(0, BLOCK_ID_RANGE);
                        int op = rng.nextInt(3);

                        switch (op) {
                            case 0:
                                table.put(blockId, createSentinelSegment(blockId));
                                break;
                            case 1:
                                MemorySegment seg = table.get(blockId);
                                if (seg != null && !hasSentinelFill(seg)) {
                                    corruptionDetected.set(true);
                                    corruptionDetail.set(
                                            "Corrupted fill on get at blockId=" + blockId);
                                }
                                break;
                            case 2:
                                MemorySegment removed = table.remove(blockId);
                                if (removed != null && !hasSentinelFill(removed)) {
                                    corruptionDetected.set(true);
                                    corruptionDetail.set(
                                            "Corrupted fill on remove at blockId=" + blockId);
                                }
                                break;
                        }
                        localOps++;
                    }
                    totalOps.addAndGet(localOps);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    corruptionDetected.set(true);
                    corruptionDetail.set("Exception in stress thread: " + e);
                }
            }));
        }

        startLatch.countDown();
        Thread.sleep(durationMs);
        running.set(false);

        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertFalse(corruptionDetected.get(),
                "No corruption during sustained stress: " + corruptionDetail.get());

        for (long blockId = 0; blockId < BLOCK_ID_RANGE; blockId++) {
            MemorySegment seg = table.get(blockId);
            if (seg != null) {
                assertTrue(hasSentinelFill(seg),
                        "Post-stress entry at blockId=" + blockId + " must have valid sentinel fill");
            }
        }

        assertTrue(totalOps.get() > 10_000,
                "Expected significant operation count, got " + totalOps.get());
    }

    // ---- Property 14: Eviction-heavy stress (simulates Caffeine eviction thread) ----

    /**
     * Simulates the real-world pattern where Caffeine's eviction thread
     * aggressively removes entries from the SparseLongBlockTable while
     * writer threads continuously add new entries and reader threads
     * perform lookups.
     *
     * Key invariants verified:
     * - Readers never see corrupted/partial MemorySegment data
     * - Pages that become empty (bitmap == 0) are reclaimed (nulled)
     * - No exceptions (NPE, AIOOBE) from concurrent page mutations
     * - After all evictors finish, remaining entries are valid
     *
     * This specifically targets the bitmap clear + compact array shrink
     * path in remove(), including the case where the last bit is cleared
     * and the page reference is set to null.
     */
    @Test
    void concurrentEvictionHeavyStress() throws Exception {
        int writerCount = 8;
        int evictorCount = 16; // evictors outnumber writers — simulates aggressive eviction
        int readerCount = 8;
        int totalThreads = writerCount + evictorCount + readerCount;
        long durationMs = 10_000;

        // Use a small block ID range to maximize contention on the same pages
        int blockIdRange = PAGE_SIZE * 4; // 4 pages worth of entries

        SparseLongBlockTable table = new SparseLongBlockTable(4);
        AtomicBoolean corruptionDetected = new AtomicBoolean(false);
        AtomicReference<String> corruptionDetail = new AtomicReference<>();
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger totalEvictions = new AtomicInteger(0);
        AtomicInteger totalPuts = new AtomicInteger(0);
        AtomicInteger totalReads = new AtomicInteger(0);
        AtomicInteger pagesFullyEvicted = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        List<Future<?>> futures = new ArrayList<>();

        // Writer threads: continuously put entries with sentinel data
        for (int w = 0; w < writerCount; w++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    ThreadLocalRandom rng = ThreadLocalRandom.current();
                    while (running.get() && !corruptionDetected.get()) {
                        long blockId = rng.nextLong(0, blockIdRange);
                        MemorySegment seg = createSentinelSegment(blockId);
                        table.put(blockId, seg);
                        totalPuts.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    corruptionDetected.set(true);
                    corruptionDetail.set("Writer exception: " + e);
                }
            }));
        }

        // Evictor threads: aggressively remove entries, simulating Caffeine eviction
        for (int ev = 0; ev < evictorCount; ev++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    ThreadLocalRandom rng = ThreadLocalRandom.current();
                    while (running.get() && !corruptionDetected.get()) {
                        long blockId = rng.nextLong(0, blockIdRange);
                        MemorySegment removed = table.remove(blockId);
                        if (removed != null) {
                            // Verify the removed segment has valid sentinel fill
                            if (!hasSentinelFill(removed)) {
                                corruptionDetected.set(true);
                                corruptionDetail.set(
                                        "Corrupted segment returned from eviction at blockId=" + blockId);
                            }
                            totalEvictions.incrementAndGet();
                        }

                        // Periodically check if a page was fully reclaimed
                        if (rng.nextInt(100) == 0) {
                            int pageIdx = (int) (blockId >>> SparseLongBlockTable.PAGE_SHIFT);
                            if (!table.isChunkAllocated(pageIdx)) {
                                pagesFullyEvicted.incrementAndGet();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    corruptionDetected.set(true);
                    corruptionDetail.set("Evictor exception: " + e);
                }
            }));
        }

        // Reader threads: continuously read and validate
        for (int r = 0; r < readerCount; r++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    ThreadLocalRandom rng = ThreadLocalRandom.current();
                    while (running.get() && !corruptionDetected.get()) {
                        long blockId = rng.nextLong(0, blockIdRange);
                        MemorySegment seg = table.get(blockId);
                        if (seg != null) {
                            // Must see fully initialized sentinel data — never partial
                            if (!hasSentinelFill(seg)) {
                                corruptionDetected.set(true);
                                corruptionDetail.set(
                                        "Reader saw corrupted fill at blockId=" + blockId);
                            }
                        }
                        // get() returning null is fine — entry may have been evicted
                        totalReads.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    corruptionDetected.set(true);
                    corruptionDetail.set("Reader exception: " + e);
                }
            }));
        }

        startLatch.countDown();
        Thread.sleep(durationMs);
        running.set(false);

        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertFalse(corruptionDetected.get(),
                "No corruption during eviction-heavy stress: " + corruptionDetail.get());

        // Verify remaining entries are valid
        for (long blockId = 0; blockId < blockIdRange; blockId++) {
            MemorySegment seg = table.get(blockId);
            if (seg != null) {
                assertTrue(hasSentinelFill(seg),
                        "Post-stress entry at blockId=" + blockId + " must have valid sentinel fill");
            }
        }

        // Verify meaningful work happened
        assertTrue(totalPuts.get() > 1_000,
                "Expected significant puts, got " + totalPuts.get());
        assertTrue(totalEvictions.get() > 1_000,
                "Expected significant evictions, got " + totalEvictions.get());
        assertTrue(totalReads.get() > 1_000,
                "Expected significant reads, got " + totalReads.get());
        assertTrue(pagesFullyEvicted.get() > 0,
                "Expected at least one page to be fully evicted (bitmap -> 0), got " + pagesFullyEvicted.get());
    }
}
