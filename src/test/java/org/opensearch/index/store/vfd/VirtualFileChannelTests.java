/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.opensearch.index.store.bufferpoolfs.StaticConfigs;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link VirtualFileChannel}: cache hit, cache miss + physical I/O,
 * dedup (two threads same block = 1 I/O), timeout, inflight map cleanup on error.
 *
 * <p><b>Validates: Requirements 5, 6, 7, 8, 30, 31</b></p>
 */
@SuppressWarnings("preview")
public class VirtualFileChannelTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE = StaticConfigs.CACHE_BLOCK_SIZE;

    private VfdRegistry registry;
    private MemoryPool pool;
    private BlockCache blockCache;
    private Path testFile;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new VfdRegistry(10, 5);
        pool = new MemoryPool(32, BLOCK_SIZE, 0); // unaligned for test simplicity
        blockCache = new BlockCache(1024, registry);

        // Create a test file with known content
        testFile = createTempFile("vfc-test", ".dat");
        byte[] data = new byte[BLOCK_SIZE * 4]; // 4 blocks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Files.write(testFile, data);
    }

    @Override
    public void tearDown() throws Exception {
        blockCache.close();
        pool.close();
        registry.close();
        super.tearDown();
    }

    private VirtualFileChannel createChannel(VirtualFileDescriptor vfd) {
        return createChannel(vfd, new Semaphore(64));
    }

    private VirtualFileChannel createChannel(VirtualFileDescriptor vfd, Semaphore limiter) {
        // Use single-block I/O (no coalescing) for existing tests
        return new VirtualFileChannel(
            vfd, new FileChannelIOBackend(), pool, blockCache,
            registry, limiter, VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
            null, BLOCK_SIZE);
    }

    private VirtualFileChannel createCoalescingChannel(VirtualFileDescriptor vfd, int maxCoalescedIoSize) {
        return new VirtualFileChannel(
            vfd, new FileChannelIOBackend(), pool, blockCache,
            registry, new Semaphore(64), VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
            null, maxCoalescedIoSize);
    }

    // ---- Test: Cache hit returns cached data without physical I/O ----

    public void testCacheHitReturnsWithoutPhysicalIO() throws IOException {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        VirtualFileChannel channel = createChannel(vfd);

        // Pre-populate L2 cache with a known segment
        byte[] blockData = new byte[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            blockData[i] = (byte) 0xAB;
        }
        ByteBuffer buf = ByteBuffer.allocateDirect(BLOCK_SIZE);
        buf.put(blockData);
        buf.flip();
        MemorySegment preloaded = MemorySegment.ofBuffer(buf.asReadOnlyBuffer());
        long key = VirtualPage.encode(vfd.id(), 0);
        blockCache.put(key, preloaded);

        // Read block 0 — should hit cache
        MemorySegment result = channel.readBlock(vfd.id(), 0, 0, true);

        assertNotNull(result);
        assertEquals(0, channel.metrics().physicalIOCount.sum());
        // Verify it's the same cached segment
        assertEquals((byte) 0xAB, result.get(ValueLayout.JAVA_BYTE, 0));

        registry.release(vfd);
    }

    // ---- Test: Cache miss triggers physical I/O and populates caches ----

    public void testCacheMissTriggersPhysicalIO() throws IOException {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        VirtualFileChannel channel = createChannel(vfd);

        // Read block 0 — cache miss, should do physical I/O
        MemorySegment result = channel.readBlock(vfd.id(), 0, 0, true);

        assertNotNull(result);
        assertEquals(1, channel.metrics().physicalIOCount.sum());
        assertEquals(BLOCK_SIZE, (int) result.byteSize());

        // Verify data matches file content
        for (int i = 0; i < BLOCK_SIZE; i++) {
            assertEquals("Byte mismatch at offset " + i,
                (byte) (i & 0xFF),
                result.get(ValueLayout.JAVA_BYTE, i));
        }

        // Verify L2 cache was populated
        long key = VirtualPage.encode(vfd.id(), 0);
        MemorySegment cached = blockCache.get(key);
        assertNotNull("L2 cache should be populated", cached);

        // Verify L1 RadixBlockTable was populated
        MemorySegment l1 = registry.getRadixBlockTable(vfd.id()).get(0);
        assertNotNull("L1 RadixBlockTable should be populated", l1);

        registry.release(vfd);
    }

    // ---- Test: Second read of same block hits cache ----

    public void testSecondReadHitsCache() throws IOException {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        VirtualFileChannel channel = createChannel(vfd);

        // First read — cache miss
        channel.readBlock(vfd.id(), 0, 0, true);
        assertEquals(1, channel.metrics().physicalIOCount.sum());

        // Second read — should hit cache
        MemorySegment result = channel.readBlock(vfd.id(), 0, 0, true);
        assertNotNull(result);
        assertEquals("No additional physical I/O expected", 1,
            channel.metrics().physicalIOCount.sum());

        registry.release(vfd);
    }

    // ---- Test: Reading different blocks at different offsets ----

    public void testReadDifferentBlocks() throws IOException {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        VirtualFileChannel channel = createChannel(vfd);

        // Read block 0
        MemorySegment block0 = channel.readBlock(vfd.id(), 0, 0, true);
        // Read block 1
        long offset1 = (long) 1 << StaticConfigs.CACHE_BLOCK_SIZE_POWER;
        MemorySegment block1 = channel.readBlock(vfd.id(), 1, offset1, true);

        assertEquals(2, channel.metrics().physicalIOCount.sum());

        // Verify block 1 data starts at the correct file offset
        assertEquals((byte) (offset1 & 0xFF),
            block1.get(ValueLayout.JAVA_BYTE, 0));

        registry.release(vfd);
    }

    // ---- Test: Dedup — two threads same block = 1 physical I/O ----

    public void testDedupTwoThreadsSameBlock() throws Exception {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        // Use a slow I/O backend to ensure both threads overlap
        SlowIOBackend slowBackend = new SlowIOBackend(new FileChannelIOBackend(), 200);
        VirtualFileChannel channel = new VirtualFileChannel(
            vfd, slowBackend, pool, blockCache, registry,
            new Semaphore(64), 30_000L, null);

        int threadCount = 4;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicReference<MemorySegment>[] results = new AtomicReference[threadCount];
        AtomicReference<Throwable> error = new AtomicReference<>();

        for (int i = 0; i < threadCount; i++) {
            results[i] = new AtomicReference<>();
            final int idx = i;
            new Thread(() -> {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                    MemorySegment seg = channel.readBlock(vfd.id(), 0, 0, true);
                    results[idx].set(seg);
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            }).start();
        }

        assertTrue("Threads should complete", done.await(10, TimeUnit.SECONDS));
        assertNull("No errors expected", error.get());

        // Verify only 1 physical I/O happened
        assertEquals("Dedup should result in exactly 1 physical I/O", 1,
            channel.metrics().physicalIOCount.sum());

        // Verify dedup count (threadCount - 1 threads joined the inflight)
        assertTrue("Dedup count should be > 0",
            channel.metrics().dedupedIOCount.sum() > 0);

        // Verify all threads got valid data
        for (int i = 0; i < threadCount; i++) {
            assertNotNull("Thread " + i + " should have a result", results[i].get());
            assertEquals((byte) 0,
                results[i].get().get(ValueLayout.JAVA_BYTE, 0));
        }

        // Verify inflight map is clean
        assertTrue("Inflight map should be empty after completion",
            registry.inflightMap().isEmpty());

        registry.release(vfd);
    }

    // ---- Test: Inflight map cleanup on I/O error ----

    public void testInflightMapCleanupOnError() throws Exception {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        // Use an I/O backend that always throws
        IOBackend failingBackend = (ch, dst, offset, length) -> {
            throw new IOException("Simulated I/O failure");
        };
        VirtualFileChannel channel = new VirtualFileChannel(
            vfd, failingBackend, pool, blockCache, registry,
            new Semaphore(64), 30_000L, null);

        try {
            channel.readBlock(vfd.id(), 0, 0, true);
            fail("Should have thrown IOException");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("Simulated I/O failure"));
        }

        // Verify inflight map is clean
        assertTrue("Inflight map should be empty after error",
            registry.inflightMap().isEmpty());

        // Verify L2 cache was NOT populated
        long key = VirtualPage.encode(vfd.id(), 0);
        assertNull("L2 cache should not contain failed block",
            blockCache.get(key));

        registry.release(vfd);
    }

    // ---- Test: Inflight limiter timeout for demand read ----

    public void testInflightLimiterTimeoutDemandRead() throws Exception {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        // Create a limiter with 0 permits — immediate timeout
        Semaphore zeroPermits = new Semaphore(0);
        VirtualFileChannel channel = new VirtualFileChannel(
            vfd, new FileChannelIOBackend(), pool, blockCache, registry,
            zeroPermits, 100L, null); // 100ms timeout

        try {
            channel.readBlock(vfd.id(), 0, 0, true);
            fail("Should have thrown IOException for inflight timeout");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("Inflight_Limiter timeout"));
        }

        // Verify inflight map is clean
        assertTrue("Inflight map should be empty after timeout",
            registry.inflightMap().isEmpty());

        registry.release(vfd);
    }

    // ---- Test: Inflight limiter fail-fast for prefetch ----

    public void testInflightLimiterFailFastPrefetch() throws Exception {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        Semaphore zeroPermits = new Semaphore(0);
        VirtualFileChannel channel = new VirtualFileChannel(
            vfd, new FileChannelIOBackend(), pool, blockCache, registry,
            zeroPermits, 30_000L, null);

        try {
            channel.readBlock(vfd.id(), 0, 0, false); // prefetch
            fail("Should have thrown MemoryBackpressureException");
        } catch (MemoryBackpressureException e) {
            assertTrue(e.getMessage().contains("inflight limit"));
        }

        assertTrue("Inflight map should be empty",
            registry.inflightMap().isEmpty());

        registry.release(vfd);
    }

    // ---- Test: Error propagation to all waiters ----

    public void testErrorPropagationToWaiters() throws Exception {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        // Slow + failing backend: delay then throw
        IOBackend delayedFailBackend = (ch, dst, offset, length) -> {
            try { Thread.sleep(200); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IOException("Delayed failure");
        };
        VirtualFileChannel channel = new VirtualFileChannel(
            vfd, delayedFailBackend, pool, blockCache, registry,
            new Semaphore(64), 30_000L, null);

        int threadCount = 3;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                    channel.readBlock(vfd.id(), 0, 0, true);
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }).start();
        }

        assertTrue("Threads should complete", done.await(10, TimeUnit.SECONDS));
        assertEquals("All threads should see the error", threadCount,
            errorCount.get());

        assertTrue("Inflight map should be empty",
            registry.inflightMap().isEmpty());

        registry.release(vfd);
    }

    // ---- Test: ChannelMetrics tracking ----

    public void testChannelMetrics() throws IOException {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        VirtualFileChannel channel = createChannel(vfd);

        // Initial state
        assertEquals(0, channel.metrics().physicalIOCount.sum());
        assertEquals(0, channel.metrics().dedupedIOCount.sum());
        assertEquals(0, channel.metrics().inflightTimeoutCount.sum());

        // Read block 0 — physical I/O
        channel.readBlock(vfd.id(), 0, 0, true);
        assertEquals(1, channel.metrics().physicalIOCount.sum());

        // Read block 0 again — cache hit, no new physical I/O
        channel.readBlock(vfd.id(), 0, 0, true);
        assertEquals(1, channel.metrics().physicalIOCount.sum());

        // Read block 1 — physical I/O
        long offset1 = (long) 1 << StaticConfigs.CACHE_BLOCK_SIZE_POWER;
        channel.readBlock(vfd.id(), 1, offset1, true);
        assertEquals(2, channel.metrics().physicalIOCount.sum());

        // Verify toString doesn't throw
        assertNotNull(channel.metrics().toString());

        registry.release(vfd);
    }

    // ---- Test: Buffer lifecycle — result is read-only MemorySegment ----

    public void testBufferLifecycleReadOnly() throws IOException {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        VirtualFileChannel channel = createChannel(vfd);

        MemorySegment result = channel.readBlock(vfd.id(), 0, 0, true);

        assertNotNull(result);
        assertTrue("Result should be read-only",
            result.isReadOnly());

        registry.release(vfd);
    }

    // ---- Test: Inflight permit is released even on error ----

    public void testInflightPermitReleasedOnError() throws Exception {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        Semaphore limiter = new Semaphore(1);
        IOBackend failingBackend = (ch, dst, offset, length) -> {
            throw new IOException("fail");
        };
        VirtualFileChannel channel = new VirtualFileChannel(
            vfd, failingBackend, pool, blockCache, registry,
            limiter, 30_000L, null);

        assertEquals(1, limiter.availablePermits());

        try {
            channel.readBlock(vfd.id(), 0, 0, true);
            fail("Should throw");
        } catch (IOException expected) {}

        // Permit should be released back
        assertEquals("Permit should be released after error", 1,
            limiter.availablePermits());

        registry.release(vfd);
    }

    // ---- Test: Coalescing produces correct data for all blocks ----

    public void testCoalescingProducesCorrectDataForAllBlocks() throws IOException {
        // Create a larger file: 16 blocks
        Path largeFile = createTempFile("vfc-coalesce", ".dat");
        byte[] data = new byte[BLOCK_SIZE * 16];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Files.write(largeFile, data);

        VirtualFileDescriptor vfd = registry.acquire(largeFile);
        // Max coalesced I/O = 4 blocks
        VirtualFileChannel channel = createCoalescingChannel(vfd, BLOCK_SIZE * 4);

        // Read block 0 — should coalesce blocks 0-3
        MemorySegment result = channel.readBlock(vfd.id(), 0, 0, true);

        assertNotNull(result);
        assertEquals(1, channel.metrics().physicalIOCount.sum());
        assertEquals(1, channel.metrics().coalescedIOCount.sum());
        assertEquals(4, channel.metrics().coalescedBlockCount.sum());

        // Verify block 0 data
        for (int i = 0; i < BLOCK_SIZE; i++) {
            assertEquals("Block 0 byte mismatch at offset " + i,
                (byte) (i & 0xFF),
                result.get(ValueLayout.JAVA_BYTE, i));
        }

        // Verify blocks 1-3 were also cached (coalesced read populated them)
        for (int b = 1; b <= 3; b++) {
            long key = VirtualPage.encode(vfd.id(), b);
            MemorySegment cached = blockCache.get(key);
            assertNotNull("Block " + b + " should be cached from coalesced read", cached);
            int fileOffset = b * BLOCK_SIZE;
            assertEquals("Block " + b + " first byte mismatch",
                (byte) (fileOffset & 0xFF),
                cached.get(ValueLayout.JAVA_BYTE, 0));
        }

        // Reading block 1 should now be a cache hit — no additional I/O
        long offset1 = (long) BLOCK_SIZE;
        MemorySegment block1 = channel.readBlock(vfd.id(), 1, offset1, true);
        assertNotNull(block1);
        assertEquals("No additional I/O for coalesced block", 1,
            channel.metrics().physicalIOCount.sum());

        registry.release(vfd);
    }

    // ---- Test: Coalescing gap handling — cached blocks are gaps in ownership ----

    public void testCoalescingGapHandling() throws IOException {
        Path largeFile = createTempFile("vfc-gap", ".dat");
        byte[] data = new byte[BLOCK_SIZE * 8];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Files.write(largeFile, data);

        VirtualFileDescriptor vfd = registry.acquire(largeFile);
        VirtualFileChannel channel = createCoalescingChannel(vfd, BLOCK_SIZE * 4);

        // Pre-populate block 1 in cache (creating a gap in ownership)
        ByteBuffer preBuf = ByteBuffer.allocateDirect(BLOCK_SIZE);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            preBuf.put((byte) 0xBB);
        }
        preBuf.flip();
        long key1 = VirtualPage.encode(vfd.id(), 1);
        blockCache.put(key1, MemorySegment.ofBuffer(preBuf.asReadOnlyBuffer()));

        // Read block 0 — should coalesce [0..3], but block 1 is a gap (already cached)
        MemorySegment result = channel.readBlock(vfd.id(), 0, 0, true);
        assertNotNull(result);
        assertEquals(1, channel.metrics().physicalIOCount.sum());

        // Block 1 should still have the pre-populated data (0xBB), not overwritten
        MemorySegment cachedBlock1 = blockCache.get(key1);
        assertNotNull(cachedBlock1);
        assertEquals("Block 1 should retain pre-populated data",
            (byte) 0xBB, cachedBlock1.get(ValueLayout.JAVA_BYTE, 0));

        // Blocks 2 and 3 should be populated from the coalesced read
        for (int b = 2; b <= 3; b++) {
            long key = VirtualPage.encode(vfd.id(), b);
            MemorySegment cached = blockCache.get(key);
            assertNotNull("Block " + b + " should be cached", cached);
        }

        registry.release(vfd);
    }

    // ---- Test: Max I/O size is respected ----

    public void testMaxCoalescedIoSizeRespected() throws IOException {
        Path largeFile = createTempFile("vfc-maxio", ".dat");
        byte[] data = new byte[BLOCK_SIZE * 16];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Files.write(largeFile, data);

        VirtualFileDescriptor vfd = registry.acquire(largeFile);
        // Max coalesced I/O = 2 blocks only
        VirtualFileChannel channel = createCoalescingChannel(vfd, BLOCK_SIZE * 2);

        // Read block 0 — should coalesce at most 2 blocks
        channel.readBlock(vfd.id(), 0, 0, true);
        assertEquals(1, channel.metrics().physicalIOCount.sum());

        // Block 0 and 1 should be cached
        assertNotNull(blockCache.get(VirtualPage.encode(vfd.id(), 0)));
        assertNotNull(blockCache.get(VirtualPage.encode(vfd.id(), 1)));

        // Block 2 should NOT be cached (beyond max coalesced I/O)
        assertNull("Block 2 should not be cached (beyond max I/O size)",
            blockCache.get(VirtualPage.encode(vfd.id(), 2)));

        registry.release(vfd);
    }

    // ---- Test: No coalescing for single-block max I/O size ----

    public void testNoCoalescingForSingleBlockMaxIoSize() throws IOException {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        // Max coalesced I/O = 1 block (no coalescing)
        VirtualFileChannel channel = createCoalescingChannel(vfd, BLOCK_SIZE);

        channel.readBlock(vfd.id(), 0, 0, true);
        assertEquals(1, channel.metrics().physicalIOCount.sum());
        assertEquals(0, channel.metrics().coalescedIOCount.sum());

        // Block 1 should NOT be cached
        assertNull(blockCache.get(VirtualPage.encode(vfd.id(), 1)));

        registry.release(vfd);
    }

    // ---- Test: owned[] array correctness in planCoalescedRead ----

    public void testOwnedArrayCorrectness() throws IOException {
        Path largeFile = createTempFile("vfc-owned", ".dat");
        byte[] data = new byte[BLOCK_SIZE * 8];
        Files.write(largeFile, data);

        VirtualFileDescriptor vfd = registry.acquire(largeFile);
        VirtualFileChannel channel = createCoalescingChannel(vfd, BLOCK_SIZE * 4);

        // Pre-populate blocks 1 and 2 in cache
        for (int b = 1; b <= 2; b++) {
            ByteBuffer buf = ByteBuffer.allocateDirect(BLOCK_SIZE);
            buf.flip();
            long key = VirtualPage.encode(vfd.id(), b);
            blockCache.put(key, MemorySegment.ofBuffer(buf.asReadOnlyBuffer()));
        }

        // Plan coalesced read starting at block 0
        VirtualFileChannel.CoalescedRead plan = channel.planCoalescedRead(vfd.id(), 0);

        // Block 0 should be owned (trigger block)
        assertTrue("Block 0 should be owned", plan.owned()[0]);
        // Block 1 should NOT be owned (already cached)
        assertFalse("Block 1 should not be owned (cached)", plan.owned()[1]);
        // Block 2 should NOT be owned (already cached)
        assertFalse("Block 2 should not be owned (cached)", plan.owned()[2]);
        // Block 3 should be owned (not cached, not inflight)
        assertTrue("Block 3 should be owned", plan.owned()[3]);

        assertEquals(4, plan.totalBlocks());
        assertEquals(4 * BLOCK_SIZE, plan.totalBytes());

        registry.release(vfd);
    }

    // ---- Test: Coalescing metrics tracking ----

    public void testCoalescingMetrics() throws IOException {
        Path largeFile = createTempFile("vfc-metrics", ".dat");
        byte[] data = new byte[BLOCK_SIZE * 16];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Files.write(largeFile, data);

        VirtualFileDescriptor vfd = registry.acquire(largeFile);
        VirtualFileChannel channel = createCoalescingChannel(vfd, BLOCK_SIZE * 4);

        // Initial state
        assertEquals(0, channel.metrics().coalescedIOCount.sum());
        assertEquals(0, channel.metrics().coalescedBlockCount.sum());

        // Read block 0 — coalesces 4 blocks
        channel.readBlock(vfd.id(), 0, 0, true);
        assertEquals(1, channel.metrics().coalescedIOCount.sum());
        assertEquals(4, channel.metrics().coalescedBlockCount.sum());

        // Read block 4 — coalesces another 4 blocks
        long offset4 = (long) 4 * BLOCK_SIZE;
        channel.readBlock(vfd.id(), 4, offset4, true);
        assertEquals(2, channel.metrics().coalescedIOCount.sum());
        assertEquals(8, channel.metrics().coalescedBlockCount.sum());

        // Verify toString includes coalescing metrics
        String metricsStr = channel.metrics().toString();
        assertTrue(metricsStr.contains("coalescedIO="));
        assertTrue(metricsStr.contains("coalescedBlocks="));

        registry.release(vfd);
    }

    // ---- Test: Coalescing inflight map cleanup on error ----

    public void testCoalescingInflightMapCleanupOnError() throws Exception {
        Path largeFile = createTempFile("vfc-coalesce-err", ".dat");
        byte[] data = new byte[BLOCK_SIZE * 8];
        Files.write(largeFile, data);

        VirtualFileDescriptor vfd = registry.acquire(largeFile);
        IOBackend failingBackend = (ch, dst, offset, length) -> {
            throw new IOException("Coalesced I/O failure");
        };
        VirtualFileChannel channel = new VirtualFileChannel(
            vfd, failingBackend, pool, blockCache, registry,
            new Semaphore(64), 30_000L, null, BLOCK_SIZE * 4);

        try {
            channel.readBlock(vfd.id(), 0, 0, true);
            fail("Should have thrown IOException");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("Coalesced I/O failure"));
        }

        // Verify inflight map is completely clean
        assertTrue("Inflight map should be empty after coalesced error",
            registry.inflightMap().isEmpty());

        registry.release(vfd);
    }

    // ---- Helper: Slow I/O backend that adds latency ----

    private static class SlowIOBackend implements IOBackend {
        private final IOBackend delegate;
        private final long delayMs;

        SlowIOBackend(IOBackend delegate, long delayMs) {
            this.delegate = delegate;
            this.delayMs = delayMs;
        }

        @Override
        public int read(FileChannel channel, ByteBuffer dst, long fileOffset, int length)
                throws IOException {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during slow read", e);
            }
            return delegate.read(channel, dst, fileOffset, length);
        }
    }

    // ---- Helper: Create channel with prefetch executor ----

    private VirtualFileChannel createPrefetchChannel(VirtualFileDescriptor vfd,
                                                      ExecutorService prefetchExec,
                                                      Semaphore limiter,
                                                      MemoryPool memPool) {
        return new VirtualFileChannel(
            vfd, new FileChannelIOBackend(), memPool, blockCache,
            registry, limiter, VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
            prefetchExec, BLOCK_SIZE);
    }

    // ---- Test: Prefetch populates L2 cache ----

    public void testPrefetchPopulatesCache() throws Exception {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        ExecutorService prefetchExec = Executors.newSingleThreadExecutor();
        try {
            VirtualFileChannel channel = createPrefetchChannel(
                vfd, prefetchExec, new Semaphore(64), pool);

            // Prefetch blocks 0 and 1
            channel.prefetch(vfd.id(), 0, 2);

            // Wait for async tasks to complete
            assertBusy(() -> {
                long key0 = VirtualPage.encode(vfd.id(), 0);
                long key1 = VirtualPage.encode(vfd.id(), 1);
                assertNotNull("Block 0 should be in L2 cache", blockCache.get(key0));
                assertNotNull("Block 1 should be in L2 cache", blockCache.get(key1));
            }, 5, TimeUnit.SECONDS);

            assertTrue("prefetchCount should be > 0",
                channel.metrics().prefetchCount.sum() > 0);
        } finally {
            prefetchExec.shutdown();
            prefetchExec.awaitTermination(5, TimeUnit.SECONDS);
            registry.release(vfd);
        }
    }

    // ---- Test: Prefetch skipped under pool pressure (MemoryBackpressureException) ----

    public void testPrefetchSkippedUnderPoolPressure() throws Exception {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        // Create a pool with 1 buffer, then exhaust it so prefetch sees backpressure
        MemoryPool tinyPool = new MemoryPool(1, BLOCK_SIZE, 0);
        java.nio.ByteBuffer exhaustedBuf = tinyPool.acquire(true, 1000);

        ExecutorService prefetchExec = Executors.newSingleThreadExecutor();
        try {
            VirtualFileChannel channel = createPrefetchChannel(
                vfd, prefetchExec, new Semaphore(64), tinyPool);

            channel.prefetch(vfd.id(), 0, 2);

            // Wait for async tasks to complete and skip count to increment
            assertBusy(() -> {
                long skipCount = channel.metrics().prefetchSkipCount.sum();
                assertTrue("prefetchSkipCount should be > 0, was " + skipCount,
                    skipCount > 0);
            }, 5, TimeUnit.SECONDS);

            // Blocks should NOT be in cache
            assertNull(blockCache.get(VirtualPage.encode(vfd.id(), 0)));
            assertNull(blockCache.get(VirtualPage.encode(vfd.id(), 1)));
        } finally {
            prefetchExec.shutdown();
            prefetchExec.awaitTermination(5, TimeUnit.SECONDS);
            tinyPool.close();
            registry.release(vfd);
        }
    }

    // ---- Test: Prefetch skipped under inflight pressure ----

    public void testPrefetchSkippedUnderInflightPressure() throws Exception {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        // Semaphore with 0 permits — readBlock(isDemandRead=false) will throw MemoryBackpressureException
        Semaphore zeroPermits = new Semaphore(0);
        ExecutorService prefetchExec = Executors.newSingleThreadExecutor();
        try {
            VirtualFileChannel channel = createPrefetchChannel(
                vfd, prefetchExec, zeroPermits, pool);

            channel.prefetch(vfd.id(), 0, 2);

            // Wait for async tasks to complete and skip count to increment
            assertBusy(() -> {
                long skipCount = channel.metrics().prefetchSkipCount.sum();
                assertTrue("prefetchSkipCount should be > 0, was " + skipCount,
                    skipCount > 0);
            }, 5, TimeUnit.SECONDS);

            // Blocks should NOT be in cache
            assertNull(blockCache.get(VirtualPage.encode(vfd.id(), 0)));
            assertNull(blockCache.get(VirtualPage.encode(vfd.id(), 1)));
        } finally {
            prefetchExec.shutdown();
            prefetchExec.awaitTermination(5, TimeUnit.SECONDS);
            registry.release(vfd);
        }
    }

    // ---- Test: Prefetch with null executor is a no-op ----

    public void testPrefetchWithNullExecutorIsNoOp() throws Exception {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        // Channel with null prefetchExecutor (default from createChannel helper)
        VirtualFileChannel channel = createChannel(vfd);

        // Should not throw and should not populate cache
        channel.prefetch(vfd.id(), 0, 2);

        // Give a brief moment to confirm nothing happens
        Thread.sleep(100);

        assertEquals(0, channel.metrics().prefetchCount.sum());
        assertEquals(0, channel.metrics().prefetchSkipCount.sum());
        assertNull(blockCache.get(VirtualPage.encode(vfd.id(), 0)));
        assertNull(blockCache.get(VirtualPage.encode(vfd.id(), 1)));

        registry.release(vfd);
    }

    // ---- Test: Prefetch skips already-cached blocks ----

    public void testPrefetchSkipsCachedBlocks() throws Exception {
        VirtualFileDescriptor vfd = registry.acquire(testFile);
        ExecutorService prefetchExec = Executors.newSingleThreadExecutor();
        // Use a counting I/O backend to track how many reads happen
        AtomicInteger ioCount = new AtomicInteger(0);
        IOBackend countingBackend = (ch, dst, offset, length) -> {
            ioCount.incrementAndGet();
            return new FileChannelIOBackend().read(ch, dst, offset, length);
        };
        try {
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, countingBackend, pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                prefetchExec, BLOCK_SIZE);

            // Pre-populate block 0 in L2 cache
            ByteBuffer preBuf = ByteBuffer.allocateDirect(BLOCK_SIZE);
            for (int i = 0; i < BLOCK_SIZE; i++) {
                preBuf.put((byte) 0xCC);
            }
            preBuf.flip();
            long key0 = VirtualPage.encode(vfd.id(), 0);
            blockCache.put(key0, MemorySegment.ofBuffer(preBuf.asReadOnlyBuffer()));

            // Prefetch blocks 0, 1, 2 — block 0 is already cached, should be skipped
            channel.prefetch(vfd.id(), 0, 3);

            // Wait for async tasks to complete
            assertBusy(() -> {
                long key1 = VirtualPage.encode(vfd.id(), 1);
                long key2 = VirtualPage.encode(vfd.id(), 2);
                assertNotNull("Block 1 should be in L2 cache", blockCache.get(key1));
                assertNotNull("Block 2 should be in L2 cache", blockCache.get(key2));
            }, 5, TimeUnit.SECONDS);

            // Block 0 should still have the pre-populated data (0xCC), not overwritten
            MemorySegment cachedBlock0 = blockCache.get(key0);
            assertNotNull(cachedBlock0);
            assertEquals("Block 0 should retain pre-populated data",
                (byte) 0xCC, cachedBlock0.get(ValueLayout.JAVA_BYTE, 0));

            // Only 2 physical I/Os should have happened (blocks 1 and 2, not block 0)
            assertEquals("Only uncached blocks should trigger I/O", 2, ioCount.get());
        } finally {
            prefetchExec.shutdown();
            prefetchExec.awaitTermination(5, TimeUnit.SECONDS);
            registry.release(vfd);
        }
    }
}
