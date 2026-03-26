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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

/**
 * Preservation property tests for buffer lifecycle — these capture the
 * UNCHANGED behavior that must be preserved after the bugfix.
 *
 * <p>All tests MUST PASS on the current unfixed code.</p>
 *
 * <p><b>Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7</b></p>
 */
@SuppressWarnings("preview")
public class BufferLifecyclePreservationTests {

    private static final int BLOCK_SIZE = StaticConfigs.CACHE_BLOCK_SIZE;

    // ---- 3.1: Successful read path ----

    /**
     * Property 3.1: For valid inputs, doPhysicalRead returns a non-null
     * MemorySegment, the data matches the file content, and blockCache
     * contains the key.
     *
     * <p><b>Validates: Requirements 3.1</b></p>
     */
    @Property(tries = 10)
    void successfulReadPathReturnsCorrectData(
            @ForAll @IntRange(min = 0, max = 3) int blockId) throws Exception {

        VfdRegistry registry = new VfdRegistry(10, 5);
        MemoryPool pool = new MemoryPool(32, BLOCK_SIZE, 0);
        BlockCache blockCache = new BlockCache(1024, registry);

        Path tempFile = createTempFileWithData(BLOCK_SIZE * 4);
        VirtualFileDescriptor vfd = registry.acquire(tempFile);

        try {
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, new FileChannelIOBackend(), pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                null, BLOCK_SIZE);

            long fileOffset = (long) blockId * BLOCK_SIZE;
            MemorySegment result = channel.readBlock(vfd.id(), blockId, fileOffset, true);

            // Result must be non-null
            assert result != null : "3.1: readBlock returned null for blockId=" + blockId;

            // Result must be read-only
            assert result.isReadOnly() : "3.1: result should be read-only";

            // Result size must be BLOCK_SIZE
            assert result.byteSize() == BLOCK_SIZE :
                "3.1: expected size=" + BLOCK_SIZE + ", got=" + result.byteSize();

            // Data must match file content
            for (int i = 0; i < BLOCK_SIZE; i++) {
                byte expected = (byte) ((fileOffset + i) & 0xFF);
                byte actual = result.get(ValueLayout.JAVA_BYTE, i);
                assert expected == actual :
                    "3.1: byte mismatch at offset " + i + " for blockId=" + blockId +
                    ": expected=" + expected + ", actual=" + actual;
            }

            // BlockCache must contain the key
            long key = VirtualPage.encode(vfd.id(), blockId);
            MemorySegment cached = blockCache.get(key);
            assert cached != null :
                "3.1: blockCache should contain key for blockId=" + blockId;
        } finally {
            registry.release(vfd);
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    // ---- 3.2: Pool invariant ----

    /**
     * Property 3.2: For sequences of acquire operations without failures,
     * availableBuffers + acquired = totalBuffers.
     *
     * <p><b>Validates: Requirements 3.2</b></p>
     */
    @Property(tries = 20)
    void poolInvariantHoldsWithoutFailures(
            @ForAll @IntRange(min = 4, max = 16) int totalBuffers,
            @ForAll @IntRange(min = 0, max = 3) int acquireCount) throws Exception {

        // Ensure we don't try to acquire more than available
        int toAcquire = Math.min(acquireCount, totalBuffers);

        try (MemoryPool pool = new MemoryPool(totalBuffers, BLOCK_SIZE, 0)) {
            List<ByteBuffer> acquired = new ArrayList<>();

            for (int i = 0; i < toAcquire; i++) {
                acquired.add(pool.acquire(true, 1000));
            }

            int available = pool.availableBuffers();
            int held = acquired.size();

            assert available + held == totalBuffers :
                "3.2: Pool invariant violated: available(" + available +
                ") + held(" + held + ") != total(" + totalBuffers + ")";
        }
    }

    // ---- 3.3: Cleaner fires on GC ----

    /**
     * Property 3.3: When a buffer becomes unreachable, the Cleaner fires
     * and replenishes the pool. Best-effort test using System.gc().
     *
     * <p><b>Validates: Requirements 3.3</b></p>
     */
    @Example
    void cleanerFiresOnGCAndReplenishesPool() throws Exception {
        int totalBuffers = 4;
        try (MemoryPool pool = new MemoryPool(totalBuffers, BLOCK_SIZE, 0)) {
            // Acquire a buffer
            ByteBuffer buf = pool.acquire(true, 1000);
            assert buf != null : "3.3: acquire returned null";

            int afterAcquire = pool.availableBuffers();
            assert afterAcquire == totalBuffers - 1 :
                "3.3: after acquire, available should be " + (totalBuffers - 1) +
                ", got " + afterAcquire;

            // Make the buffer unreachable
            //noinspection UnusedAssignment
            buf = null;

            // Best-effort GC + wait for Cleaner
            boolean recovered = false;
            for (int attempt = 0; attempt < 10; attempt++) {
                System.gc();
                Thread.sleep(100);
                if (pool.availableBuffers() >= totalBuffers) {
                    recovered = true;
                    break;
                }
            }

            assert recovered :
                "3.3: Pool did not recover after GC. available=" +
                pool.availableBuffers() + ", expected=" + totalBuffers +
                ". Cleaner may not have fired (best-effort test).";
        }
    }

    // ---- 3.4: Prefetch backpressure ----

    /**
     * Property 3.4: When pool is exhausted, prefetch (isDemandRead=false)
     * throws MemoryBackpressureException.
     *
     * <p><b>Validates: Requirements 3.4</b></p>
     */
    @Property(tries = 5)
    void prefetchThrowsBackpressureWhenPoolExhausted(
            @ForAll @IntRange(min = 1, max = 4) int totalBuffers) throws Exception {

        try (MemoryPool pool = new MemoryPool(totalBuffers, BLOCK_SIZE, 0)) {
            // Exhaust the pool
            List<ByteBuffer> acquired = new ArrayList<>();
            for (int i = 0; i < totalBuffers; i++) {
                acquired.add(pool.acquire(true, 1000));
            }

            assert pool.availableBuffers() == 0 :
                "3.4: pool should be exhausted, available=" + pool.availableBuffers();

            // Prefetch acquire should throw MemoryBackpressureException
            boolean threwBackpressure = false;
            try {
                pool.acquire(false, 0);
            } catch (MemoryBackpressureException e) {
                threwBackpressure = true;
            }

            assert threwBackpressure :
                "3.4: prefetch acquire on exhausted pool should throw " +
                "MemoryBackpressureException";
        }
    }

    // ---- 3.5: Inflight deduplication ----

    /**
     * Property 3.5: Two concurrent reads for the same block result in only
     * one physical I/O. Uses a slow IOBackend to ensure overlap.
     *
     * <p><b>Validates: Requirements 3.5</b></p>
     */
    @Example
    void inflightDeduplicationReducesPhysicalIO() throws Exception {
        VfdRegistry registry = new VfdRegistry(10, 5);
        MemoryPool pool = new MemoryPool(32, BLOCK_SIZE, 0);
        BlockCache blockCache = new BlockCache(1024, registry);

        Path tempFile = createTempFileWithData(BLOCK_SIZE * 4);
        VirtualFileDescriptor vfd = registry.acquire(tempFile);

        try {
            // Slow backend: 300ms delay ensures threads overlap
            SlowIOBackend slowBackend = new SlowIOBackend(new FileChannelIOBackend(), 300);
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, slowBackend, pool, blockCache, registry,
                new Semaphore(64), 30_000L, null, BLOCK_SIZE);

            int threadCount = 4;
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            CountDownLatch done = new CountDownLatch(threadCount);
            AtomicReference<Throwable> error = new AtomicReference<>();

            for (int i = 0; i < threadCount; i++) {
                new Thread(() -> {
                    try {
                        barrier.await(5, TimeUnit.SECONDS);
                        channel.readBlock(vfd.id(), 0, 0, true);
                    } catch (Exception e) {
                        error.compareAndSet(null, e);
                    } finally {
                        done.countDown();
                    }
                }).start();
            }

            assert done.await(10, TimeUnit.SECONDS) : "3.5: threads did not complete";
            assert error.get() == null : "3.5: unexpected error: " + error.get();

            long physicalIO = channel.metrics().physicalIOCount.sum();
            long dedupedIO = channel.metrics().dedupedIOCount.sum();

            assert physicalIO == 1 :
                "3.5: expected 1 physical I/O, got " + physicalIO;
            assert dedupedIO >= 1 :
                "3.5: expected dedupedIOCount >= 1, got " + dedupedIO;
        } finally {
            registry.release(vfd);
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    // ---- 3.6: Coalescing merges adjacent blocks ----

    /**
     * Property 3.6: When multiple adjacent blocks are not cached, a coalesced
     * read merges them into a single I/O operation.
     *
     * <p><b>Validates: Requirements 3.6</b></p>
     */
    @Example
    void coalescingMergesAdjacentBlocks() throws Exception {
        VfdRegistry registry = new VfdRegistry(10, 5);
        MemoryPool pool = new MemoryPool(32, BLOCK_SIZE, 0);
        BlockCache blockCache = new BlockCache(1024, registry);

        // Create a file large enough for coalescing (16 blocks)
        Path tempFile = createTempFileWithData(BLOCK_SIZE * 16);
        VirtualFileDescriptor vfd = registry.acquire(tempFile);

        try {
            // maxCoalescedIoSize = 4 blocks
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, new FileChannelIOBackend(), pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                null, BLOCK_SIZE * 4);

            // Read block 0 — should coalesce blocks 0-3
            MemorySegment result = channel.readBlock(vfd.id(), 0, 0, true);

            assert result != null : "3.6: readBlock returned null";
            assert channel.metrics().physicalIOCount.sum() == 1 :
                "3.6: expected 1 physical I/O";

            long coalescedIO = channel.metrics().coalescedIOCount.sum();
            long coalescedBlocks = channel.metrics().coalescedBlockCount.sum();

            assert coalescedIO > 0 :
                "3.6: expected coalescedIOCount > 0, got " + coalescedIO;
            assert coalescedBlocks > 1 :
                "3.6: expected coalescedBlockCount > 1, got " + coalescedBlocks;

            // Verify adjacent blocks were also cached
            for (int b = 1; b <= 3; b++) {
                long key = VirtualPage.encode(vfd.id(), b);
                assert blockCache.get(key) != null :
                    "3.6: block " + b + " should be cached from coalesced read";
            }
        } finally {
            registry.release(vfd);
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    // ---- 3.7: VFD release invalidates cache ----

    /**
     * Property 3.7: When a VFD is released (refCount drops to 0), L2 cache
     * entries are invalidated.
     *
     * <p><b>Validates: Requirements 3.7</b></p>
     */
    @Example
    void vfdReleaseInvalidatesCache() throws Exception {
        VfdRegistry registry = new VfdRegistry(10, 5);
        MemoryPool pool = new MemoryPool(32, BLOCK_SIZE, 0);
        BlockCache blockCache = new BlockCache(1024, registry);
        registry.setBlockCache(blockCache);

        Path tempFile = createTempFileWithData(BLOCK_SIZE * 4);
        VirtualFileDescriptor vfd = registry.acquire(tempFile);

        try {
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, new FileChannelIOBackend(), pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                null, BLOCK_SIZE);

            // Read block 0 — populates L2 cache
            channel.readBlock(vfd.id(), 0, 0, true);

            long key = VirtualPage.encode(vfd.id(), 0);
            blockCache.cleanUp();
            assert blockCache.get(key) != null :
                "3.7: block should be in cache after read";

            long vfdId = vfd.id();

            // Release the VFD (refCount drops to 0)
            registry.release(vfd);

            // invalidateForVfd is synchronous on the Caffeine map,
            // but cleanUp ensures maintenance is done
            blockCache.cleanUp();

            // Cache should no longer contain the key
            MemorySegment afterRelease = blockCache.get(key);
            assert afterRelease == null :
                "3.7: cache should not contain key after VFD release, " +
                "but found entry for vfdId=" + vfdId;
        } finally {
            // vfd already released above; close remaining resources
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    // ---- Full lifecycle invariant: pool + cache + inflight = total ----

    /**
     * Property: After a mix of successful reads and I/O failures (Category A/B),
     * the total buffer count is conserved: pool_available + cached + inflight = totalBuffers.
     *
     * <p>Failures use recycle() which returns the buffer to the pool immediately,
     * so no buffers are lost. This is the key invariant the bugfix preserves.</p>
     */
    @Property(tries = 10)
    void fullLifecycleInvariantHoldsAcrossSuccessAndFailure(
            @ForAll @IntRange(min = 1, max = 4) int successCount,
            @ForAll @IntRange(min = 1, max = 4) int failureCount) throws Exception {

        int totalBuffers = 16;
        VfdRegistry registry = new VfdRegistry(10, 5);
        MemoryPool pool = new MemoryPool(totalBuffers, BLOCK_SIZE, 0);
        BlockCache blockCache = new BlockCache(1024, registry);

        Path tempFile = createTempFileWithData(BLOCK_SIZE * 16);
        VirtualFileDescriptor vfd = registry.acquire(tempFile);

        try {
            // Successful reads first
            VirtualFileChannel goodChannel = new VirtualFileChannel(
                vfd, new FileChannelIOBackend(), pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                null, BLOCK_SIZE);

            for (int i = 0; i < successCount; i++) {
                goodChannel.readBlock(vfd.id(), i, (long) i * BLOCK_SIZE, true);
            }

            // Failing reads — IOBackend throws, buffer should be recycled
            IOBackend failingBackend = (ch, dst, offset, length) -> {
                throw new IOException("Simulated I/O failure");
            };
            VirtualFileChannel badChannel = new VirtualFileChannel(
                vfd, failingBackend, pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                null, BLOCK_SIZE);

            for (int i = 0; i < failureCount; i++) {
                int blockId = successCount + i;
                try {
                    badChannel.readBlock(vfd.id(), blockId, (long) blockId * BLOCK_SIZE, true);
                } catch (IOException expected) {
                    // Expected
                }
            }

            // Count cached blocks (each successful read caches one block)
            int cachedCount = 0;
            for (int i = 0; i < successCount; i++) {
                long key = VirtualPage.encode(vfd.id(), i);
                if (blockCache.get(key) != null) {
                    cachedCount++;
                }
            }

            // No inflight operations at this point (all reads completed)
            int available = pool.availableBuffers();

            // Invariant: available + cached = totalBuffers
            // (inflight = 0 since all reads are done)
            assert available + cachedCount == totalBuffers :
                "Lifecycle invariant violated: available(" + available +
                ") + cached(" + cachedCount + ") = " + (available + cachedCount) +
                " != total(" + totalBuffers + "). " +
                "successCount=" + successCount + ", failureCount=" + failureCount +
                ", recycleCount=" + pool.metrics().recycleCount.sum();
        } finally {
            registry.release(vfd);
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    // ---- MemoryBackpressureException from allocateOneTimeBuffer ----

    /**
     * Property: allocateOneTimeBuffer() throws MemoryBackpressureException
     * when direct memory usage would exceed the configured threshold.
     */
    @Example
    void allocateOneTimeBufferThrowsBackpressureUnderPressure() throws Exception {
        try (MemoryPool pool = new MemoryPool(4, BLOCK_SIZE, 0)) {
            // Set threshold very low so any allocation triggers backpressure
            pool.setDirectMemoryThreshold(0.0001);

            boolean threwBackpressure = false;
            try {
                pool.allocateOneTimeBuffer(BLOCK_SIZE * 16);
            } catch (MemoryBackpressureException e) {
                threwBackpressure = true;
            }

            assert threwBackpressure :
                "allocateOneTimeBuffer should throw MemoryBackpressureException " +
                "when direct memory usage exceeds threshold";
        }
    }

    /**
     * Property: allocateOneTimeBuffer() succeeds when direct memory usage
     * is well below the threshold.
     */
    @Example
    void allocateOneTimeBufferSucceedsUnderNormalConditions() throws Exception {
        try (MemoryPool pool = new MemoryPool(4, BLOCK_SIZE, 0)) {
            // Default threshold is 0.85 — should have plenty of headroom
            ByteBuffer buf = pool.allocateOneTimeBuffer(BLOCK_SIZE);
            assert buf != null : "allocateOneTimeBuffer should return a non-null buffer";
            assert buf.capacity() >= BLOCK_SIZE :
                "buffer capacity should be >= requested size";
        }
    }

    // ---- Helper: Slow I/O backend ----

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

    // ---- Helper: Create temp file with known data ----

    private static Path createTempFileWithData(int size) throws IOException {
        Path tempFile = Files.createTempFile("buf-lifecycle-preserve", ".dat");
        tempFile.toFile().deleteOnExit();
        byte[] data = new byte[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Files.write(tempFile, data);
        return tempFile;
    }
}
