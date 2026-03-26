/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

/**
 * Bug condition exploration tests for buffer lifecycle failure paths.
 *
 * <p>These tests MUST FAIL on the current unfixed code — failure confirms
 * the bugs described in bugfix.md exist. DO NOT fix the code or the test
 * when it fails.</p>
 *
 * <p><b>Validates: Requirements 1.1, 1.2, 1.4, 1.5, 1.6</b></p>
 */
@SuppressWarnings("preview")
public class BufferLifecycleBugConditionTests {

    private static final int BLOCK_SIZE = StaticConfigs.CACHE_BLOCK_SIZE;

    // ---- C1: acquireChannel() failure wastes direct memory via compensate() ----

    /**
     * C1: When beforeRead (simulating acquireChannel failure) throws before
     * the buffer reaches the kernel, the system calls compensate() which
     * allocates a FRESH buffer instead of recycling the original. This wastes
     * direct memory — the buffer was never kernel-touched and could have been
     * safely recycled.
     *
     * <p>On UNFIXED code: compensationCount increases (compensate() is called).
     * The pool should use recycle() instead, which would NOT increment
     * compensationCount.</p>
     *
     * <p>On FIXED code: compensationCount should be 0 because recycle() is
     * used instead of compensate(). A recycleCount metric should exist and
     * increase.</p>
     *
     * <p><b>Validates: Requirements 1.1</b></p>
     */
    @Property(tries = 10)
    void c1_acquireChannelFailureUsesRecycleNotCompensate(
            @ForAll @IntRange(min = 1, max = 5) int failureCount) throws Exception {

        int totalBuffers = 8;
        ThrowingBeforeReadInjector injector = new ThrowingBeforeReadInjector();
        VfdRegistry registry = new VfdRegistry(10, 5, 5000L, injector);
        MemoryPool pool = new MemoryPool(totalBuffers, BLOCK_SIZE, 0, injector);
        BlockCache blockCache = new BlockCache(64, registry);

        Path tempFile = createTempFileWithData(BLOCK_SIZE * 4);
        VirtualFileDescriptor vfd = registry.acquire(tempFile);

        try {
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, new FileChannelIOBackend(), pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                null, BLOCK_SIZE, injector);

            for (int i = 0; i < failureCount; i++) {
                try {
                    channel.readBlock(vfd.id(), i, (long) i * BLOCK_SIZE, true);
                } catch (IOException expected) {
                    // Expected: beforeRead throws IOException
                }
            }

            long recycleCount = pool.metrics().recycleCount.sum();

            // On FIXED code: recycleCount == failureCount (recycle used)
            // On UNFIXED code: recycleCount == 0 (compensate called instead)
            assert recycleCount == failureCount :
                "C1 BUG: After " + failureCount + " pre-kernel failures, " +
                "recycleCount=" + recycleCount + " (should be " + failureCount + "). " +
                "compensate() wastes direct memory by allocating fresh buffers " +
                "instead of recycling the original buffer that was never kernel-touched.";
        } finally {
            registry.release(vfd);
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    // ---- C2: FileChannel.read() failure wastes direct memory via compensate() ----

    /**
     * C2: When FileChannel.read() throws (e.g., ClosedChannelException),
     * the system calls compensate() identically to the timeout case.
     * The buffer was never kernel-touched (the syscall returned with an error)
     * and could have been safely recycled.
     *
     * <p>On UNFIXED code: compensationCount increases.</p>
     * <p>On FIXED code: compensationCount == 0, recycle() used instead.</p>
     *
     * <p><b>Validates: Requirements 1.2</b></p>
     */
    @Property(tries = 10)
    void c2_fileChannelReadFailureUsesRecycleNotCompensate(
            @ForAll @IntRange(min = 1, max = 5) int failureCount) throws Exception {

        int totalBuffers = 8;
        VfdRegistry registry = new VfdRegistry(10, 5);
        MemoryPool pool = new MemoryPool(totalBuffers, BLOCK_SIZE, 0);
        BlockCache blockCache = new BlockCache(64, registry);

        Path tempFile = createTempFileWithData(BLOCK_SIZE * 4);
        VirtualFileDescriptor vfd = registry.acquire(tempFile);

        // IOBackend that throws ClosedChannelException on read()
        IOBackend failingBackend = (ch, dst, offset, length) -> {
            throw new ClosedChannelException();
        };

        try {
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, failingBackend, pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                null, BLOCK_SIZE);

            for (int i = 0; i < failureCount; i++) {
                try {
                    channel.readBlock(vfd.id(), i, (long) i * BLOCK_SIZE, true);
                } catch (IOException expected) {
                    // Expected: ClosedChannelException wrapped in IOException
                }
            }

            long recycleCount = pool.metrics().recycleCount.sum();

            // On FIXED code: recycleCount == failureCount (recycle used)
            // On UNFIXED code: recycleCount == 0 (compensate called instead)
            assert recycleCount == failureCount :
                "C2 BUG: After " + failureCount + " FileChannel.read() failures " +
                "(ClosedChannelException), recycleCount=" + recycleCount +
                " (should be " + failureCount + "). compensate() wastes direct memory " +
                "on post-kernel exceptions where the buffer is safe to recycle.";
        } finally {
            registry.release(vfd);
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    // ---- C4: doCoalescedPhysicalRead() uses unchecked ByteBuffer.allocateDirect() ----

    /**
     * C4: doCoalescedPhysicalRead() calls ByteBuffer.allocateDirect(plan.totalBytes())
     * without any pressure check. On unfixed code, there is no
     * allocateOneTimeBuffer() method on MemoryPool — just unchecked allocation.
     *
     * <p>On UNFIXED code: MemoryPool lacks allocateOneTimeBuffer() method.</p>
     * <p>On FIXED code: MemoryPool should have allocateOneTimeBuffer() which
     * throws MemoryBackpressureException under pressure.</p>
     *
     * <p><b>Validates: Requirements 1.4</b></p>
     */
    @Property(tries = 1)
    void c4_coalescedReadLacksPressureGuard() {
        boolean hasAllocateOneTimeBuffer = false;
        try {
            MemoryPool.class.getMethod("allocateOneTimeBuffer", int.class);
            hasAllocateOneTimeBuffer = true;
        } catch (NoSuchMethodException e) {
            // Expected on unfixed code
        }

        assert hasAllocateOneTimeBuffer :
            "C4 BUG: MemoryPool lacks allocateOneTimeBuffer() method. " +
            "doCoalescedPhysicalRead() uses unchecked ByteBuffer.allocateDirect() " +
            "which can cause OOM under direct memory pressure.";
    }

    // ---- C5: Concurrent failures cause pool exhaustion via compensate() ----

    /**
     * C5: Under concurrent failures, compensate() is called for each failure.
     * Each compensate() call allocates a fresh buffer AND the original buffer
     * gets a new Cleaner registered (via registerCleaner in acquire()). When
     * GC eventually collects the original, the Cleaner fires and checks
     * compensationPending. But under rapid concurrent failures, the pool
     * temporarily inflates (more buffers than totalBuffers) because compensate()
     * adds fresh buffers before GC collects the originals.
     *
     * <p>The real bug: compensate() should not be used for Category A/B failures.
     * On fixed code, recycle() returns the buffer immediately without allocating
     * new memory, so pool size stays exactly at totalBuffers.</p>
     *
     * <p>On UNFIXED code: after concurrent failures, compensationCount > 0
     * (proving compensate was used instead of recycle).</p>
     *
     * <p><b>Validates: Requirements 1.5</b></p>
     */
    @Property(tries = 5)
    void c5_concurrentFailuresUseCompensateInsteadOfRecycle(
            @ForAll @IntRange(min = 4, max = 8) int threadCount) throws Exception {

        int totalBuffers = threadCount * 2;
        VfdRegistry registry = new VfdRegistry(10, 5);
        MemoryPool pool = new MemoryPool(totalBuffers, BLOCK_SIZE, 0);
        BlockCache blockCache = new BlockCache(64, registry);

        Path tempFile = createTempFileWithData(BLOCK_SIZE * 64);
        VirtualFileDescriptor vfd = registry.acquire(tempFile);

        // IOBackend that throws on every read
        IOBackend failingBackend = (ch, dst, offset, length) -> {
            throw new IOException("Simulated concurrent failure");
        };

        try {
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, failingBackend, pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                null, BLOCK_SIZE);

            int readsPerThread = 3;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicInteger completedThreads = new AtomicInteger(0);

            for (int t = 0; t < threadCount; t++) {
                final int threadIdx = t;
                Thread thread = new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < readsPerThread; i++) {
                            int blockId = threadIdx * readsPerThread + i;
                            try {
                                channel.readBlock(vfd.id(), blockId,
                                    (long) blockId * BLOCK_SIZE, true);
                            } catch (IOException expected) {
                                // Expected failure
                            }
                        }
                        completedThreads.incrementAndGet();
                    } catch (Exception e) {
                        // Unexpected
                    } finally {
                        doneLatch.countDown();
                    }
                });
                thread.setDaemon(true);
                thread.start();
            }

            startLatch.countDown();
            boolean allCompleted = doneLatch.await(15, TimeUnit.SECONDS);

            long recycleCount = pool.metrics().recycleCount.sum();
            int expectedFailures = threadCount * readsPerThread;

            // On FIXED code: recycleCount == expectedFailures (recycle used)
            // On UNFIXED code: recycleCount == 0 (compensate called instead)
            assert allCompleted && recycleCount == expectedFailures :
                "C5 BUG: Under " + threadCount + " concurrent threads with failures, " +
                "recycleCount=" + recycleCount + " (should be " + expectedFailures + "). " +
                "completedThreads=" + completedThreads.get() + "/" + threadCount + ". " +
                "compensate() wastes direct memory under concurrent failures " +
                "instead of recycling buffers. This can lead to pool exhaustion " +
                "and deadlock under sustained failure load.";
        } finally {
            registry.release(vfd);
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    // ---- C6: No way to disable coalescing ----

    /**
     * C6: There is no way to disable I/O coalescing in VirtualFileChannel.
     * On unfixed code, there is no coalescingEnabled field or constructor
     * parameter — coalescing is always attempted in readBlock().
     *
     * <p>On UNFIXED code: VirtualFileChannel has no coalescingEnabled field.</p>
     * <p>On FIXED code: VirtualFileChannel should have a coalescingEnabled field.</p>
     *
     * <p><b>Validates: Requirements 1.6</b></p>
     */
    @Property(tries = 1)
    void c6_coalescingCanBeDisabled() {
        boolean hasCoalescingEnabledField = false;
        try {
            Field field = VirtualFileChannel.class.getDeclaredField("coalescingEnabled");
            hasCoalescingEnabledField = (field != null);
        } catch (NoSuchFieldException e) {
            // Expected on unfixed code
        }

        assert hasCoalescingEnabledField :
            "C6 BUG: VirtualFileChannel lacks 'coalescingEnabled' field. " +
            "There is no way to disable I/O coalescing for debugging or " +
            "random-access workloads.";
    }

    // ---- Helper: ThrowingBeforeReadInjector ----

    /**
     * A FaultInjector that throws IOException on beforeRead to simulate
     * failures after pool acquire but before the kernel gets the buffer.
     * This exercises the Category A failure path (pre-kernel).
     */
    static class ThrowingBeforeReadInjector implements FaultInjector {
        @Override
        public void beforeRead(long vfdId, long fileOffset) throws IOException {
            throw new IOException("Injected fault: pre-kernel failure simulation");
        }

        @Override
        public void afterRead(long vfdId, long fileOffset, ByteBuffer buffer) {}

        @Override
        public void beforeChannelAcquire(long vfdId) {}

        @Override
        public void afterChannelClose(long vfdId) {}

        @Override
        public void beforePoolAcquire() {}

        @Override
        public void afterCleanerCallback() {}
    }

    // ---- Helper: Create temp file with known data ----

    private static Path createTempFileWithData(int size) throws IOException {
        Path tempFile = Files.createTempFile("buf-lifecycle-bug", ".dat");
        tempFile.toFile().deleteOnExit();
        byte[] data = new byte[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Files.write(tempFile, data);
        return tempFile;
    }
}
