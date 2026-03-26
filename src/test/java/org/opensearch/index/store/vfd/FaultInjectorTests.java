/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.index.store.bufferpoolfs.StaticConfigs;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link FaultInjector}, {@link NoOpFaultInjector}, and
 * {@link FaultInjectionConfig}.
 *
 * <ul>
 *   <li>Verify {@link NoOpFaultInjector} has zero overhead (all methods are no-ops).</li>
 *   <li>Verify injection points fire correctly with a test injector when
 *       {@link FaultInjectionConfig#FAULT_INJECTION_ENABLED} is {@code true}.</li>
 * </ul>
 *
 * <p><b>Validates: Requirement 50</b></p>
 */
@SuppressWarnings("preview")
public class FaultInjectorTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE = StaticConfigs.CACHE_BLOCK_SIZE;

    // ---- NoOpFaultInjector: singleton and no-op verification ----

    public void testNoOpIsSingleton() {
        assertSame(NoOpFaultInjector.INSTANCE, NoOpFaultInjector.INSTANCE);
    }

    public void testNoOpBeforeReadDoesNotThrow() throws IOException {
        NoOpFaultInjector.INSTANCE.beforeRead(1L, 0L);
    }

    public void testNoOpAfterReadDoesNotThrow() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(8);
        NoOpFaultInjector.INSTANCE.afterRead(1L, 0L, buf);
    }

    public void testNoOpBeforeChannelAcquireDoesNotThrow() {
        NoOpFaultInjector.INSTANCE.beforeChannelAcquire(1L);
    }

    public void testNoOpAfterChannelCloseDoesNotThrow() {
        NoOpFaultInjector.INSTANCE.afterChannelClose(1L);
    }

    public void testNoOpBeforePoolAcquireDoesNotThrow() {
        NoOpFaultInjector.INSTANCE.beforePoolAcquire();
    }

    public void testNoOpAfterCleanerCallbackDoesNotThrow() {
        NoOpFaultInjector.INSTANCE.afterCleanerCallback();
    }

    // ---- Default constructors use NoOpFaultInjector ----

    public void testVfdRegistryDefaultsToNoOp() {
        VfdRegistry registry = new VfdRegistry(10, 5);
        try {
            assertSame(NoOpFaultInjector.INSTANCE, registry.faultInjector());
        } finally {
            registry.close();
        }
    }

    public void testMemoryPoolDefaultsToNoOp() {
        MemoryPool pool = new MemoryPool(4, BLOCK_SIZE, 0);
        try {
            assertSame(NoOpFaultInjector.INSTANCE, pool.faultInjector());
        } finally {
            pool.close();
        }
    }

    public void testVirtualFileChannelDefaultsToNoOp() throws Exception {
        VfdRegistry registry = new VfdRegistry(10, 5);
        MemoryPool pool = new MemoryPool(4, BLOCK_SIZE, 0);
        BlockCache blockCache = new BlockCache(64, registry);
        Path tempFile = createTempFile("fi-default", ".dat");
        Files.write(tempFile, new byte[BLOCK_SIZE]);
        VirtualFileDescriptor vfd = registry.acquire(tempFile);
        try {
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, new FileChannelIOBackend(), pool, blockCache,
                registry, new Semaphore(64));
            assertSame(NoOpFaultInjector.INSTANCE, channel.faultInjector());
        } finally {
            registry.release(vfd);
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    // ---- Custom injector is accepted by constructors ----

    public void testVfdRegistryAcceptsCustomInjector() {
        RecordingFaultInjector injector = new RecordingFaultInjector();
        VfdRegistry registry = new VfdRegistry(10, 5, 5000L, injector);
        try {
            assertSame(injector, registry.faultInjector());
        } finally {
            registry.close();
        }
    }

    public void testMemoryPoolAcceptsCustomInjector() {
        RecordingFaultInjector injector = new RecordingFaultInjector();
        MemoryPool pool = new MemoryPool(4, BLOCK_SIZE, 0, injector);
        try {
            assertSame(injector, pool.faultInjector());
        } finally {
            pool.close();
        }
    }

    public void testVirtualFileChannelAcceptsCustomInjector() throws Exception {
        RecordingFaultInjector injector = new RecordingFaultInjector();
        VfdRegistry registry = new VfdRegistry(10, 5);
        MemoryPool pool = new MemoryPool(4, BLOCK_SIZE, 0);
        BlockCache blockCache = new BlockCache(64, registry);
        Path tempFile = createTempFile("fi-custom", ".dat");
        Files.write(tempFile, new byte[BLOCK_SIZE]);
        VirtualFileDescriptor vfd = registry.acquire(tempFile);
        try {
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, new FileChannelIOBackend(), pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                null, VirtualFileChannel.DEFAULT_MAX_COALESCED_IO_SIZE,
                injector);
            assertSame(injector, channel.faultInjector());
        } finally {
            registry.release(vfd);
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    // ---- FaultInjectionConfig flag ----

    public void testFaultInjectionEnabledInTests() {
        // The test JVM is started with -Dvfd.fault.injection.enabled=true
        assertTrue("FAULT_INJECTION_ENABLED must be true in test JVM",
            FaultInjectionConfig.FAULT_INJECTION_ENABLED);
    }

    // ---- Injection points fire with test injector ----

    /**
     * Verify that beforeRead and afterRead fire during a physical I/O
     * through VirtualFileChannel.readBlock().
     */
    public void testBeforeReadAndAfterReadFire() throws Exception {
        RecordingFaultInjector injector = new RecordingFaultInjector();
        VfdRegistry registry = new VfdRegistry(10, 5, 5000L, injector);
        MemoryPool pool = new MemoryPool(8, BLOCK_SIZE, 0, injector);
        BlockCache blockCache = new BlockCache(64, registry);

        Path tempFile = createTempFile("fi-read", ".dat");
        byte[] data = new byte[BLOCK_SIZE];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Files.write(tempFile, data);

        VirtualFileDescriptor vfd = registry.acquire(tempFile);
        try {
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, new FileChannelIOBackend(), pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                null, BLOCK_SIZE, injector);

            // Trigger a cache-miss read (physical I/O)
            MemorySegment seg = channel.readBlock(vfd.id(), 0, 0L, true);
            assertNotNull(seg);

            // Verify beforeRead and afterRead were called
            assertEquals("beforeRead should fire once", 1, injector.beforeReadCount.get());
            assertEquals("afterRead should fire once", 1, injector.afterReadCount.get());

            // Verify the data is correct
            assertEquals((byte) 0, seg.get(ValueLayout.JAVA_BYTE, 0));
            assertEquals((byte) 1, seg.get(ValueLayout.JAVA_BYTE, 1));
        } finally {
            registry.release(vfd);
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    /**
     * Verify that beforeChannelAcquire fires when acquireChannel is called
     * on VfdRegistry.
     */
    public void testBeforeChannelAcquireFires() throws Exception {
        RecordingFaultInjector injector = new RecordingFaultInjector();
        VfdRegistry registry = new VfdRegistry(10, 5, 5000L, injector);

        Path tempFile = createTempFile("fi-acquire", ".dat");
        Files.write(tempFile, new byte[BLOCK_SIZE]);

        VirtualFileDescriptor vfd = registry.acquire(tempFile);
        try {
            assertEquals(0, injector.beforeChannelAcquireCount.get());

            VfdRegistry.AcquiredChannel acquired = registry.acquireChannel(vfd);
            try {
                assertNotNull(acquired.channel());
                assertEquals("beforeChannelAcquire should fire once",
                    1, injector.beforeChannelAcquireCount.get());
            } finally {
                acquired.release();
            }

            // Second acquire should fire again
            VfdRegistry.AcquiredChannel acquired2 = registry.acquireChannel(vfd);
            try {
                assertEquals("beforeChannelAcquire should fire twice",
                    2, injector.beforeChannelAcquireCount.get());
            } finally {
                acquired2.release();
            }
        } finally {
            registry.release(vfd);
            registry.close();
        }
    }

    /**
     * Verify that beforePoolAcquire fires when a buffer is acquired
     * from the MemoryPool.
     */
    public void testBeforePoolAcquireFires() throws Exception {
        RecordingFaultInjector injector = new RecordingFaultInjector();
        MemoryPool pool = new MemoryPool(4, BLOCK_SIZE, 0, injector);

        try {
            assertEquals(0, injector.beforePoolAcquireCount.get());

            ByteBuffer buf = pool.acquire(true, 5000L);
            assertNotNull(buf);
            assertEquals("beforePoolAcquire should fire once",
                1, injector.beforePoolAcquireCount.get());
        } finally {
            pool.close();
        }
    }

    /**
     * Verify that a beforeRead injector that throws IOException propagates
     * the exception to the caller.
     */
    public void testBeforeReadInjectorThrowsPropagates() throws Exception {
        ThrowingFaultInjector injector = new ThrowingFaultInjector();
        VfdRegistry registry = new VfdRegistry(10, 5, 5000L, injector);
        MemoryPool pool = new MemoryPool(8, BLOCK_SIZE, 0);
        BlockCache blockCache = new BlockCache(64, registry);

        Path tempFile = createTempFile("fi-throw", ".dat");
        Files.write(tempFile, new byte[BLOCK_SIZE]);

        VirtualFileDescriptor vfd = registry.acquire(tempFile);
        try {
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, new FileChannelIOBackend(), pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                null, BLOCK_SIZE, injector);

            IOException ex = expectThrows(IOException.class,
                () -> channel.readBlock(vfd.id(), 0, 0L, true));
            assertEquals("injected fault", ex.getMessage());
        } finally {
            registry.release(vfd);
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    /**
     * Verify that a cache hit does NOT fire beforeRead/afterRead
     * (injection points are only on the physical I/O path).
     */
    public void testCacheHitDoesNotFireReadInjection() throws Exception {
        RecordingFaultInjector injector = new RecordingFaultInjector();
        VfdRegistry registry = new VfdRegistry(10, 5, 5000L, injector);
        MemoryPool pool = new MemoryPool(8, BLOCK_SIZE, 0, injector);
        BlockCache blockCache = new BlockCache(64, registry);

        Path tempFile = createTempFile("fi-cachehit", ".dat");
        Files.write(tempFile, new byte[BLOCK_SIZE]);

        VirtualFileDescriptor vfd = registry.acquire(tempFile);
        try {
            VirtualFileChannel channel = new VirtualFileChannel(
                vfd, new FileChannelIOBackend(), pool, blockCache,
                registry, new Semaphore(64),
                VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
                null, BLOCK_SIZE, injector);

            // First read: physical I/O
            channel.readBlock(vfd.id(), 0, 0L, true);
            assertEquals(1, injector.beforeReadCount.get());
            assertEquals(1, injector.afterReadCount.get());

            // Second read: cache hit — no physical I/O
            channel.readBlock(vfd.id(), 0, 0L, true);
            assertEquals("beforeRead should NOT fire on cache hit",
                1, injector.beforeReadCount.get());
            assertEquals("afterRead should NOT fire on cache hit",
                1, injector.afterReadCount.get());
        } finally {
            registry.release(vfd);
            blockCache.close();
            pool.close();
            registry.close();
        }
    }

    // ---- Test injector implementations ----

    /**
     * A FaultInjector that records how many times each method is called.
     */
    static class RecordingFaultInjector implements FaultInjector {
        final AtomicInteger beforeReadCount = new AtomicInteger();
        final AtomicInteger afterReadCount = new AtomicInteger();
        final AtomicInteger beforeChannelAcquireCount = new AtomicInteger();
        final AtomicInteger afterChannelCloseCount = new AtomicInteger();
        final AtomicInteger beforePoolAcquireCount = new AtomicInteger();
        final AtomicInteger afterCleanerCallbackCount = new AtomicInteger();

        @Override
        public void beforeRead(long vfdId, long fileOffset) {
            beforeReadCount.incrementAndGet();
        }

        @Override
        public void afterRead(long vfdId, long fileOffset, ByteBuffer buffer) {
            afterReadCount.incrementAndGet();
        }

        @Override
        public void beforeChannelAcquire(long vfdId) {
            beforeChannelAcquireCount.incrementAndGet();
        }

        @Override
        public void afterChannelClose(long vfdId) {
            afterChannelCloseCount.incrementAndGet();
        }

        @Override
        public void beforePoolAcquire() {
            beforePoolAcquireCount.incrementAndGet();
        }

        @Override
        public void afterCleanerCallback() {
            afterCleanerCallbackCount.incrementAndGet();
        }
    }

    /**
     * A FaultInjector that throws IOException on beforeRead to test
     * exception propagation.
     */
    static class ThrowingFaultInjector implements FaultInjector {
        @Override
        public void beforeRead(long vfdId, long fileOffset) throws IOException {
            throw new IOException("injected fault");
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
}
