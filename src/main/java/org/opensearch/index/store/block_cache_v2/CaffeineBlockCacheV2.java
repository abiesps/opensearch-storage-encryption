/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.opensearch.index.store.bufferpoolfs.RadixBlockTable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

/**
 * V2 block cache using Caffeine with direct ByteBuffer values.
 * No RefCountedMemorySegment, no MemorySegmentPool, no pin/unpin.
 * GC handles ByteBuffer reclamation after eviction.
 *
 * Integrates {@link DirectMemoryAdmissionController} to gate
 * {@code ByteBuffer.allocateDirect()} calls with soft/hard thresholds.
 *
 * <p>Implements {@link CacheController} so the {@link DirectMemoryAllocator}
 * can query cache state and take proportional corrective actions (GC hint,
 * cache shrink, gradual restore) under memory pressure.
 *
 * <p>L2 eviction propagates to L1 via {@link RadixBlockTableCache}: the
 * removal listener extracts the file path from the key and looks up the
 * shared {@link RadixBlockTable} to null the evicted slot.
 */
@SuppressWarnings("removal") // DirectMemoryAdmissionController is deprecated; retained during gradual migration
public final class CaffeineBlockCacheV2 implements CacheController {

    private final Cache<BlockCacheKeyV2, ByteBuffer> cache;
    private final RadixBlockTableCache radixBlockTableCache;
    private final DirectMemoryAdmissionController admissionController;
    private final DirectMemoryAllocator allocator;
    private final long originalMaxBlocks;

    public CaffeineBlockCacheV2(long maxBlocks, DirectMemoryAdmissionController admissionController,
                                DirectMemoryAllocator allocator,
                                RadixBlockTableCache radixBlockTableCache) {
        this.radixBlockTableCache = radixBlockTableCache;
        this.admissionController = admissionController;
        this.allocator = allocator;
        this.originalMaxBlocks = maxBlocks;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxBlocks)
                .removalListener(this::onRemoval)
                .build();

        // Register this CacheController so the allocator can diagnose pressure
        // and take proportional corrective action (GC hint vs cache shrink)
        allocator.registerCacheController(this);
    }

    public CaffeineBlockCacheV2(long maxBlocks, DirectMemoryAdmissionController admissionController,
                                DirectMemoryAllocator allocator) {
        this(maxBlocks, admissionController, allocator, new RadixBlockTableCache());
    }

    public CaffeineBlockCacheV2(long maxBlocks, DirectMemoryAdmissionController admissionController) {
        this(maxBlocks, admissionController, new DirectMemoryAllocator());
    }

    public CaffeineBlockCacheV2(long maxBlocks) {
        this(maxBlocks, new DirectMemoryAdmissionController(), new DirectMemoryAllocator());
    }

    /**
     * Returns the cached ByteBuffer for the key, or loads it from disk.
     * The returned ByteBuffer is a read-only slice of the cached parent buffer.
     */
    public ByteBuffer getOrLoad(BlockCacheKeyV2 key) throws IOException {
        try {
            ByteBuffer parent = cache.get(key, k -> loadBlock(k));
            if (parent == null) {
                throw new IOException("Failed to load block: " + key);
            }
            // Return a read-only slice so readers can't corrupt the cached data
            return parent.asReadOnlyBuffer();
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException ioe) throw ioe;
            throw new IOException("Failed to load block: " + key, e);
        }
    }

    /**
     * Returns the cached ByteBuffer if present, or null.
     */
    public ByteBuffer getIfPresent(BlockCacheKeyV2 key) {
        ByteBuffer parent = cache.getIfPresent(key);
        return parent != null ? parent.asReadOnlyBuffer() : null;
    }

    /**
     * Returns the {@link RadixBlockTableCache} used by this cache.
     */
    public RadixBlockTableCache getRadixBlockTableCache() {
        return radixBlockTableCache;
    }

    /**
     * Invalidates all cache entries for a given file path.
     */
    public void invalidate(String filePath) {
        // Scan and invalidate — not ideal for production but fine for POC
        cache.asMap().keySet().removeIf(k -> filePath.equals(k.filePath()));
    }

    public long estimatedSize() {
        return cache.estimatedSize();
    }

    /**
     * Forces pending eviction listener callbacks to execute synchronously.
     * Useful for testing. Also satisfies {@link CacheController#cleanUp()}.
     */
    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

    /**
     * Returns the admission controller used by this cache.
     */
    public DirectMemoryAdmissionController getAdmissionController() {
        return admissionController;
    }

    /**
     * Returns the memory allocator used by this cache.
     */
    public DirectMemoryAllocator getAllocator() {
        return allocator;
    }

    public void close() {
        cache.invalidateAll();
        cache.cleanUp();
        allocator.deregisterCacheController();
    }

    // ======================== CacheController implementation ========================

    /** {@inheritDoc} */
    @Override
    public long cacheHeldBytes() {
        return cache.estimatedSize() * CACHE_BLOCK_SIZE;
    }

    /**
     * {@inheritDoc}
     * <p>Sets the Caffeine cache maximum to {@code max(1, newMaxBlocks)}.
     * Used for both shrink and restore by the allocator.
     */
    @Override
    public void setMaxBlocks(long newMaxBlocks) {
        cache.policy().eviction().ifPresent(e -> e.setMaximum(Math.max(1, newMaxBlocks)));
    }

    /** {@inheritDoc} */
    @Override
    public long currentMaxBlocks() {
        return cache.policy().eviction()
                .map(e -> e.getMaximum()).orElse(originalMaxBlocks);
    }

    /** {@inheritDoc} */
    @Override
    public long originalMaxBlocks() {
        return originalMaxBlocks;
    }

    // ---- internal ----

    private ByteBuffer loadBlock(BlockCacheKeyV2 key) {
        admissionController.acquire(CACHE_BLOCK_SIZE);
        try {
            String path = key.filePath();
            ByteBuffer buf;
            try {
                buf = allocator.allocate(CACHE_BLOCK_SIZE);
            } catch (MemoryBackPressureException mbpe) {
                // Required operation (on-demand block read): wrap in OutOfMemoryError
                // so the Lucene layer sees the system cannot serve the read (Req 13.4)
                OutOfMemoryError oome = new OutOfMemoryError(mbpe.getMessage());
                oome.initCause(mbpe);
                throw oome;
            }
            // Use NativeFileIO pread for non-encrypted block reads (Req 9.5)
            long fileSize = Files.size(Path.of(path));
            long offset = key.blockOffset();
            int toRead = (int) Math.min(CACHE_BLOCK_SIZE, fileSize - offset);
            buf.limit(toRead);
            int fd = NativeFileIO.open(path, NativeFileIO.O_RDONLY);
            try {
                NativeFileIO.pread(fd, buf, offset, toRead);
            } finally {
                NativeFileIO.close(fd);
            }
            buf.flip();
            return buf;
        } catch (OutOfMemoryError oome) {
            // Let OOM (including our wrapped MemoryBackPressureException) propagate
            admissionController.release(CACHE_BLOCK_SIZE);
            throw oome;
        } catch (Exception e) {
            admissionController.release(CACHE_BLOCK_SIZE);
            throw (e instanceof RuntimeException re) ? re : new RuntimeException(e);
        }
    }

    private void onRemoval(BlockCacheKeyV2 key, ByteBuffer value, RemovalCause cause) {
        if (key == null) return;
        admissionController.release(CACHE_BLOCK_SIZE);

        // Propagate L2 eviction to L1 via RadixBlockTableCache (Req 6.1, 6.2, 6.3)
        RadixBlockTable<?> table = radixBlockTableCache.getTable(key.filePath());
        if (table == null) return; // table already removed — graceful no-op
        table.remove(key.blockId());
    }
}
