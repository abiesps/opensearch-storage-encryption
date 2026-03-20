/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;

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
 */
public final class CaffeineBlockCacheV2 {

    private final Cache<BlockCacheKeyV2, ByteBuffer> cache;
    private final ConcurrentHashMap<Integer, WeakReference<RadixBlockTable<MemorySegment>>> vfdToTable =
            new ConcurrentHashMap<>();
    private final VirtualFileDescriptorRegistry vfdRegistry;
    private final DirectMemoryAdmissionController admissionController;

    public CaffeineBlockCacheV2(long maxBlocks, DirectMemoryAdmissionController admissionController) {
        this.vfdRegistry = VirtualFileDescriptorRegistry.getInstance();
        this.admissionController = admissionController;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxBlocks)
                .removalListener(this::onRemoval)
                .build();
    }

    public CaffeineBlockCacheV2(long maxBlocks) {
        this(maxBlocks, new DirectMemoryAdmissionController());
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
     * Registers a RadixBlockTable for eviction-driven cleanup.
     */
    public void registerTable(int vfd, RadixBlockTable<MemorySegment> table) {
        vfdToTable.put(vfd, new WeakReference<>(table));
    }

    /**
     * Deregisters a table (called on parent close).
     */
    public void deregisterTable(int vfd) {
        vfdToTable.remove(vfd);
    }

    /**
     * Invalidates all cache entries for a given vfd.
     */
    public void invalidate(int vfd) {
        // Scan and invalidate — not ideal for production but fine for POC
        cache.asMap().keySet().removeIf(k -> k.vfd() == vfd);
    }

    public long estimatedSize() {
        return cache.estimatedSize();
    }

    /**
     * Forces pending eviction listener callbacks to execute synchronously.
     * Useful for testing.
     */
    public void cleanUp() {
        cache.cleanUp();
    }

    /**
     * Returns the admission controller used by this cache.
     */
    public DirectMemoryAdmissionController getAdmissionController() {
        return admissionController;
    }

    public void close() {
        cache.invalidateAll();
        cache.cleanUp();
        vfdToTable.clear();
    }

    // ---- internal ----

    private ByteBuffer loadBlock(BlockCacheKeyV2 key) {
        admissionController.acquire(CACHE_BLOCK_SIZE);
        try {
            String path = vfdRegistry.getPath(key.vfd());
            if (path == null) {
                throw new RuntimeException("No path registered for vfd=" + key.vfd());
            }
            ByteBuffer buf = ByteBuffer.allocateDirect(CACHE_BLOCK_SIZE);
            try (FileChannel ch = FileChannel.open(Path.of(path), StandardOpenOption.READ)) {
                long fileSize = ch.size();
                long offset = key.blockOffset(); // reconstructs from blockId
                int toRead = (int) Math.min(CACHE_BLOCK_SIZE, fileSize - offset);
                buf.limit(toRead);
                ch.position(offset);
                while (buf.hasRemaining()) {
                    int n = ch.read(buf);
                    if (n < 0) break;
                }
            }
            buf.flip();
            return buf;
        } catch (Exception e) {
            admissionController.release(CACHE_BLOCK_SIZE);
            throw (e instanceof RuntimeException re) ? re : new RuntimeException(e);
        }
    }

    private void onRemoval(BlockCacheKeyV2 key, ByteBuffer value, RemovalCause cause) {
        if (key == null) return;
        admissionController.release(CACHE_BLOCK_SIZE);

        WeakReference<RadixBlockTable<MemorySegment>> ref = vfdToTable.get(key.vfd());
        if (ref == null) return;
        RadixBlockTable<MemorySegment> table = ref.get();
        if (table == null) {
            vfdToTable.remove(key.vfd()); // opportunistic cleanup
            return;
        }
        // blockId stored directly in key — no shift needed
        table.remove(key.blockId());
    }
}
