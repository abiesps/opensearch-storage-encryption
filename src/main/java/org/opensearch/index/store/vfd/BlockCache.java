/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.opensearch.index.store.bufferpoolfs.RadixBlockTable;

/**
 * Primitive-key Caffeine L2 block cache. Keys are {@code long} values
 * encoded via {@link VirtualPage} (VFD id + block id). Values are
 * {@link MemorySegment} references to cached block data.
 *
 * <p>On eviction, the cache cascades removal into the per-VFD L1
 * {@link RadixBlockTable} via the {@link VfdRegistry}.</p>
 *
 * <p><b>Satisfies: Requirements 23, 24</b></p>
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
public class BlockCache implements AutoCloseable {

    private final Cache<Long, MemorySegment> cache;
    private final VfdRegistry registry;
    private final CacheMetrics metrics;
    private final ExecutorService evictionExecutor;

    /**
     * Creates a new BlockCache.
     *
     * @param maxBlocks maximum number of blocks to cache
     * @param registry  the VfdRegistry for L1 eviction cascade
     */
    public BlockCache(long maxBlocks, VfdRegistry registry) {
        this.registry = registry;
        this.metrics = new CacheMetrics();

        this.evictionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "block-cache-eviction");
            t.setDaemon(true);
            return t;
        });

        this.cache = Caffeine.newBuilder()
            .maximumSize(maxBlocks)
            .removalListener((Long key, MemorySegment value, RemovalCause cause) -> {
                if (key == null) return;
                onEviction(key, cause);
            })
            .executor(evictionExecutor)
            .build();
    }

    /**
     * L1 eviction cascade: extract VFD id and block id from the key,
     * look up the RadixBlockTable, and remove the block entry.
     */
    private void onEviction(Long key, RemovalCause cause) {
        long vfdId = VirtualPage.extractVfdId(key);
        int blockId = VirtualPage.extractBlockId(key);

        RadixBlockTable<MemorySegment> rbt = registry.getRadixBlockTable(vfdId);
        if (rbt != null) {
            rbt.remove(blockId);
            metrics.l1CascadeRemovalCount.increment();
        }
        metrics.evictionCount.increment();
    }

    /**
     * Get a cached block by its encoded key.
     *
     * @param key the VirtualPage-encoded key
     * @return the cached MemorySegment, or null if not present
     */
    public MemorySegment get(long key) {
        MemorySegment result = cache.getIfPresent(key);
        if (result != null) {
            metrics.hitCount.increment();
        } else {
            metrics.missCount.increment();
        }
        return result;
    }

    /**
     * Put a block into the cache.
     *
     * @param key   the VirtualPage-encoded key
     * @param value the MemorySegment to cache
     */
    public void put(long key, MemorySegment value) {
        cache.put(key, value);
    }

    /**
     * Invalidate a specific cache entry.
     *
     * @param key the VirtualPage-encoded key to invalidate
     */
    public void invalidate(long key) {
        cache.invalidate(key);
    }

    /**
     *
     * ToDo : Is this scan fine ?
     * Eagerly invalidate all L2 cache entries belonging to a specific VFD.
     * Called from {@link VfdRegistry#release} when a VFD's refCount drops to 0,
     * preventing orphaned cache entries from holding pool buffers indefinitely.
     *
     * <p>Scans the cache key set and removes entries whose encoded VFD ID
     * matches the given {@code vfdId}. This is O(n) in cache size but only
     * runs on VFD close — not on the hot read path.</p>
     *
     * @param vfdId the VFD identifier whose entries should be evicted
     */
    public void invalidateForVfd(long vfdId) {
        cache.asMap().keySet().removeIf(key -> VirtualPage.extractVfdId(key) == vfdId);
    }


    /** Returns the estimated number of entries in the cache. */
    public long estimatedSize() {
        return cache.estimatedSize();
    }

    /** Perform Caffeine maintenance (for testing). */
    public void cleanUp() {
        cache.cleanUp();
    }

    /** Returns the cache metrics. */
    public CacheMetrics metrics() {
        return metrics;
    }

    @Override
    public void close() {
        cache.invalidateAll();
        cache.cleanUp();
        evictionExecutor.shutdown();
        try {
            if (!evictionExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                evictionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            evictionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Observable metrics for the BlockCache.
     */
    public static final class CacheMetrics {
        public final LongAdder hitCount = new LongAdder();
        public final LongAdder missCount = new LongAdder();
        public final LongAdder evictionCount = new LongAdder();
        public final LongAdder l1CascadeRemovalCount = new LongAdder();

        CacheMetrics() {}

        public long estimatedSize() {
            return hitCount.sum() + missCount.sum();
        }

        @Override
        public String toString() {
            return "CacheMetrics[hits=" + hitCount.sum()
                + ", misses=" + missCount.sum()
                + ", evictions=" + evictionCount.sum()
                + ", l1Cascades=" + l1CascadeRemovalCount.sum() + "]";
        }
    }
}
