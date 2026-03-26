/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.harness;

import com.github.benmanes.caffeine.cache.Cache;

import org.opensearch.index.store.block_cache_v2.CacheController;

/**
 * Adapts a raw Caffeine {@link Cache} to the {@link CacheController} interface
 * so {@code DirectMemoryAllocator} can shrink/restore/query the cache.
 */
public final class HarnessCacheController implements CacheController {

    private final Cache<?, ?> cache;
    private final int blockSize;
    private final long originalMaxBlocks;

    public HarnessCacheController(Cache<?, ?> cache, int blockSize, long originalMaxBlocks) {
        this.cache = cache;
        this.blockSize = blockSize;
        this.originalMaxBlocks = originalMaxBlocks;
    }

    @Override
    public long cacheHeldBytes() {
        return cache.estimatedSize() * blockSize;
    }

    @Override
    public void setMaxBlocks(long n) {
        long clamped = Math.max(1, n);
        cache.policy().eviction().ifPresent(e -> e.setMaximum(clamped));
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

    @Override
    public long currentMaxBlocks() {
        return cache.policy().eviction()
            .map(e -> e.getMaximum())
            .orElse(originalMaxBlocks);
    }

    @Override
    public long originalMaxBlocks() {
        return originalMaxBlocks;
    }
}
