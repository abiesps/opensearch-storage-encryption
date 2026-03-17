/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.locks.LockSupport;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;

/**
 * A no-op variant of {@link BlockSlotTinyCache} for benchmarking.
 *
 * <p>Skips the 32-slot L1 cache entirely — every {@code acquireRefCountedValue}
 * call goes directly to the L2 Caffeine cache. This allows measuring the
 * overhead of the L1 tiny cache by comparing benchmarks with and without it.
 */
public class NoOpBlockSlotTinyCache extends BlockSlotTinyCache {

    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path normalizedPath;
    private final String normalizedPathString;

    public NoOpBlockSlotTinyCache(BlockCache<RefCountedMemorySegment> cache, Path path, long fileLength) {
        super(cache, path, fileLength);
        this.cache = cache;
        this.normalizedPath = path.toAbsolutePath().normalize();
        this.normalizedPathString = this.normalizedPath.toString();
    }

    @Override
    public BlockCacheValue<RefCountedMemorySegment> acquireRefCountedValue(long blockOff, CacheHitHolder hitHolder) throws IOException {
        final int maxAttempts = 10;

        FileBlockCacheKey key = new FileBlockCacheKey(normalizedPath, normalizedPathString, blockOff);

        for (int attempts = 0; attempts < maxAttempts; attempts++) {
            BlockCacheValue<RefCountedMemorySegment> v = cache.get(key);
            if (v != null) {
                final int expectedGen = v.value().getGeneration();
                if (v.tryPin()) {
                    if (v.value().getGeneration() == expectedGen) {
                        if (hitHolder != null) hitHolder.setWasCacheHit(true);
                        return v;
                    }
                    v.unpin();
                }
            }

            BlockCacheValue<RefCountedMemorySegment> loaded = cache.getOrLoad(key);
            if (loaded != null) {
                final int expectedGen = loaded.value().getGeneration();
                if (loaded.tryPin()) {
                    if (loaded.value().getGeneration() == expectedGen) {
                        if (hitHolder != null) hitHolder.setWasCacheHit(false);
                        return loaded;
                    }
                    loaded.unpin();
                }
            }

            if (attempts < maxAttempts - 1) {
                LockSupport.parkNanos(50_000L << attempts);
            }
        }

        throw new IOException("Unable to pin memory segment for block offset " + blockOff + " after " + maxAttempts + " attempts");
    }
}
