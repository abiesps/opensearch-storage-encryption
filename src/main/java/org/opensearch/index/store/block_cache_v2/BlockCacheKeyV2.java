/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.nio.file.Path;

import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

/**
 * Cache key using integer vfd + long blockId (pre-shifted).
 * No Path or String references — zero allocation on the hot path.
 *
 * blockId = fileOffset >>> CACHE_BLOCK_SIZE_POWER
 * blockOffset() reconstructs the byte offset when needed (e.g., disk load).
 */
public final class BlockCacheKeyV2 implements BlockCacheKey {
    private final int vfd;
    private final long blockId;

    public BlockCacheKeyV2(int vfd, long blockId) {
        this.vfd = vfd;
        this.blockId = blockId;
    }

    public int vfd() { return vfd; }
    public long blockId() { return blockId; }

    /** Reconstruct byte offset when needed (e.g., disk load). */
    public long blockOffset() { return blockId << StaticConfigs.CACHE_BLOCK_SIZE_POWER; }

    @Override
    public int hashCode() {
        return vfd * 31 ^ Long.hashCode(blockId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BlockCacheKeyV2 k)) return false;
        return vfd == k.vfd && blockId == k.blockId;
    }

    // BlockCacheKey interface — not used on hot path, only for compatibility
    @Override
    public Path filePath() {
        String path = VirtualFileDescriptorRegistry.getInstance().getPath(vfd);
        return path != null ? Path.of(path) : null;
    }

    @Override
    public long offset() { return blockOffset(); }

    @Override
    public String toString() {
        return "BlockCacheKeyV2{vfd=" + vfd + ", blockId=" + blockId + "}";
    }
}
