/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

/**
 * L2 cache key combining file path and block ID.
 * The file path lets the eviction listener resolve the shared
 * {@link org.opensearch.index.store.bufferpoolfs.RadixBlockTable}
 * from the {@link RadixBlockTableCache} without a VFD registry.
 *
 * @param filePath absolute file path
 * @param blockId  block index within the file
 */
public record BlockCacheKeyV2(String filePath, long blockId) {

    /**
     * Returns the byte offset of this block within the file.
     */
    public long blockOffset() {
        return blockId * CACHE_BLOCK_SIZE;
    }
}
