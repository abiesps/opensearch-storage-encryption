/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

/**
 * Abstraction over the block cache for memory pressure actions.
 * Decouples the allocator from Caffeine internals.
 * Implemented by {@code CaffeineBlockCacheV2}.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>{@link #cacheHeldBytes()} must return a non-negative value. It may be an
 *       estimate (e.g., Caffeine's {@code estimatedSize() × blockSize}).</li>
 *   <li>{@link #setMaxBlocks(long)} must be idempotent — calling with the same
 *       value twice has no additional effect.</li>
 *   <li>{@link #setMaxBlocks(long)} must clamp to {@code max(1, n)} — the cache
 *       capacity is never set to zero.</li>
 *   <li>{@link #setMaxBlocks(long)} is used for both shrink ({@code n < current})
 *       and restore ({@code n > current}). The name reflects "set capacity to n"
 *       semantics, not directional shrink.</li>
 *   <li>{@link #cleanUp()} forces Caffeine's eviction listeners to fire
 *       synchronously, ensuring evicted ByteBuffer references are dropped before
 *       the allocator proceeds.</li>
 *   <li>{@link #currentMaxBlocks()} and {@link #originalMaxBlocks()} must be
 *       consistent: {@code currentMaxBlocks() <= originalMaxBlocks()} after any
 *       shrink, and {@code currentMaxBlocks() == originalMaxBlocks()} when fully
 *       restored.</li>
 * </ul>
 */
public interface CacheController {

    /**
     * Returns estimated bytes held by live cache entries.
     *
     * @return a non-negative byte count; may be an estimate
     */
    long cacheHeldBytes();

    /**
     * Sets cache max to the given number of blocks. Used for both shrink and
     * restore. The implementation must clamp to {@code max(1, newMaxBlocks)} so
     * the cache is never set to zero capacity. This method is idempotent —
     * calling with the same value twice has no additional effect.
     *
     * @param newMaxBlocks the desired maximum number of cache blocks
     */
    void setMaxBlocks(long newMaxBlocks);

    /**
     * Forces pending eviction callbacks to run synchronously. This ensures
     * evicted ByteBuffer references are dropped before the allocator proceeds
     * with further diagnostics.
     */
    void cleanUp();

    /**
     * Returns the current cache max in blocks.
     *
     * @return the current maximum block count
     */
    long currentMaxBlocks();

    /**
     * Returns the originally configured cache max in blocks.
     *
     * @return the original maximum block count set at construction
     */
    long originalMaxBlocks();
}
