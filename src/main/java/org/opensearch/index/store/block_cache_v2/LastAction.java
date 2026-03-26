/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

/**
 * Enumerates the most recent corrective action taken by
 * {@link DirectMemoryAllocator} during a diagnostic sample.
 *
 * <p>Exposed via {@code DirectMemoryAllocator.getLastAction()} for
 * observability and test assertions.
 *
 * @see DirectMemoryAllocator
 */
public enum LastAction {

    /** No corrective action was taken (Tier 1 / low pressure). */
    NONE,

    /** A rate-limited {@code System.gc()} hint was issued (Tier 2). */
    GC_HINT,

    /** The cache capacity was proportionally shrunk (Tier 3). */
    CACHE_SHRINK,

    /** The cache capacity was gradually restored toward its original size. */
    CACHE_RESTORE
}
