/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.index.store.bufferpoolfs.RadixBlockTable;

/**
 * Shared, reference-counted cache of {@link RadixBlockTable} instances
 * keyed by file absolute path. Multiple {@code DirectByteBufferIndexInput}
 * parents opened for the same file share a single L1 table. Clones and
 * slices inherit the parent's table without affecting the reference count.
 *
 * <h2>Thread safety</h2>
 * All mutations go through {@link ConcurrentHashMap#compute}, which
 * serialises per-key updates. This guarantees that lookup-or-create
 * ({@link #acquire}) and decrement-or-remove ({@link #release}) are
 * atomic with respect to each other for the same key.
 */
@SuppressWarnings("preview")
public final class RadixBlockTableCache {

    /**
     * Holds a {@link RadixBlockTable} and a reference count tracking
     * the number of live parent {@code IndexInput} instances sharing
     * this table.
     */
    static final class RefCountedEntry {
        final RadixBlockTable<MemorySegment> table;
        final AtomicInteger refCount;

        RefCountedEntry() {
            this.table = new RadixBlockTable<>();
            this.refCount = new AtomicInteger(1);
        }
    }

    private final ConcurrentHashMap<String, RefCountedEntry> cache = new ConcurrentHashMap<>();

    /**
     * Acquires a shared {@link RadixBlockTable} for the given file path.
     * If no entry exists, a new one is created with refCount 1.
     * If an entry already exists, its refCount is incremented.
     *
     * @param filePath absolute file path (must not be null)
     * @return the shared RadixBlockTable for this path
     */
    public RadixBlockTable<MemorySegment> acquire(String filePath) {
        RefCountedEntry entry = cache.compute(filePath, (key, existing) -> {
            if (existing == null) {
                return new RefCountedEntry();
            }
            existing.refCount.incrementAndGet();
            return existing;
        });
        return entry.table;
    }

    /**
     * Releases one reference for the given file path. If the refCount
     * reaches zero, the table is cleared and the entry is removed.
     *
     * @param filePath absolute file path
     * @throws IllegalStateException if no entry exists or refCount is already 0
     */
    public void release(String filePath) {
        cache.compute(filePath, (key, existing) -> {
            if (existing == null) {
                throw new IllegalStateException(
                        "release() called for absent path: " + filePath);
            }
            int newCount = existing.refCount.decrementAndGet();
            if (newCount < 0) {
                throw new IllegalStateException(
                        "refCount went negative for path: " + filePath);
            }
            if (newCount == 0) {
                existing.table.clear();
                return null; // remove entry from map
            }
            return existing;
        });
    }

    /**
     * Returns the {@link RadixBlockTable} for the given path, or {@code null}
     * if no entry exists. Used by the L2 eviction listener to propagate
     * removals to L1.
     *
     * @param filePath absolute file path
     * @return the table, or null if the path has no active entry
     */
    public RadixBlockTable<MemorySegment> getTable(String filePath) {
        RefCountedEntry entry = cache.get(filePath);
        return entry != null ? entry.table : null;
    }

    /**
     * Returns the number of file paths currently in the cache.
     * Useful for diagnostics and testing.
     */
    public int size() {
        return cache.size();
    }
}
