/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Node-level singleton that assigns a globally unique integer vfd to each openInput() call.
 * Each parent IndexInput gets its own vfd, even for the same file path (Option 1).
 * This ensures a 1:1 mapping between vfd and SparseLongBlockTable.
 *
 * The vfdToPath map is needed so the cache loader can resolve the file path from a vfd
 * when loading blocks from disk.
 */
public final class VirtualFileDescriptorRegistry {
    private static final VirtualFileDescriptorRegistry INSTANCE = new VirtualFileDescriptorRegistry();

    private final AtomicInteger nextVfd = new AtomicInteger(1);
    private final ConcurrentHashMap<Integer, String> vfdToPath = new ConcurrentHashMap<>();

    private VirtualFileDescriptorRegistry() {}

    public static VirtualFileDescriptorRegistry getInstance() { return INSTANCE; }

    /**
     * Assigns a new unique vfd for this openInput() call.
     * Always returns a fresh vfd — no deduplication by path.
     */
    public int register(Path absolutePath) {
        String normalized = absolutePath.toAbsolutePath().normalize().toString();
        int vfd = nextVfd.getAndIncrement();
        if (vfd < 0) { // wrapped past Integer.MAX_VALUE
            throw new IllegalStateException("VFD counter overflow");
        }
        vfdToPath.put(vfd, normalized);
        return vfd;
    }

    /**
     * Returns the path for a vfd, or null if not registered.
     */
    public String getPath(int vfd) {
        return vfdToPath.get(vfd);
    }

    /**
     * Removes the mapping for a vfd. Called when parent IndexInput is closed.
     */
    public void deregister(int vfd) {
        vfdToPath.remove(vfd);
    }

    /** For testing / metrics. */
    public int size() { return vfdToPath.size(); }

    /** Reset for test isolation. */
    public void clear() {
        vfdToPath.clear();
        nextVfd.set(1);
    }
}
