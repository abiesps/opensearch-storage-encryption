/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

/**
 * Node-level cache of {@link FileChannel} instances, bounded by max open FDs.
 *
 * <p>Each cached entry is a {@link RefCountedChannel}. The cache holds one "base"
 * reference. When Caffeine evicts an entry (size pressure or explicit invalidation),
 * the base ref is released via {@link RefCountedChannel#releaseBase()}. If no I/O
 * is in flight, the FileChannel closes immediately. If I/O is in flight, the channel
 * stays open until the last I/O completes.
 *
 * <p>Usage pattern in BlockLoader:
 * <pre>{@code
 *   try (RefCountedChannel ref = fileChannelCache.acquire(path)) {
 *       ref.channel().read(buf, offset);
 *   }
 * </pre>
 *
 * <p>For EFS hub migration: call {@link #close()} which invalidates all entries.
 * Idle channels close immediately. In-flight channels close when I/O finishes.
 */
public class FileChannelCache implements Closeable {

    private static final int MAX_ACQUIRE_RETRIES = 3;

    private final Cache<String, RefCountedChannel> fdCache;
    private final OpenOption directOpenOption;

    /**
     * @param maxOpenFDs maximum number of cached FileChannels
     * @param directOpenOption the O_DIRECT open option, or null for buffered I/O
     */
    public FileChannelCache(int maxOpenFDs, OpenOption directOpenOption) {
        this.directOpenOption = directOpenOption;
        this.fdCache = Caffeine
            .newBuilder()
            .maximumSize(maxOpenFDs)
            .evictionListener((String path, RefCountedChannel ref, RemovalCause cause) -> {
                if (ref != null) {
                    ref.releaseBase();
                }
            })
            .build();
    }

    /**
     * Acquire a {@link RefCountedChannel} for the given path. The returned ref
     * has already been acquired (refCount incremented) and MUST be closed when
     * done, typically via try-with-resources.
     *
     * <p>If the cached entry was concurrently evicted (stale), this method
     * transparently evicts the stale entry and retries up to {@link #MAX_ACQUIRE_RETRIES} times.
     *
     * @param normalizedPath the absolute normalized file path
     * @return an acquired RefCountedChannel — never null
     * @throws IOException if the channel cannot be opened or acquired after retries
     */
    public RefCountedChannel acquire(String normalizedPath) throws IOException {
        for (int attempt = 0; attempt < MAX_ACQUIRE_RETRIES; attempt++) {
            RefCountedChannel ref = fdCache.get(normalizedPath, this::openChannel);
            try {
                return ref.acquire();
            } catch (IllegalStateException e) {
                // Stale entry — evict and retry
                fdCache.asMap().remove(normalizedPath, ref);
            }
        }
        throw new IOException("Failed to acquire FileChannel after " + MAX_ACQUIRE_RETRIES + " retries: " + normalizedPath);
    }

    /**
     * Invalidate the cached channel for a specific path.
     * The eviction listener will release the base reference.
     */
    public void invalidate(String normalizedPath) {
        fdCache.invalidate(normalizedPath);
    }

    private RefCountedChannel openChannel(String path) {
        try {
            FileChannel fc;
            if (directOpenOption != null) {
                fc = FileChannel.open(Paths.get(path), StandardOpenOption.READ, directOpenOption);
            } else {
                fc = FileChannel.open(Paths.get(path), StandardOpenOption.READ);
            }
            return new RefCountedChannel(fc);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open FileChannel: " + path, e);
        }
    }

    @Override
    public void close() {
        fdCache.invalidateAll();
        fdCache.cleanUp();
    }
}
