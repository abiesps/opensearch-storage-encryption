/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.nio.file.Path;

/**
 * Cache key for file block cache entries.
 * 
 * <p>Each cache key uniquely identifies a block within a file by combining:
 * <ul>
 * <li>Full file path - absolute normalized path to ensure uniqueness across directories</li>
 * <li>File offset - identifies the position within the file where the block starts</li>
 * </ul>
 * 
 * <p>This key implementation is generic and can be used with any file-based block caching
 * system, not limited to DirectIO operations.
 * 
 * <p>Hash code is precomputed for optimal performance in concurrent environments.
 * <p>Using full path instead of just filename prevents cache key collisions between
 * different directories that contain files with the same name (e.g., multiple indexes
 * with "_6.cfs" files).
 */
public final class FileBlockCacheKey implements BlockCacheKey {

    private final Path filePath;
    private final long fileOffset;
    private final String pathString; // cached for fast comparison
    private int hash; // 0 means "not yet computed"

    /**
     * Creates a new FileBlockCacheKey for the specified file and offset.
     * 
     * <p>The file path is normalized and converted to an absolute path to ensure
     * consistent cache keys across different directory representations.
     * 
     * @param filePath the path to the file containing the cached block
     * @param fileOffset the byte offset within the file where the block starts
     */
    public FileBlockCacheKey(Path filePath, long fileOffset) {
        this.filePath = filePath.toAbsolutePath().normalize();
        this.fileOffset = fileOffset;
        this.pathString = this.filePath.toString();
    }

    /**
     * Fast constructor that accepts a pre-normalized path and its cached string representation.
     * Skips {@code toAbsolutePath().normalize()} — caller is responsible for ensuring the path
     * is already absolute and normalized.
     *
     * <p>This is used on the hot read path where the same file's path is reused across many
     * block lookups, avoiding repeated {@code UnixPath.normalize()} overhead.
     *
     * @param normalizedPath pre-normalized absolute path
     * @param pathString cached {@code normalizedPath.toString()}
     * @param fileOffset the byte offset within the file where the block starts
     */
    public FileBlockCacheKey(Path normalizedPath, String pathString, long fileOffset) {
        this.filePath = normalizedPath;
        this.fileOffset = fileOffset;
        this.pathString = pathString;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            // compute once - use cached string for consistent hashing
            h = 31 * pathString.hashCode() + Long.hashCode(fileOffset);
            if (h == 0)
                h = 1; // avoid sentinel clash
            hash = h;
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof FileBlockCacheKey other))
            return false;
        return fileOffset == other.fileOffset && pathString.equals(other.pathString);
    }

    @Override
    public String toString() {
        return "FileBlockCacheKey[filePath=" + filePath + ", fileOffset=" + fileOffset + "]";
    }

    @Override
    public long offset() {
        return fileOffset;
    }

    @Override
    public Path filePath() {
        return filePath;
    }

    /**
     * Returns the byte offset within the file where this cached block starts.
     * 
     * @return the file offset in bytes
     */
    public long fileOffset() {
        return fileOffset;
    }
}
