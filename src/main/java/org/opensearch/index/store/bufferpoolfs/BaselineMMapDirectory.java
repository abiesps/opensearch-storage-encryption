package org.opensearch.index.store.bufferpoolfs;

import org.apache.lucene.store.MMapDirectory;

import java.io.IOException;
import java.nio.file.Path;

public class BaselineMMapDirectory extends MMapDirectory {
    /**
     * Create a new MMapDirectory for the named location and {@link FSLockFactory#getDefault()}. The
     * directory is created at the named location if it does not yet exist.
     *
     * @param path         the path of the directory
     * @param maxChunkSize maximum chunk size (for default see {@link #DEFAULT_MAX_CHUNK_SIZE}) used
     *                     for memory mapping.
     * @throws IOException if there is a low-level I/O error
     */
    public BaselineMMapDirectory(Path path, long maxChunkSize) throws IOException {
        super(path, maxChunkSize);
    }
}
