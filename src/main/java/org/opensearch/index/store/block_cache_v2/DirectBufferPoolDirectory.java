/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.bufferpoolfs.SparseLongBlockTable;

/**
 * POC directory that uses DirectByteBufferIndexInput for reads
 * and delegates to a plain FSDirectory for writes (no encryption).
 * Designed for benchmarking V2 block cache against MMapDirectory.
 */
public class DirectBufferPoolDirectory extends FSDirectory {

    private final CaffeineBlockCacheV2 blockCache;
    private final VirtualFileDescriptorRegistry vfdRegistry;
    private final FSDirectory writeDelegate;
    private SparseLongBlockTable blockTableOverride;

    public DirectBufferPoolDirectory(Path path, CaffeineBlockCacheV2 blockCache) throws IOException {
        super(path, FSLockFactory.getDefault());
        this.blockCache = blockCache;
        this.vfdRegistry = VirtualFileDescriptorRegistry.getInstance();
        // Use a plain FSDirectory for writes — no encryption for POC
        this.writeDelegate = FSDirectory.open(path);
    }

    /**
     * Sets a SparseLongBlockTable override for all subsequent openInput calls.
     * When set, all new IndexInputs will use this table instead of creating their own.
     * Pass a {@link org.opensearch.index.store.bufferpoolfs.NoOpSparseLongBlockTable}
     * to disable L1 caching for benchmarking.
     */
    public void setBlockTableOverride(SparseLongBlockTable blockTableOverride) {
        this.blockTableOverride = blockTableOverride;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        Path file = getDirectory().resolve(name);
        long fileSize = Files.size(file);
        if (fileSize == 0) {
            throw new IOException("Cannot open empty file: " + file);
        }

        int vfd = vfdRegistry.register(file.toAbsolutePath());
        if (blockTableOverride != null) {
            return DirectByteBufferIndexInput.newInstance(
                    "DirectByteBufferIndexInput(path=\"" + file + "\")",
                    file.toAbsolutePath(),
                    fileSize,
                    vfd,
                    blockCache,
                    blockTableOverride
            );
        }
        return DirectByteBufferIndexInput.newInstance(
                "DirectByteBufferIndexInput(path=\"" + file + "\")",
                file.toAbsolutePath(),
                fileSize,
                vfd,
                blockCache
        );
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        return writeDelegate.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        ensureOpen();
        return writeDelegate.createTempOutput(prefix, suffix, context);
    }

    @Override
    public void close() throws IOException {
        try {
            writeDelegate.close();
        } finally {
            super.close();
        }
        blockCache.close();
    }
}
