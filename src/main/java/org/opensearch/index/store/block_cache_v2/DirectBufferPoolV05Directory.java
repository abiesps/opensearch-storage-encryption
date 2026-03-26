/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.bufferpoolfs.RadixBlockTable;

/**
 * Directory for V0.5 benchmarking: wires CachedMemorySegmentIndexInputV0_5
 * (V0 read path) with V2 infrastructure (CaffeineBlockCacheV2 + RadixBlockTable).
 * Plaintext writes (no encryption) — same as DirectBufferPoolDirectory.
 */
@SuppressWarnings("preview")
public class DirectBufferPoolV05Directory extends FSDirectory {

    private final CaffeineBlockCacheV2 blockCache;
    private final RadixBlockTableCache radixBlockTableCache;
    private final FSDirectory writeDelegate;
    private RadixBlockTable<MemorySegment> blockTableOverride;

    public DirectBufferPoolV05Directory(Path path, CaffeineBlockCacheV2 blockCache) throws IOException {
        super(path, FSLockFactory.getDefault());
        this.blockCache = blockCache;
        this.radixBlockTableCache = blockCache.getRadixBlockTableCache();
        this.writeDelegate = FSDirectory.open(path);
    }

    public void setBlockTableOverride(RadixBlockTable<MemorySegment> blockTableOverride) {
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
        String absolutePath = file.toAbsolutePath().toString();
        RadixBlockTable<MemorySegment> table = (blockTableOverride != null)
                ? blockTableOverride : radixBlockTableCache.acquire(absolutePath);
        if (blockTableOverride != null) {
            radixBlockTableCache.acquire(absolutePath); // track refCount even with override
        }
        return CachedMemorySegmentIndexInputV0_5.newInstance(
                "CachedMemorySegmentIndexInputV0_5(path=\"" + file + "\")",
                file.toAbsolutePath(), fileSize, absolutePath, blockCache, table);
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
        try { writeDelegate.close(); } finally { super.close(); }
        blockCache.close();
    }
}
