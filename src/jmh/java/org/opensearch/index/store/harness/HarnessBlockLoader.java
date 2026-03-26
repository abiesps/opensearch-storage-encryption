/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.harness;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache_v2.DirectMemoryAllocator;
import org.opensearch.index.store.block_cache_v2.MemoryBackPressureException;
import org.opensearch.index.store.block_loader.BlockLoader;

/**
 * A {@link BlockLoader} that allocates via {@link DirectMemoryAllocator} without
 * performing any file I/O or encryption. Each loaded block is a fresh direct
 * {@link ByteBuffer} wrapped in a {@link RefCountedMemorySegment} with a no-op
 * releaser — identical to the production {@code DirectByteBufferBlockLoader} path.
 */
public final class HarnessBlockLoader implements BlockLoader<RefCountedMemorySegment> {

    private final DirectMemoryAllocator allocator;
    private final int blockSize;

    public HarnessBlockLoader(DirectMemoryAllocator allocator, int blockSize) {
        this.allocator = allocator;
        this.blockSize = blockSize;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RefCountedMemorySegment[] load(Path filePath, long startOffset, long blockCount, long poolTimeoutMs) throws Exception {
        RefCountedMemorySegment[] result = new RefCountedMemorySegment[(int) blockCount];
        try {
            for (int i = 0; i < blockCount; i++) {
                ByteBuffer buf = allocator.allocate(blockSize);
                buf.put(0, (byte) 42); // touch to fault the page
                MemorySegment seg = MemorySegment.ofBuffer(buf);
                result[i] = new RefCountedMemorySegment(seg, blockSize, rcms -> {});
            }
        } catch (MemoryBackPressureException e) {
            // Clean up already-allocated blocks on failure
            for (int i = 0; i < result.length; i++) {
                if (result[i] != null) {
                    result[i].close();
                    result[i] = null;
                }
            }
            throw new BlockLoadFailedException("Back-pressure during harness load", e);
        }
        return result;
    }
}
