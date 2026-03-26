/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

/**
 * Lucene {@link IndexOutput} implementation that writes data to disk via a
 * dedicated write-mode {@link FileChannel}. The write path is intentionally
 * simple — write performance optimization is deferred.
 *
 * <p>Key design decisions:
 * <ul>
 *   <li>The write FileChannel is completely separate from the read-path
 *       Caffeine FD cache. No FD_Open_Limiter permit, no RefCountedChannel,
 *       no VFD ref count.</li>
 *   <li>Writes bypass L1 (RadixBlockTable) and L2 (BlockCache) caches
 *       entirely.</li>
 *   <li>CRC32 checksum is maintained over all written bytes.</li>
 *   <li>{@code force(true)} on close ensures durability.</li>
 * </ul>
 *
 * <p><b>Satisfies: Requirement 52</b></p>
 *
 * @opensearch.internal
 */
public class MemorySegmentPoolIndexOutput extends IndexOutput {

    private final FileChannel writeChannel;
    private final ByteBuffer writeBuffer;
    private final CRC32 checksum;
    private long filePointer;
    private boolean closed;

    /**
     * Creates a new MemorySegmentPoolIndexOutput.
     *
     * @param resourceDescription Lucene resource description for debugging
     * @param name                the file name (used by Lucene for identification)
     * @param filePath            the path to write to
     * @throws IOException if the file cannot be opened for writing
     */
    public MemorySegmentPoolIndexOutput(String resourceDescription, String name,
                                         Path filePath) throws IOException {
        super(resourceDescription, name);
        this.writeChannel = FileChannel.open(filePath,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING);
        this.writeBuffer = ByteBuffer.allocateDirect(StaticConfigs.CACHE_BLOCK_SIZE);
        this.checksum = new CRC32();
        this.filePointer = 0;
        this.closed = false;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        assert !closed : "IndexOutput already closed";
        if (!writeBuffer.hasRemaining()) {
            flushBuffer();
        }
        writeBuffer.put(b);
        checksum.update(b);
        filePointer++;
    }

    @Override
    public void writeBytes(byte[] src, int offset, int length) throws IOException {
        assert !closed : "IndexOutput already closed";
        checksum.update(src, offset, length);
        int remaining = length;
        int srcOff = offset;
        while (remaining > 0) {
            if (!writeBuffer.hasRemaining()) {
                flushBuffer();
            }
            int chunk = Math.min(remaining, writeBuffer.remaining());
            writeBuffer.put(src, srcOff, chunk);
            srcOff += chunk;
            remaining -= chunk;
        }
        filePointer += length;
    }

    /**
     * Flush the write buffer to the FileChannel.
     * flip → write to channel → clear.
     */
    private void flushBuffer() throws IOException {
        writeBuffer.flip();
        while (writeBuffer.hasRemaining()) {
            writeChannel.write(writeBuffer);
        }
        writeBuffer.clear();
    }

    @Override
    public long getFilePointer() {
        return filePointer;
    }

    @Override
    public long getChecksum() {
        return checksum.getValue();
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            flushBuffer();
            writeChannel.force(true);
        } finally {
            writeChannel.close();
        }
    }
}
