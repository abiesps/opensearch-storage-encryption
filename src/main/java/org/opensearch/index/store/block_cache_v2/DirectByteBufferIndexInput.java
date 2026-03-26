/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.store.bufferpoolfs.RadixBlockTable;

/**
 * POC IndexInput backed by direct ByteBuffers + Caffeine + RadixBlockTable.
 *
 * No encryption, no MemorySegmentPool, no RefCountedMemorySegment, no pin/unpin.
 * ByteBuffer.allocateDirect + GC reclamation. MemorySegment.ofBuffer() for hot-path reads.
 *
 * Uses Lucene-style single-counter pattern: only {@code curPosition} is tracked.
 * Block offset is computed as {@code curPosition - currentSegmentOffset}.
 * Seek is lazy (just sets curPosition). Block resolution happens on next read via IOOBE catch.
 *
 * Designed for benchmarking against MMapDirectory.
 */
@SuppressWarnings("preview")
public class DirectByteBufferIndexInput extends IndexInput implements RandomAccessInput {

    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    static final ValueLayout.OfShort LAYOUT_LE_SHORT =
            ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfInt LAYOUT_LE_INT =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfLong LAYOUT_LE_LONG =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT =
            ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final long length;
    final String filePath;
    final CaffeineBlockCacheV2 blockCache;
    final RadixBlockTableCache tableCache;
    final long absoluteBaseOffset;
    final boolean isClone;
    final RadixBlockTable<MemorySegment> blockTable;

    long curPosition = 0L;
    boolean isOpen = true;

    // Hot path fields — single-counter pattern like Lucene's MultiSegmentImpl
    private MemorySegment currentSegment = null;
    private long currentSegmentOffset = 0L; // absolute file offset where currentSegment starts

    /**
     * The curPosition value at which the current block is exhausted.
     * When curPosition >= currentBlockEnd, we must switch blocks.
     * Initialized to 0 so the first read triggers a block load.
     */
    private long currentBlockEnd = 0L;

    /**
     * Creates a new parent instance with a caller-supplied RadixBlockTable
     * (obtained from {@link RadixBlockTableCache#acquire}).
     */
    public static DirectByteBufferIndexInput newInstance(
            String resourceDescription, Path path, long length,
            String filePath, CaffeineBlockCacheV2 blockCache,
            RadixBlockTableCache tableCache,
            RadixBlockTable<MemorySegment> blockTable) {
        DirectByteBufferIndexInput input = new DirectByteBufferIndexInput(
                resourceDescription, 0L, length, filePath, blockCache,
                tableCache, false, blockTable);
        try { input.seek(0L); } catch (IOException e) { throw new AssertionError(e); }
        return input;
    }

    private DirectByteBufferIndexInput(
            String resourceDescription, long absoluteBaseOffset, long length,
            String filePath, CaffeineBlockCacheV2 blockCache,
            RadixBlockTableCache tableCache, boolean isClone,
            RadixBlockTable<MemorySegment> blockTable) {
        super(resourceDescription);
        this.absoluteBaseOffset = absoluteBaseOffset;
        this.length = length;
        this.filePath = filePath;
        this.blockCache = blockCache;
        this.tableCache = tableCache;
        this.isClone = isClone;
        this.blockTable = blockTable;
    }

    void ensureOpen() {
        if (!isOpen) throw new AlreadyClosedException("Already closed: " + this);
    }

    // ======================== Block loading ========================

    /**
     * Resolves the block for the given logical position and switches currentSegment.
     * Called on the slow path when IOOBE indicates we've crossed a block boundary.
     */
    private void switchToBlock(long pos) throws IOException {
        final long fileOffset = absoluteBaseOffset + pos;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        final long blockId = fileOffset >>> CACHE_BLOCK_SIZE_POWER;

        // Check SparseLongBlockTable (L1)
        MemorySegment seg = blockTable.get(blockId);
        if (seg != null) {
            currentSegment = seg;
            currentSegmentOffset = blockOffset;
            long blockEndFileOffset = blockOffset + seg.byteSize();
            currentBlockEnd = Math.min(blockEndFileOffset - absoluteBaseOffset, length);
            return;
        }

        // L1 miss → load from Caffeine (L2)
        loadFromCaffeine(blockId, blockOffset);
    }

    private void loadFromCaffeine(long blockId, long blockOffset) throws IOException {
        BlockCacheKeyV2 key = new BlockCacheKeyV2(filePath, blockId);
        ByteBuffer buf = blockCache.getOrLoad(key);

        // Convert ByteBuffer → MemorySegment for hot-path layout accessors
        MemorySegment seg = MemorySegment.ofBuffer(buf);

        // Reinterpret to constant CACHE_BLOCK_SIZE for JIT bounds-check elimination
        final long fileEnd = absoluteBaseOffset + length;
        if (blockOffset + CACHE_BLOCK_SIZE <= fileEnd) {
            seg = seg.reinterpret(CACHE_BLOCK_SIZE);
        }

        blockTable.put(blockId, seg);
        currentSegment = seg;
        currentSegmentOffset = blockOffset;
        long blockEndFileOffset = blockOffset + seg.byteSize();
        currentBlockEnd = Math.min(blockEndFileOffset - absoluteBaseOffset, length);
    }

    // ======================== Sequential scalar reads ========================

    /**
     * readByte() — fast path with currentBlockEnd guard + eager seek.
     */
    @Override
    public final byte readByte() throws IOException {
        final long pos = curPosition;
        if (pos < currentBlockEnd) {
            final long off = absoluteBaseOffset + pos - currentSegmentOffset;
            final byte v = currentSegment.get(LAYOUT_BYTE, off);
            curPosition = pos + 1;
            return v;
        }
        return readByteSlow();
    }

    private byte readByteSlow() throws IOException {
        if (curPosition >= length) throw new EOFException("read past EOF: " + this);
        switchToBlock(curPosition);
        final long off = absoluteBaseOffset + curPosition - currentSegmentOffset;
        final byte v = currentSegment.get(LAYOUT_BYTE, off);
        curPosition++;
        return v;
    }

    @Override
    public final short readShort() throws IOException {
        final long pos = curPosition;
        if (pos + Short.BYTES <= currentBlockEnd) {
            final long off = absoluteBaseOffset + pos - currentSegmentOffset;
            final short v = currentSegment.get(LAYOUT_LE_SHORT, off);
            curPosition = pos + Short.BYTES;
            return v;
        }
        return readShortSlow();
    }

    private short readShortSlow() throws IOException {
        if (curPosition >= length) throw new EOFException("read past EOF: " + this);
        final long fileOffset = absoluteBaseOffset + curPosition;
        final int offInBlock = (int)(fileOffset & CACHE_BLOCK_MASK);
        if (offInBlock + Short.BYTES > CACHE_BLOCK_SIZE) return super.readShort();
        switchToBlock(curPosition);
        final long off = fileOffset - currentSegmentOffset;
        if (off + Short.BYTES > currentSegment.byteSize()) return super.readShort();
        final short v = currentSegment.get(LAYOUT_LE_SHORT, off);
        curPosition += Short.BYTES;
        return v;
    }

    @Override
    public final int readInt() throws IOException {
        final long pos = curPosition;
        if (pos + Integer.BYTES <= currentBlockEnd) {
            final long off = absoluteBaseOffset + pos - currentSegmentOffset;
            final int v = currentSegment.get(LAYOUT_LE_INT, off);
            curPosition = pos + Integer.BYTES;
            return v;
        }
        return readIntSlow();
    }

    private int readIntSlow() throws IOException {
        if (curPosition >= length) throw new EOFException("read past EOF: " + this);
        final long fileOffset = absoluteBaseOffset + curPosition;
        final int offInBlock = (int)(fileOffset & CACHE_BLOCK_MASK);
        if (offInBlock + Integer.BYTES > CACHE_BLOCK_SIZE) return super.readInt();
        switchToBlock(curPosition);
        final long off = fileOffset - currentSegmentOffset;
        if (off + Integer.BYTES > currentSegment.byteSize()) return super.readInt();
        final int v = currentSegment.get(LAYOUT_LE_INT, off);
        curPosition += Integer.BYTES;
        return v;
    }

    @Override
    public final long readLong() throws IOException {
        final long pos = curPosition;
        if (pos + Long.BYTES <= currentBlockEnd) {
            final long off = absoluteBaseOffset + pos - currentSegmentOffset;
            final long v = currentSegment.get(LAYOUT_LE_LONG, off);
            curPosition = pos + Long.BYTES;
            return v;
        }
        return readLongSlow();
    }

    private long readLongSlow() throws IOException {
        if (curPosition >= length) throw new EOFException("read past EOF: " + this);
        final long fileOffset = absoluteBaseOffset + curPosition;
        final int offInBlock = (int)(fileOffset & CACHE_BLOCK_MASK);
        if (offInBlock + Long.BYTES > CACHE_BLOCK_SIZE) return super.readLong();
        switchToBlock(curPosition);
        final long off = fileOffset - currentSegmentOffset;
        if (off + Long.BYTES > currentSegment.byteSize()) return super.readLong();
        final long v = currentSegment.get(LAYOUT_LE_LONG, off);
        curPosition += Long.BYTES;
        return v;
    }

    @Override public final int readVInt() throws IOException { return super.readVInt(); }
    @Override public final long readVLong() throws IOException { return super.readVLong(); }

    // ======================== Bulk reads ========================

    @Override
    public final void readBytes(byte[] b, int offset, int len) throws IOException {
        if (len == 0) return;
        int remaining = len;
        int bufferOffset = offset;

        while (remaining > 0) {
            // Ensure we have a valid block
            if (curPosition >= currentBlockEnd || currentSegment == null) {
                if (curPosition >= length) throw new EOFException("read past EOF: " + this);
                switchToBlock(curPosition);
            }

            final long fileOffset = absoluteBaseOffset + curPosition;
            final int offInBlock = (int)(fileOffset - currentSegmentOffset);
            final int avail = (int)(currentBlockEnd - curPosition);

            if (offInBlock == 0 && remaining >= CACHE_BLOCK_SIZE
                    && currentSegment.byteSize() >= CACHE_BLOCK_SIZE) {
                MemorySegment.copy(currentSegment, LAYOUT_BYTE, 0L, b,
                        bufferOffset, CACHE_BLOCK_SIZE);
                remaining -= CACHE_BLOCK_SIZE;
                bufferOffset += CACHE_BLOCK_SIZE;
                curPosition += CACHE_BLOCK_SIZE;
                continue;
            }

            final int toRead = Math.min(remaining, avail);
            MemorySegment.copy(currentSegment, LAYOUT_BYTE, offInBlock, b,
                    bufferOffset, toRead);
            remaining -= toRead;
            bufferOffset += toRead;
            curPosition += toRead;
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        if (length == 0) return;
        final long totalBytes = Integer.BYTES * (long) length;
        try {
            if (currentSegment == null) {
                switchToBlock(curPosition);
            }
            long off = absoluteBaseOffset + curPosition - currentSegmentOffset;
            if (off < 0 || off + totalBytes > currentSegment.byteSize()) {
                switchToBlock(curPosition);
                off = absoluteBaseOffset + curPosition - currentSegmentOffset;
            }
            if (off + totalBytes <= currentSegment.byteSize()) {
                MemorySegment.copy(currentSegment, LAYOUT_LE_INT, off, dst, offset, length);
                curPosition += totalBytes;
            } else {
                super.readInts(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException e) {
            throw new EOFException("read past EOF: " + this);
        } catch (NullPointerException e) {
            if (!isOpen) throw new AlreadyClosedException("Already closed: " + this);
            throw new IOException("Unexpected null in readInts: " + this, e);
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        if (length == 0) return;
        final long totalBytes = Long.BYTES * (long) length;
        try {
            if (currentSegment == null) {
                switchToBlock(curPosition);
            }
            long off = absoluteBaseOffset + curPosition - currentSegmentOffset;
            if (off < 0 || off + totalBytes > currentSegment.byteSize()) {
                switchToBlock(curPosition);
                off = absoluteBaseOffset + curPosition - currentSegmentOffset;
            }
            if (off + totalBytes <= currentSegment.byteSize()) {
                MemorySegment.copy(currentSegment, LAYOUT_LE_LONG, off, dst, offset, length);
                curPosition += totalBytes;
            } else {
                super.readLongs(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException e) {
            throw new EOFException("read past EOF: " + this);
        } catch (NullPointerException e) {
            if (!isOpen) throw new AlreadyClosedException("Already closed: " + this);
            throw new IOException("Unexpected null in readLongs: " + this, e);
        }
    }

    @Override
    public void readFloats(float[] dst, int offset, int length) throws IOException {
        if (length == 0) return;
        final long totalBytes = Float.BYTES * (long) length;
        try {
            if (currentSegment == null) {
                switchToBlock(curPosition);
            }
            long off = absoluteBaseOffset + curPosition - currentSegmentOffset;
            if (off < 0 || off + totalBytes > currentSegment.byteSize()) {
                switchToBlock(curPosition);
                off = absoluteBaseOffset + curPosition - currentSegmentOffset;
            }
            if (off + totalBytes <= currentSegment.byteSize()) {
                MemorySegment.copy(currentSegment, LAYOUT_LE_FLOAT, off, dst, offset, length);
                curPosition += totalBytes;
            } else {
                super.readFloats(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException e) {
            throw new EOFException("read past EOF: " + this);
        } catch (NullPointerException e) {
            if (!isOpen) throw new AlreadyClosedException("Already closed: " + this);
            throw new IOException("Unexpected null in readFloats: " + this, e);
        }
    }

    // ======================== RandomAccessInput positional reads ========================

    @Override
    public byte readByte(long pos) throws IOException {
        if (pos < 0 || pos >= length) return 0;
        try {
            final long fileOffset = absoluteBaseOffset + pos;
            if (currentSegment != null) {
                long off = fileOffset - currentSegmentOffset;
                if (off >= 0 && off < currentSegment.byteSize()) {
                    return currentSegment.get(LAYOUT_BYTE, off);
                }
            }
            switchToBlock(pos);
            long off = fileOffset - currentSegmentOffset;
            return currentSegment.get(LAYOUT_BYTE, off);
        } catch (IndexOutOfBoundsException e) {
            throw new EOFException("read past EOF: " + this);
        } catch (NullPointerException e) {
            if (!isOpen) throw new AlreadyClosedException("Already closed: " + this);
            throw new IOException("Unexpected null in readByte(pos): " + this, e);
        }
    }

    @Override
    public void readBytes(long pos, byte[] b, int off, int len) throws IOException {
        if (len == 0) return;
        try {
            long currentPos = pos;
            int remaining = len;
            int bufferOffset = off;
            while (remaining > 0) {
                final long fileOffset = absoluteBaseOffset + currentPos;
                long segOff = fileOffset - currentSegmentOffset;
                if (segOff < 0 || segOff >= currentSegment.byteSize()) {
                    switchToBlock(currentPos);
                    segOff = fileOffset - currentSegmentOffset;
                }
                final int offInBlock = (int) segOff;
                final int avail = (int)(currentSegment.byteSize() - offInBlock);
                final int toRead = Math.min(remaining, avail);
                MemorySegment.copy(currentSegment, LAYOUT_BYTE, offInBlock, b,
                        bufferOffset, toRead);
                remaining -= toRead;
                bufferOffset += toRead;
                currentPos += toRead;
            }
        } catch (IndexOutOfBoundsException e) {
            throw new EOFException("read past EOF: " + this);
        } catch (NullPointerException e) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        try {
            final long fileOffset = absoluteBaseOffset + pos;
            if (currentSegment != null) {
                long off = fileOffset - currentSegmentOffset;
                if (off >= 0 && off + Short.BYTES <= currentSegment.byteSize()) {
                    return currentSegment.get(LAYOUT_LE_SHORT, off);
                }
            }
            // Check if it spans a block boundary
            final int offInBlock = (int)(fileOffset & CACHE_BLOCK_MASK);
            if (offInBlock + Short.BYTES > CACHE_BLOCK_SIZE) {
                long saved = curPosition;
                try { curPosition = pos; return readShort(); } finally { curPosition = saved; }
            }
            switchToBlock(pos);
            long off = fileOffset - currentSegmentOffset;
            return currentSegment.get(LAYOUT_LE_SHORT, off);
        } catch (IndexOutOfBoundsException e) {
            throw new EOFException("read past EOF: " + this);
        } catch (NullPointerException e) {
            if (!isOpen) throw new AlreadyClosedException("Already closed: " + this);
            throw new IOException("Unexpected null in readShort(pos): " + this, e);
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        try {
            final long fileOffset = absoluteBaseOffset + pos;
            if (currentSegment != null) {
                long off = fileOffset - currentSegmentOffset;
                if (off >= 0 && off + Integer.BYTES <= currentSegment.byteSize()) {
                    return currentSegment.get(LAYOUT_LE_INT, off);
                }
            }
            final int offInBlock = (int)(fileOffset & CACHE_BLOCK_MASK);
            if (offInBlock + Integer.BYTES > CACHE_BLOCK_SIZE) {
                long saved = curPosition;
                try { curPosition = pos; return readInt(); } finally { curPosition = saved; }
            }
            switchToBlock(pos);
            long off = fileOffset - currentSegmentOffset;
            return currentSegment.get(LAYOUT_LE_INT, off);
        } catch (IndexOutOfBoundsException e) {
            throw new EOFException("read past EOF: " + this);
        } catch (NullPointerException e) {
            if (!isOpen) throw new AlreadyClosedException("Already closed: " + this);
            throw new IOException("Unexpected null in readInt(pos): " + this, e);
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        try {
            final long fileOffset = absoluteBaseOffset + pos;
            if (currentSegment != null) {
                long off = fileOffset - currentSegmentOffset;
                if (off >= 0 && off + Long.BYTES <= currentSegment.byteSize()) {
                    return currentSegment.get(LAYOUT_LE_LONG, off);
                }
            }
            final int offInBlock = (int)(fileOffset & CACHE_BLOCK_MASK);
            if (offInBlock + Long.BYTES > CACHE_BLOCK_SIZE) {
                long saved = curPosition;
                try { curPosition = pos; return readLong(); } finally { curPosition = saved; }
            }
            switchToBlock(pos);
            long off = fileOffset - currentSegmentOffset;
            return currentSegment.get(LAYOUT_LE_LONG, off);
        } catch (IndexOutOfBoundsException e) {
            throw new EOFException("read past EOF: " + this);
        } catch (NullPointerException e) {
            if (!isOpen) throw new AlreadyClosedException("Already closed: " + this);
            throw new IOException("Unexpected null in readLong(pos): " + this, e);
        }
    }

    // ======================== Position / length ========================

    @Override public long getFilePointer() { ensureOpen(); return curPosition; }
    @Override public final long length() { return length; }

    /**
     * Seek — sets curPosition and eagerly resolves the block if it changed.
     * Eager resolution avoids a slow-path call on the next readByte, which
     * helps C2 profile the outer loop more accurately during warmup.
     */
    @Override
    public void seek(long pos) throws IOException {
        ensureOpen();
        if (pos < 0 || pos > length) {
            throw new EOFException("seek past EOF (pos=" + pos + "): " + this);
        }
        this.curPosition = pos;
        // Eagerly resolve block if outside current block bounds
        if (pos < length && (pos >= currentBlockEnd || currentSegment == null)) {
            switchToBlock(pos);
        }
    }

    // ======================== Clone / Slice ========================

    @Override
    public final DirectByteBufferIndexInput clone() {
        final DirectByteBufferIndexInput clone = buildSlice(null, 0L, this.length);
        try { clone.seek(getFilePointer()); } catch (IOException e) { throw new AssertionError(e); }
        return clone;
    }

    @Override
    public final DirectByteBufferIndexInput slice(String sliceDescription, long offset, long length)
            throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length) {
            throw new IllegalArgumentException("slice() " + sliceDescription
                    + " out of bounds: offset=" + offset + ",length=" + length
                    + ",fileLength=" + this.length + ": " + this);
        }
        var slice = buildSlice(sliceDescription, offset, length);
        slice.seek(0L);
        return slice;
    }

    DirectByteBufferIndexInput buildSlice(String sliceDescription, long sliceOffset, long length) {
        ensureOpen();
        final long sliceAbsoluteBase = this.absoluteBaseOffset + sliceOffset;
        final String desc = getFullSliceDescription(sliceDescription);
        // Clone/slice shares same blockTable reference — no cache interaction (Req 3.1, 3.2, 3.3)
        DirectByteBufferIndexInput slice = new DirectByteBufferIndexInput(
                desc, sliceAbsoluteBase, length, filePath, blockCache,
                tableCache, true, blockTable);
        try { slice.seek(0L); } catch (IOException e) { throw new AssertionError(e); }
        return slice;
    }

    // ======================== Close ========================

    @Override
    public final void close() throws IOException {
        if (!isOpen) return;
        isOpen = false;
        currentSegment = null;

        if (!isClone) {
            // Parent close: release the shared table via RadixBlockTableCache (Req 4.1)
            // This decrements refCount; if zero, clears table and removes entry
            tableCache.release(filePath);
        }
        // Clone/slice close: no cache interaction (Req 4.2)
    }
}
