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
import org.apache.lucene.util.GroupVIntUtil;
import org.opensearch.index.store.bufferpoolfs.RadixBlockTable;

/**
 * V0.5 IndexInput: V0's MemorySegment-based read path + V2's infrastructure.
 *
 * <p>Purpose: isolate the JIT-friendliness difference between V0's read path
 * (MemorySegment.get with bounds checks, IOOBE-driven block switching) and
 * V2's read path (DirectByteBuffer with currentBlockEnd guard).
 *
 * <p>Same as V0:
 * <ul>
 *   <li>{@code MemorySegment.get(LAYOUT_BYTE, offset)} with MemorySegment bounds checks</li>
 *   <li>IOOBE-driven block switching via try/catch IndexOutOfBoundsException</li>
 *   <li>{@code getCacheBlockWithOffset()} pattern returning MemorySegment + lastOffsetInBlock</li>
 * </ul>
 *
 * <p>Same as V2:
 * <ul>
 *   <li>{@link RadixBlockTable} for L1 cache (no BlockSlotTinyCache, no CAS, no ref counting)</li>
 *   <li>{@link CaffeineBlockCacheV2} for L2 cache (no BlockCacheValue pinning)</li>
 *   <li>GC-based lifecycle (no RefCountedMemorySegment)</li>
 * </ul>
 */
@SuppressWarnings("preview")
public class CachedMemorySegmentIndexInputV0_5 extends IndexInput implements RandomAccessInput {
    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    static final ValueLayout.OfShort LAYOUT_LE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final long length;
    final String filePath;
    final CaffeineBlockCacheV2 blockCache;
    final RadixBlockTable<MemorySegment> blockTable;
    final long absoluteBaseOffset;
    final boolean isClone;

    long curPosition = 0L;
    volatile boolean isOpen = true;

    // V0-style: cached segment from last getCacheBlockWithOffset call
    private long currentBlockOffset = -1;
    private MemorySegment currentSegment = null;
    private int lastOffsetInBlock;

    public static CachedMemorySegmentIndexInputV0_5 newInstance(
            String resourceDescription, Path path, long length,
            String filePath, CaffeineBlockCacheV2 blockCache,
            RadixBlockTable<MemorySegment> blockTable) {
        CachedMemorySegmentIndexInputV0_5 input = new CachedMemorySegmentIndexInputV0_5(
                resourceDescription, 0L, length, filePath, blockCache, false, blockTable);
        try { input.seek(0L); } catch (IOException e) { throw new AssertionError(e); }
        return input;
    }

    private CachedMemorySegmentIndexInputV0_5(
            String resourceDescription, long absoluteBaseOffset, long length,
            String filePath, CaffeineBlockCacheV2 blockCache, boolean isClone,
            RadixBlockTable<MemorySegment> blockTable) {
        super(resourceDescription);
        this.absoluteBaseOffset = absoluteBaseOffset;
        this.length = length;
        this.filePath = filePath;
        this.blockCache = blockCache;
        this.isClone = isClone;
        this.blockTable = blockTable;
    }

    void ensureOpen() {
        if (!isOpen) throw new AlreadyClosedException("Already closed: " + this);
    }

    RuntimeException handlePositionalIOOBE(RuntimeException unused, String action, long pos) throws IOException {
        if (pos < 0L) {
            return new IllegalArgumentException(action + " negative position (pos=" + pos + "): " + this);
        } else {
            throw new EOFException(action + " past EOF (pos=" + pos + "): " + this);
        }
    }

    AlreadyClosedException alreadyClosed(RuntimeException unused) {
        return new AlreadyClosedException("Already closed: " + this);
    }

    // ======================== V0-style getCacheBlockWithOffset ========================

    /**
     * V0-style block lookup: returns MemorySegment, sets lastOffsetInBlock.
     * Uses RadixBlockTable (L1) + CaffeineBlockCacheV2 (L2) instead of
     * BlockSlotTinyCache + CaffeineBlockCache.
     * No ref counting, no pin/unpin.
     */
    private MemorySegment getCacheBlockWithOffset(long pos) throws IOException {
        final long fileOffset = absoluteBaseOffset + pos;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        final int offsetInBlock = (int) (fileOffset - blockOffset);

        // Fast path: reuse current segment if still valid
        if (blockOffset == currentBlockOffset && currentSegment != null) {
            lastOffsetInBlock = offsetInBlock;
            return currentSegment;
        }

        final long blockId = fileOffset >>> CACHE_BLOCK_SIZE_POWER;

        // L1: RadixBlockTable lookup (two plain array loads, no CAS, no fences)
        MemorySegment seg = blockTable.get(blockId);
        if (seg != null) {
            currentBlockOffset = blockOffset;
            currentSegment = seg;
            lastOffsetInBlock = offsetInBlock;
            return seg;
        }

        // L2: CaffeineBlockCacheV2 (load from disk if needed)
        BlockCacheKeyV2 key = new BlockCacheKeyV2(filePath, blockId);
        ByteBuffer buf = blockCache.getOrLoad(key);
        seg = MemorySegment.ofBuffer(buf);

        // Reinterpret to constant size for JIT bounds-check elimination on full blocks
        final long fileEnd = absoluteBaseOffset + length;
        if (blockOffset + CACHE_BLOCK_SIZE <= fileEnd) {
            seg = seg.reinterpret(CACHE_BLOCK_SIZE);
        }

        blockTable.put(blockId, seg);
        currentBlockOffset = blockOffset;
        currentSegment = seg;
        lastOffsetInBlock = offsetInBlock;
        return seg;
    }

    // ======================== V0-style read path (IOOBE-driven) ========================

    @Override
    public final byte readByte() throws IOException {
        final long currentPos = curPosition;
        try {
            final MemorySegment segment = getCacheBlockWithOffset(currentPos);
            final byte v = segment.get(LAYOUT_BYTE, lastOffsetInBlock);
            curPosition = currentPos + 1;
            return v;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", currentPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final void readBytes(byte[] b, int offset, int len) throws IOException {
        if (len == 0) return;
        final long startPos = curPosition;
        int remaining = len;
        int bufferOffset = offset;
        long currentPos = startPos;

        try {
            while (remaining > 0) {
                final MemorySegment seg = getCacheBlockWithOffset(currentPos);
                final int offInBlock = lastOffsetInBlock;
                final int avail = (int) (seg.byteSize() - offInBlock);

                if (offInBlock == 0 && remaining >= CACHE_BLOCK_SIZE && seg.byteSize() >= CACHE_BLOCK_SIZE) {
                    MemorySegment.copy(seg, LAYOUT_BYTE, 0L, b, bufferOffset, CACHE_BLOCK_SIZE);
                    remaining -= CACHE_BLOCK_SIZE;
                    bufferOffset += CACHE_BLOCK_SIZE;
                    currentPos += CACHE_BLOCK_SIZE;
                    continue;
                }

                final int toRead = Math.min(remaining, avail);
                MemorySegment.copy(seg, LAYOUT_BYTE, offInBlock, b, bufferOffset, toRead);
                remaining -= toRead;
                bufferOffset += toRead;
                currentPos += toRead;
            }
            curPosition = startPos + len;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        if (length == 0) return;
        final long startPos = getFilePointer();
        final long totalBytes = Integer.BYTES * (long) length;
        try {
            final MemorySegment segment = getCacheBlockWithOffset(startPos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + totalBytes <= segment.byteSize()) {
                MemorySegment.copy(segment, LAYOUT_LE_INT, offsetInBlock, dst, offset, length);
                curPosition += totalBytes;
            } else {
                super.readInts(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        if (length == 0) return;
        final long startPos = getFilePointer();
        final long totalBytes = Long.BYTES * (long) length;
        try {
            final MemorySegment segment = getCacheBlockWithOffset(startPos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + totalBytes <= segment.byteSize()) {
                MemorySegment.copy(segment, LAYOUT_LE_LONG, offsetInBlock, dst, offset, length);
                curPosition += totalBytes;
            } else {
                super.readLongs(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readFloats(float[] dst, int offset, int length) throws IOException {
        if (length == 0) return;
        final long startPos = getFilePointer();
        final long totalBytes = Float.BYTES * (long) length;
        try {
            final MemorySegment segment = getCacheBlockWithOffset(startPos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + totalBytes <= segment.byteSize()) {
                MemorySegment.copy(segment, LAYOUT_LE_FLOAT, offsetInBlock, dst, offset, length);
                curPosition += totalBytes;
            } else {
                super.readFloats(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final short readShort() throws IOException {
        final long currentPos = curPosition;
        try {
            final MemorySegment segment = getCacheBlockWithOffset(currentPos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + Short.BYTES > segment.byteSize()) {
                return super.readShort();
            }
            final short v = segment.get(LAYOUT_LE_SHORT, offsetInBlock);
            curPosition = currentPos + Short.BYTES;
            return v;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", currentPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final int readInt() throws IOException {
        final long currentPos = curPosition;
        try {
            final MemorySegment segment = getCacheBlockWithOffset(currentPos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock <= segment.byteSize() - Integer.BYTES) {
                final int v = segment.get(LAYOUT_LE_INT, offsetInBlock);
                curPosition = currentPos + Integer.BYTES;
                return v;
            }
            return super.readInt();
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", currentPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final long readLong() throws IOException {
        final long currentPos = curPosition;
        try {
            final MemorySegment segment = getCacheBlockWithOffset(currentPos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock <= segment.byteSize() - Long.BYTES) {
                final long v = segment.get(LAYOUT_LE_LONG, offsetInBlock);
                curPosition = currentPos + Long.BYTES;
                return v;
            }
            return super.readLong();
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", currentPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public void readGroupVInt(int[] dst, int offset) throws IOException {
        try {
            final MemorySegment segment = getCacheBlockWithOffset(curPosition);
            final int offsetInBlock = lastOffsetInBlock;
            final int len = GroupVIntUtil.readGroupVInt(
                    this,
                    segment.byteSize() - offsetInBlock,
                    p -> segment.get(LAYOUT_LE_INT, p),
                    offsetInBlock,
                    dst,
                    offset
            );
            curPosition += len;
        } catch (IllegalStateException | NullPointerException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final int readVInt() throws IOException {
        return super.readVInt();
    }

    @Override
    public final long readVLong() throws IOException {
        return super.readVLong();
    }

    // ======================== Positional reads (RandomAccessInput) ========================

    @Override
    public byte readByte(long pos) throws IOException {
        if (pos < 0 || pos >= length) return 0;
        try {
            final MemorySegment segment = getCacheBlockWithOffset(pos);
            return segment.get(LAYOUT_BYTE, lastOffsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        try {
            final MemorySegment segment = getCacheBlockWithOffset(pos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + Short.BYTES > segment.byteSize()) {
                long savedPos = getFilePointer();
                try { seek(pos); return readShort(); } finally { seek(savedPos); }
            }
            return segment.get(LAYOUT_LE_SHORT, offsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        try {
            final MemorySegment segment = getCacheBlockWithOffset(pos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + Integer.BYTES > segment.byteSize()) {
                long savedPos = getFilePointer();
                try { seek(pos); return readInt(); } finally { seek(savedPos); }
            }
            return segment.get(LAYOUT_LE_INT, offsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        try {
            final MemorySegment segment = getCacheBlockWithOffset(pos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + Long.BYTES > segment.byteSize()) {
                long savedPos = getFilePointer();
                try { seek(pos); return readLong(); } finally { seek(savedPos); }
            }
            return segment.get(LAYOUT_LE_LONG, offsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    // ======================== Navigation ========================

    @Override
    public long getFilePointer() { ensureOpen(); return curPosition; }

    @Override
    public final long length() { return length; }

    @Override
    public void seek(long pos) throws IOException {
        ensureOpen();
        if (pos < 0 || pos > length) {
            throw handlePositionalIOOBE(null, "seek", pos);
        }
        this.curPosition = pos;
    }

    // ======================== Clone / Slice ========================

    @Override
    public final CachedMemorySegmentIndexInputV0_5 clone() {
        final CachedMemorySegmentIndexInputV0_5 clone = buildSlice(null, 0L, this.length);
        try { clone.seek(getFilePointer()); } catch (IOException ioe) { throw new AssertionError(ioe); }
        return clone;
    }

    @Override
    public final CachedMemorySegmentIndexInputV0_5 slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length) {
            throw new IllegalArgumentException(
                    "slice() " + sliceDescription + " out of bounds: offset=" + offset
                            + ",length=" + length + ",fileLength=" + this.length + ": " + this);
        }
        var slice = buildSlice(sliceDescription, offset, length);
        slice.seek(0L);
        return slice;
    }

    CachedMemorySegmentIndexInputV0_5 buildSlice(String sliceDescription, long sliceOffset, long length) {
        ensureOpen();
        final long sliceAbsoluteBaseOffset = this.absoluteBaseOffset + sliceOffset;
        final String newResourceDescription = getFullSliceDescription(sliceDescription);
        CachedMemorySegmentIndexInputV0_5 slice = new CachedMemorySegmentIndexInputV0_5(
                newResourceDescription, sliceAbsoluteBaseOffset, length,
                filePath, blockCache, true, blockTable);
        try { slice.seek(0L); } catch (IOException ioe) { throw new AssertionError(ioe); }
        return slice;
    }

    // ======================== Close ========================

    @Override
    public final void close() throws IOException {
        if (!isOpen) return;
        isOpen = false;
        currentSegment = null;
        currentBlockOffset = -1L;
        // No ref counting to clean up — GC handles everything
    }
}
