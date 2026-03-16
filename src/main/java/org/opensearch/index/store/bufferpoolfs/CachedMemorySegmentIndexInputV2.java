/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.file.Path;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;

import sun.misc.Unsafe;

/**
 * V2 IndexInput using SparseLongBlockTable for per-file block lookup.
 *
 * <p>Key differences from V1 (CachedMemorySegmentIndexInput):
 * <ul>
 *   <li>Replaces BlockSlotTinyCache (32-slot direct-mapped) with SparseLongBlockTable
 *       (256-entry chunks, two-level sparse array) for O(1) block lookup by block ID</li>
 *   <li>Lucene-style exception-driven hot path: try currentSegment → catch IOOBE
 *       (advance block) → catch NPE (load from table then Caffeine)</li>
 *   <li>Clones/slices share parent's SparseLongBlockTable — no per-clone cache</li>
 * </ul>
 *
 * <p>POC simplifications (to be addressed in full implementation):
 * <ul>
 *   <li>No VirtualFileDescriptorRegistry — still uses Path-based cache keys</li>
 *   <li>No Cleaner registration for clones/slices</li>
 *   <li>No admission control</li>
 *   <li>Still uses existing MemorySegmentPool via BlockLoader (not ByteBuffer.allocateDirect)</li>
 * </ul>
 */
@SuppressWarnings("preview")
public class CachedMemorySegmentIndexInputV2 extends IndexInput implements RandomAccessInput {

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
    final Path path;
    final BlockCache<RefCountedMemorySegment> blockCache;
    final ReadaheadManager readaheadManager;
    final ReadaheadContext readaheadContext;
    final long absoluteBaseOffset;
    final boolean isClone;

    // Shared across parent + all clones/slices for this file
    final SparseLongBlockTable blockTable;
    final Path normalizedPath;
    final String normalizedPathString;

    long curPosition = 0L;
    volatile boolean isOpen = true;

    // Current block state — hot path fields
    private long currentBlockId = -1;
    private BlockCacheValue<RefCountedMemorySegment> currentBlockValue = null;
    private MemorySegment currentSegment = null;
    private int currentOffsetInBlock = CACHE_BLOCK_SIZE; // exhausted → first read triggers load

    private long currentSegmentAddress = 0L; // raw native address for Unsafe fast path

    // Scratch for getCacheBlockWithOffset
    private int lastOffsetInBlock;

    // Unsafe for bypassing MemorySegment bounds checks on the hot path.
    // Safe because IndexInput instances are single-threaded per Lucene contract.
    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }


    /**
     * Creates a new parent CachedMemorySegmentIndexInputV2.
     */
    public static CachedMemorySegmentIndexInputV2 newInstance(
            String resourceDescription,
            Path path,
            long length,
            BlockCache<RefCountedMemorySegment> blockCache,
            ReadaheadManager readaheadManager,
            ReadaheadContext readaheadContext) {

        // Estimate initial chunks: file blocks / 256 entries per chunk + 1
        long totalBlocks = (length + CACHE_BLOCK_SIZE - 1) >>> CACHE_BLOCK_SIZE_POWER;
        int initialChunks = (int)((totalBlocks >>> 8) + 1);

        SparseLongBlockTable table = new SparseLongBlockTable(initialChunks);
        return newInstance(resourceDescription, path, length, blockCache, readaheadManager, readaheadContext, table);
    }

    /**
     * Creates a new instance with a caller-supplied SparseLongBlockTable.
     * Useful for benchmarking with a no-op table to measure L1 cache overhead.
     */
    public static CachedMemorySegmentIndexInputV2 newInstance(
            String resourceDescription,
            Path path,
            long length,
            BlockCache<RefCountedMemorySegment> blockCache,
            ReadaheadManager readaheadManager,
            ReadaheadContext readaheadContext,
            SparseLongBlockTable blockTable) {

        Path normalized = path.toAbsolutePath().normalize();

        CachedMemorySegmentIndexInputV2 input = new CachedMemorySegmentIndexInputV2(
                resourceDescription, path, normalized, normalized.toString(),
                0L, length, blockCache, readaheadManager, readaheadContext,
                false, blockTable);
        try {
            input.seek(0L);
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
        return input;
    }

    private CachedMemorySegmentIndexInputV2(
            String resourceDescription,
            Path path,
            Path normalizedPath,
            String normalizedPathString,
            long absoluteBaseOffset,
            long length,
            BlockCache<RefCountedMemorySegment> blockCache,
            ReadaheadManager readaheadManager,
            ReadaheadContext readaheadContext,
            boolean isClone,
            SparseLongBlockTable blockTable) {
        super(resourceDescription);
        this.path = path;
        this.normalizedPath = normalizedPath;
        this.normalizedPathString = normalizedPathString;
        this.absoluteBaseOffset = absoluteBaseOffset;
        this.length = length;
        this.blockCache = blockCache;
        this.readaheadManager = readaheadManager;
        this.readaheadContext = readaheadContext;
        this.isClone = isClone;
        this.blockTable = blockTable;
    }

    void ensureOpen() {
        if (!isOpen) {
            throw alreadyClosed(null);
        }
    }

    RuntimeException handlePositionalIOOBE(String action, long pos) throws IOException {
        if (pos < 0L) {
            return new IllegalArgumentException(action + " negative position (pos=" + pos + "): " + this);
        } else {
            throw new EOFException(action + " past EOF (pos=" + pos + "): " + this);
        }
    }

    AlreadyClosedException alreadyClosed(RuntimeException e) {
        return new AlreadyClosedException("Already closed: " + this);
    }

    // ======================== Block loading ========================

    /**
     * Loads the block containing the given position (relative to this input).
     * First checks SparseLongBlockTable, then falls back to Caffeine.
     * Updates currentSegment, currentBlockId, currentOffsetInBlock.
     */
    private MemorySegment getCacheBlockWithOffset(long pos) throws IOException {
        final long fileOffset = absoluteBaseOffset + pos;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        final long blockId = fileOffset >>> CACHE_BLOCK_SIZE_POWER;
        final int offsetInBlock = (int)(fileOffset - blockOffset);

        // Fast path: same block as last access
        if (blockId == currentBlockId && currentSegment != null) {
            lastOffsetInBlock = offsetInBlock;
            currentOffsetInBlock = offsetInBlock;
            return currentSegment;
        }

        // Check SparseLongBlockTable first
        MemorySegment seg = blockTable.get(blockId);
        if (seg != null) {
            switchBlock(blockId, null, seg, offsetInBlock);
            return seg;
        }

        // Table miss → load from Caffeine (which loads from disk if needed)
        return loadFromCaffeine(blockId, blockOffset, offsetInBlock);
    }

    private MemorySegment loadFromCaffeine(long blockId, long blockOffset, int offsetInBlock) throws IOException {
        FileBlockCacheKey key = new FileBlockCacheKey(normalizedPath, normalizedPathString, blockOffset);
        BlockCacheValue<RefCountedMemorySegment> loaded = blockCache.getOrLoad(key);
        if (loaded == null) {
            throw new IOException("Failed to load block at offset " + blockOffset);
        }

        // Pin the loaded block
        if (!loaded.tryPin()) {
            throw new IOException("Failed to pin block at offset " + blockOffset);
        }

        RefCountedMemorySegment rcms = loaded.value();
        MemorySegment seg;

        // Reinterpret to constant CACHE_BLOCK_SIZE for JIT bounds-check elimination
        // (skip for last block which may be smaller)
        final long fileEnd = absoluteBaseOffset + length;
        if (blockOffset + CACHE_BLOCK_SIZE <= fileEnd) {
            seg = rcms.segment().reinterpret(CACHE_BLOCK_SIZE);
        } else {
            seg = rcms.segment();
        }

        // Populate table for future lookups
        blockTable.put(blockId, seg);

        switchBlock(blockId, loaded, seg, offsetInBlock);

        // Notify readahead
        if (readaheadContext != null) {
            readaheadContext.onAccess(blockOffset, false);
        }

        return seg;
    }

    private void switchBlock(long blockId, BlockCacheValue<RefCountedMemorySegment> newValue,
                             MemorySegment seg, int offsetInBlock) {
        // Unpin previous block if we had one pinned from Caffeine
        if (currentBlockValue != null) {
            currentBlockValue.unpin();
        }
        currentBlockId = blockId;
        currentBlockValue = newValue;
        currentSegment = seg;
        currentSegmentAddress = seg.address();
        lastOffsetInBlock = offsetInBlock;
        currentOffsetInBlock = offsetInBlock;
    }


    // ======================== Sequential scalar reads (Lucene-style hot path) ========================

    @Override
    public final byte readByte() throws IOException {
        final int off = currentOffsetInBlock;
        if (off < CACHE_BLOCK_SIZE && currentSegmentAddress != 0L) {
            final byte v = UNSAFE.getByte(currentSegmentAddress + off);
            currentOffsetInBlock = off + 1;
            curPosition++;
            return v;
        }
        return readByteSlow();
    }

    private byte readByteSlow() throws IOException {
        if (curPosition >= length) {
            throw new EOFException("read past EOF: " + this);
        }
        final MemorySegment seg = getCacheBlockWithOffset(curPosition);
        final byte v = seg.get(LAYOUT_BYTE, lastOffsetInBlock);
        currentOffsetInBlock = lastOffsetInBlock + 1;
        curPosition++;
        return v;
    }

    @Override
    public final short readShort() throws IOException {
        final int off = currentOffsetInBlock;
        if (off + Short.BYTES <= CACHE_BLOCK_SIZE && currentSegmentAddress != 0L) {
            final short v = UNSAFE.getShort(currentSegmentAddress + off);
            currentOffsetInBlock = off + Short.BYTES;
            curPosition += Short.BYTES;
            return v;
        }
        return readShortSlow();
    }

    private short readShortSlow() throws IOException {
        final long fileOffset = absoluteBaseOffset + curPosition;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        final int offInBlock = (int)(fileOffset - blockOffset);
        if (offInBlock + Short.BYTES > CACHE_BLOCK_SIZE) {
            return super.readShort();
        }
        final MemorySegment seg = getCacheBlockWithOffset(curPosition);
        final int ofs = lastOffsetInBlock;
        if (ofs + Short.BYTES > seg.byteSize()) {
            return super.readShort();
        }
        final short v = seg.get(LAYOUT_LE_SHORT, ofs);
        currentOffsetInBlock = ofs + Short.BYTES;
        curPosition += Short.BYTES;
        return v;
    }

    @Override
    public final int readInt() throws IOException {
        final int off = currentOffsetInBlock;
        if (off + Integer.BYTES <= CACHE_BLOCK_SIZE && currentSegmentAddress != 0L) {
            final int v = UNSAFE.getInt(currentSegmentAddress + off);
            currentOffsetInBlock = off + Integer.BYTES;
            curPosition += Integer.BYTES;
            return v;
        }
        return readIntSlow();
    }

    private int readIntSlow() throws IOException {
        final long fileOffset = absoluteBaseOffset + curPosition;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        final int offInBlock = (int)(fileOffset - blockOffset);
        if (offInBlock + Integer.BYTES > CACHE_BLOCK_SIZE) {
            return super.readInt();
        }
        final MemorySegment seg = getCacheBlockWithOffset(curPosition);
        final int ofs = lastOffsetInBlock;
        if (ofs + Integer.BYTES > seg.byteSize()) {
            return super.readInt();
        }
        final int v = seg.get(LAYOUT_LE_INT, ofs);
        currentOffsetInBlock = ofs + Integer.BYTES;
        curPosition += Integer.BYTES;
        return v;
    }

    @Override
    public final long readLong() throws IOException {
        final int off = currentOffsetInBlock;
        if (off + Long.BYTES <= CACHE_BLOCK_SIZE && currentSegmentAddress != 0L) {
            final long v = UNSAFE.getLong(currentSegmentAddress + off);
            currentOffsetInBlock = off + Long.BYTES;
            curPosition += Long.BYTES;
            return v;
        }
        return readLongSlow();
    }

    private long readLongSlow() throws IOException {
        final long fileOffset = absoluteBaseOffset + curPosition;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        final int offInBlock = (int)(fileOffset - blockOffset);
        if (offInBlock + Long.BYTES > CACHE_BLOCK_SIZE) {
            return super.readLong();
        }
        final MemorySegment seg = getCacheBlockWithOffset(curPosition);
        final int ofs = lastOffsetInBlock;
        if (ofs + Long.BYTES > seg.byteSize()) {
            return super.readLong();
        }
        final long v = seg.get(LAYOUT_LE_LONG, ofs);
        currentOffsetInBlock = ofs + Long.BYTES;
        curPosition += Long.BYTES;
        return v;
    }

    @Override
    public final int readVInt() throws IOException {
        return super.readVInt();
    }

    @Override
    public final long readVLong() throws IOException {
        return super.readVLong();
    }

    // ======================== Bulk reads ========================

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
                final int avail = (int)(seg.byteSize() - offInBlock);

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
            final long endFileOffset = absoluteBaseOffset + curPosition;
            final long endBlockOffset = endFileOffset & ~CACHE_BLOCK_MASK;
            if (endBlockOffset >>> CACHE_BLOCK_SIZE_POWER == currentBlockId && currentSegment != null) {
                currentOffsetInBlock = (int)(endFileOffset - endBlockOffset);
            } else {
                currentOffsetInBlock = CACHE_BLOCK_SIZE;
            }
        } catch (IndexOutOfBoundsException _) {
            throw handlePositionalIOOBE("read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        if (length == 0) return;
        final long startPos = curPosition;
        final long totalBytes = Integer.BYTES * (long) length;
        try {
            final MemorySegment seg = getCacheBlockWithOffset(startPos);
            final int ofs = lastOffsetInBlock;
            if (ofs + totalBytes <= seg.byteSize()) {
                MemorySegment.copy(seg, LAYOUT_LE_INT, ofs, dst, offset, length);
                curPosition += totalBytes;
                currentOffsetInBlock = ofs + (int) totalBytes;
            } else {
                super.readInts(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException _) {
            throw handlePositionalIOOBE("read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        if (length == 0) return;
        final long startPos = curPosition;
        final long totalBytes = Long.BYTES * (long) length;
        try {
            final MemorySegment seg = getCacheBlockWithOffset(startPos);
            final int ofs = lastOffsetInBlock;
            if (ofs + totalBytes <= seg.byteSize()) {
                MemorySegment.copy(seg, LAYOUT_LE_LONG, ofs, dst, offset, length);
                curPosition += totalBytes;
                currentOffsetInBlock = ofs + (int) totalBytes;
            } else {
                super.readLongs(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException _) {
            throw handlePositionalIOOBE("read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readFloats(float[] dst, int offset, int length) throws IOException {
        if (length == 0) return;
        final long startPos = curPosition;
        final long totalBytes = Float.BYTES * (long) length;
        try {
            final MemorySegment seg = getCacheBlockWithOffset(startPos);
            final int ofs = lastOffsetInBlock;
            if (ofs + totalBytes <= seg.byteSize()) {
                MemorySegment.copy(seg, LAYOUT_LE_FLOAT, ofs, dst, offset, length);
                curPosition += totalBytes;
                currentOffsetInBlock = ofs + (int) totalBytes;
            } else {
                super.readFloats(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException _) {
            throw handlePositionalIOOBE("read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    // ======================== RandomAccessInput positional reads ========================

    @Override
    public byte readByte(long pos) throws IOException {
        if (pos < 0 || pos >= length) return 0;
        try {
            final long fileOffset = absoluteBaseOffset + pos;
            final long blockId = fileOffset >>> CACHE_BLOCK_SIZE_POWER;
            final int offInBlock = (int)(fileOffset & CACHE_BLOCK_MASK);
            if (blockId == currentBlockId && currentSegment != null) {
                return currentSegment.get(LAYOUT_BYTE, offInBlock);
            }
            final MemorySegment seg = getCacheBlockWithOffset(pos);
            return seg.get(LAYOUT_BYTE, lastOffsetInBlock);
        } catch (IndexOutOfBoundsException _) {
            throw handlePositionalIOOBE("read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
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
                final long blockId = fileOffset >>> CACHE_BLOCK_SIZE_POWER;
                final int offInBlock = (int)(fileOffset & CACHE_BLOCK_MASK);

                MemorySegment seg;
                if (blockId == currentBlockId && currentSegment != null) {
                    seg = currentSegment;
                } else {
                    seg = getCacheBlockWithOffset(currentPos);
                }

                final int avail = (int)(seg.byteSize() - offInBlock);
                final int toRead = Math.min(remaining, avail);
                MemorySegment.copy(seg, LAYOUT_BYTE, offInBlock, b, bufferOffset, toRead);
                remaining -= toRead;
                bufferOffset += toRead;
                currentPos += toRead;
            }
        } catch (IndexOutOfBoundsException _) {
            throw handlePositionalIOOBE("read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        try {
            final long fileOffset = absoluteBaseOffset + pos;
            final long blockId = fileOffset >>> CACHE_BLOCK_SIZE_POWER;
            final int offInBlock = (int)(fileOffset & CACHE_BLOCK_MASK);
            MemorySegment seg = currentSegment;
            if (blockId == currentBlockId && seg != null) {
                if (offInBlock + Short.BYTES <= seg.byteSize()) {
                    return seg.get(LAYOUT_LE_SHORT, offInBlock);
                }
                long saved = curPosition;
                try { curPosition = pos; return readShort(); } finally { curPosition = saved; }
            }
            seg = getCacheBlockWithOffset(pos);
            final int ofs = lastOffsetInBlock;
            if (ofs + Short.BYTES > seg.byteSize()) {
                long saved = curPosition;
                try { curPosition = pos; return readShort(); } finally { curPosition = saved; }
            }
            return seg.get(LAYOUT_LE_SHORT, ofs);
        } catch (IndexOutOfBoundsException _) {
            throw handlePositionalIOOBE("read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        try {
            final long fileOffset = absoluteBaseOffset + pos;
            final long blockId = fileOffset >>> CACHE_BLOCK_SIZE_POWER;
            final int offInBlock = (int)(fileOffset & CACHE_BLOCK_MASK);
            MemorySegment seg = currentSegment;
            if (blockId == currentBlockId && seg != null) {
                if (offInBlock + Integer.BYTES <= seg.byteSize()) {
                    return seg.get(LAYOUT_LE_INT, offInBlock);
                }
                long saved = curPosition;
                try { curPosition = pos; return readInt(); } finally { curPosition = saved; }
            }
            seg = getCacheBlockWithOffset(pos);
            final int ofs = lastOffsetInBlock;
            if (ofs + Integer.BYTES > seg.byteSize()) {
                long saved = curPosition;
                try { curPosition = pos; return readInt(); } finally { curPosition = saved; }
            }
            return seg.get(LAYOUT_LE_INT, ofs);
        } catch (IndexOutOfBoundsException _) {
            throw handlePositionalIOOBE("read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        try {
            final long fileOffset = absoluteBaseOffset + pos;
            final long blockId = fileOffset >>> CACHE_BLOCK_SIZE_POWER;
            final int offInBlock = (int)(fileOffset & CACHE_BLOCK_MASK);
            MemorySegment seg = currentSegment;
            if (blockId == currentBlockId && seg != null) {
                if (offInBlock + Long.BYTES <= seg.byteSize()) {
                    return seg.get(LAYOUT_LE_LONG, offInBlock);
                }
                long saved = curPosition;
                try { curPosition = pos; return readLong(); } finally { curPosition = saved; }
            }
            seg = getCacheBlockWithOffset(pos);
            final int ofs = lastOffsetInBlock;
            if (ofs + Long.BYTES > seg.byteSize()) {
                long saved = curPosition;
                try { curPosition = pos; return readLong(); } finally { curPosition = saved; }
            }
            return seg.get(LAYOUT_LE_LONG, ofs);
        } catch (IndexOutOfBoundsException _) {
            throw handlePositionalIOOBE("read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }


    // ======================== Position / length ========================

    @Override
    public long getFilePointer() {
        ensureOpen();
        return curPosition;
    }

    @Override
    public final long length() {
        return length;
    }

    @Override
    public void seek(long pos) throws IOException {
        ensureOpen();
        if (pos < 0 || pos > length) {
            throw handlePositionalIOOBE("seek", pos);
        }
        this.curPosition = pos;
        final long fileOffset = absoluteBaseOffset + pos;
        final long blockId = fileOffset >>> CACHE_BLOCK_SIZE_POWER;
        if (blockId == currentBlockId && currentSegment != null) {
            currentOffsetInBlock = (int)(fileOffset & CACHE_BLOCK_MASK);
        } else {
            currentOffsetInBlock = CACHE_BLOCK_SIZE; // mark exhausted
        }
    }

    // ======================== Clone / Slice ========================

    @Override
    public final CachedMemorySegmentIndexInputV2 clone() {
        final CachedMemorySegmentIndexInputV2 clone = buildSlice(null, 0L, this.length);
        try {
            clone.seek(getFilePointer());
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
        return clone;
    }

    @Override
    public final CachedMemorySegmentIndexInputV2 slice(String sliceDescription, long offset, long length)
            throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length) {
            throw new IllegalArgumentException(
                    "slice() " + sliceDescription + " out of bounds: offset=" + offset
                            + ",length=" + length + ",fileLength=" + this.length + ": " + this);
        }
        var slice = buildSlice(sliceDescription, offset, length);
        slice.seek(0L);
        return slice;
    }

    CachedMemorySegmentIndexInputV2 buildSlice(String sliceDescription, long sliceOffset, long length) {
        ensureOpen();
        final long sliceAbsoluteBase = this.absoluteBaseOffset + sliceOffset;
        final String desc = getFullSliceDescription(sliceDescription);

        CachedMemorySegmentIndexInputV2 slice = new CachedMemorySegmentIndexInputV2(
                desc, path, normalizedPath, normalizedPathString,
                sliceAbsoluteBase, length, blockCache, readaheadManager, readaheadContext,
                true, blockTable); // share parent's blockTable

        try {
            slice.seek(0L);
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
        return slice;
    }

    // ======================== Close ========================

    @Override
    @SuppressWarnings("ConvertToTryWithResources")
    public final void close() throws IOException {
        if (!isOpen) return;
        isOpen = false;

        // Unpin current block
        if (currentBlockValue != null) {
            currentBlockValue.unpin();
            currentBlockValue = null;
        }
        currentSegment = null;
        currentSegmentAddress = 0L;
        currentOffsetInBlock = CACHE_BLOCK_SIZE;

        // Only parent clears the table and closes readahead
        if (!isClone) {
            blockTable.clear();
            readaheadManager.close();
        }
    }
}
