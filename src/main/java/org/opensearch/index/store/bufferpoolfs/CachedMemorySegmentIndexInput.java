/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Path;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.GroupVIntUtil;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;

/**
 * A high-performance IndexInput implementation that uses memory-mapped segments with block-level caching.
 *
 * <p>This implementation provides :
 * <ul>
 * <li>Block-aligned cached memory segments for efficient random access</li>
 * <li>Reference counting to manage memory lifecycle</li>
 * <li>Read-ahead support for sequential access patterns</li>
 * <li>Optimized bulk operations for primitive arrays</li>
 * <li>Slice support with offset management</li>
 * </ul>
 *
 * <p>The class uses a {@link BlockSlotTinyCache} for L1 caching and falls back to
 * the main {@link BlockCache} for cache misses. Memory segments are pinned during
 * access to prevent eviction races and unpinned when no longer needed.
 *
 * <p><b>JIT bounds-check elimination:</b> Sequential scalar reads (readByte, readShort,
 * readInt, readLong) use a {@code currentOffsetInBlock} field bounded by the compile-time
 * constant {@code CACHE_BLOCK_SIZE}. HotSpot should be able to prove {@code 0 <= off < CACHE_BLOCK_SIZE}
 * and eliminate MemorySegment bounds checks (checkBounds, checkValidStateRaw, sessionImpl).
 * This branch investigates why that elimination may not be happening compared to Lucene's
 * MemorySegmentIndexInput. Bulk and positional reads use {@code seg.byteSize()} because
 * the last block of a file may be smaller than CACHE_BLOCK_SIZE.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
public class CachedMemorySegmentIndexInput extends IndexInput implements RandomAccessInput {

    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    static final ValueLayout.OfShort LAYOUT_LE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final long length;

    final Path path;
    final BlockCache<RefCountedMemorySegment> blockCache;
    final ReadaheadManager readaheadManager;
    final ReadaheadContext readaheadContext;

    final long absoluteBaseOffset; // absolute position in original file where this input starts
    final boolean isSlice; // true for slices, false for main instances

    long curPosition = 0L; // absolute position within this input (0-based)
    volatile boolean isOpen = true;

    // Single block cache for current access
    private long currentBlockOffset = -1;
    private BlockCacheValue<RefCountedMemorySegment> currentBlock = null;
    private MemorySegment currentSegment = null; // cached raw segment — avoids value().segment() virtual calls on hot path

    // JIT-friendly: track current offset within the cached block.
    // Sequential scalar reads use `off < CACHE_BLOCK_SIZE` guard with this field so
    // HotSpot can prove the access is safe and eliminate MemorySegment bounds checks.
    // Initialized to CACHE_BLOCK_SIZE (exhausted) so first read triggers block load.
    private int currentOffsetInBlock = CACHE_BLOCK_SIZE;

    // Cached offset from last getCacheBlockWithOffset call (avoid BlockAccess allocation)
    private int lastOffsetInBlock;

    private final BlockSlotTinyCache blockSlotTinyCache;

    // Safe because IndexInput instances are not thread-safe per Lucene contract -
    // each thread must use its own clone().
    private final BlockSlotTinyCache.CacheHitHolder cacheHitHolder = new BlockSlotTinyCache.CacheHitHolder();

    /**
     * Creates a new CachedMemorySegmentIndexInput instance.
     *
     * @param resourceDescription description of the resource for debugging
     * @param path the file path being accessed
     * @param length the length of the file in bytes
     * @param blockCache the main block cache for storing memory segments
     * @param readaheadManager manager for read-ahead operations
     * @param readaheadContext context for read-ahead policy decisions
     * @param blockSlotTinyCache L1 cache for recently accessed blocks
     * @return a new CachedMemorySegmentIndexInput instance
     */
    public static CachedMemorySegmentIndexInput newInstance(
        String resourceDescription,
        Path path,
        long length,
        BlockCache<RefCountedMemorySegment> blockCache,
        ReadaheadManager readaheadManager,
        ReadaheadContext readaheadContext,
        BlockSlotTinyCache blockSlotTinyCache
    ) {
        CachedMemorySegmentIndexInput input = new CachedMemorySegmentIndexInput(
            resourceDescription,
            path,
            0,
            length,
            blockCache,
            readaheadManager,
            readaheadContext,
            false,
            blockSlotTinyCache
        );
        try {
            input.seek(0L);
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
        return input;
    }

    private CachedMemorySegmentIndexInput(
        String resourceDescription,
        Path path,
        long absoluteBaseOffset,
        long length,
        BlockCache<RefCountedMemorySegment> blockCache,
        ReadaheadManager readaheadManager,
        ReadaheadContext readaheadContext,
        boolean isSlice,
        BlockSlotTinyCache blockSlotTinyCache
    ) {
        super(resourceDescription);
        this.path = path;
        this.absoluteBaseOffset = absoluteBaseOffset;
        this.length = length;
        this.blockCache = blockCache;
        this.readaheadManager = readaheadManager;
        this.readaheadContext = readaheadContext;
        this.isSlice = isSlice;
        this.blockSlotTinyCache = blockSlotTinyCache;
    }

    void ensureOpen() {
        if (!isOpen) {
            throw alreadyClosed(null);
        }
    }

    // the unused parameter is just to silence javac about unused variables
    RuntimeException handlePositionalIOOBE(RuntimeException unused, String action, long pos) throws IOException {
        if (pos < 0L) {
            return new IllegalArgumentException(action + " negative position (pos=" + pos + "): " + this);
        } else {
            throw new EOFException(action + " past EOF (pos=" + pos + "): " + this);
        }
    }

    // the unused parameter is just to silence javac about unused variables
    AlreadyClosedException alreadyClosed(RuntimeException unused) {
        return new AlreadyClosedException("Already closed: " + this);
    }

    /**
     * Optimized method to get both cache block and offset in one operation.
     * Returns a pinned block that must be managed via currentBlock.
     * Also updates currentSegment and currentOffsetInBlock for JIT fast paths.
     *
     * @param pos position relative to this input
     * @return MemorySegment for the cache block (offset available in lastOffsetInBlock)
     * @throws IOException if the block cannot be acquired
     */
    private MemorySegment getCacheBlockWithOffset(long pos) throws IOException {
        final long fileOffset = absoluteBaseOffset + pos;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        final int offsetInBlock = (int) (fileOffset - blockOffset);

        // Fast path: reuse current block if still valid.
        // This access is safe without generation check because currentBlock
        // is pinned (refCount > 1) so it cannot be returned to pool or reused
        // for different data while we hold it.
        if (blockOffset == currentBlockOffset && currentSegment != null) {
            lastOffsetInBlock = offsetInBlock;
            currentOffsetInBlock = offsetInBlock;
            return currentSegment;
        }

        cacheHitHolder.reset();

        // BlockSlotTinyCache returns already-pinned values
        final BlockCacheValue<RefCountedMemorySegment> cacheValue = blockSlotTinyCache.acquireRefCountedValue(blockOffset, cacheHitHolder);

        if (cacheValue == null) {
            throw new IOException("Failed to acquire cache value for block at offset " + blockOffset);
        }

        RefCountedMemorySegment pinnedBlock = cacheValue.value();

        // Unpin old block before swapping
        if (currentBlock != null) {
            currentBlock.unpin();
        }

        currentBlockOffset = blockOffset;
        currentBlock = cacheValue;

        // Reinterpret to constant CACHE_BLOCK_SIZE so the JIT sees a fixed-size segment
        // and can eliminate MemorySegment bounds checks (checkBounds, sessionImpl,
        // checkValidStateRaw) at compile time. Skip for the last block of the file
        // where valid data may be less than CACHE_BLOCK_SIZE.
        final long fileEnd = absoluteBaseOffset + length;
        if (blockOffset + CACHE_BLOCK_SIZE <= fileEnd) {
            currentSegment = pinnedBlock.segment().reinterpret(CACHE_BLOCK_SIZE);
        } else {
            currentSegment = pinnedBlock.segment();
        }

        // Notify readahead manager of access pattern
        if (readaheadContext != null) {
            readaheadContext.onAccess(blockOffset, cacheHitHolder.wasCacheHit());
        }

        lastOffsetInBlock = offsetInBlock;
        currentOffsetInBlock = offsetInBlock;
        return currentSegment;
    }

    @Override
    public final byte readByte() throws IOException {
        try {
            // Lucene-style hot path: just read, let exceptions handle boundaries.
            // On the happy path this is 1 memory load + 2 increments — no branches.
            final byte v = currentSegment.get(LAYOUT_BYTE, currentOffsetInBlock);
            currentOffsetInBlock++;
            curPosition++;
            return v;
        } catch (IndexOutOfBoundsException _) {
            // Block exhausted — load next block and retry
            return readByteSlow();
        } catch (NullPointerException _) {
            // First access or segment not yet loaded — load current block and retry
            if (!isOpen) throw alreadyClosed(null);
            return readByteSlow();
        } catch (IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    private byte readByteSlow() throws IOException {
        final long currentPos = curPosition;
        if (currentPos >= length) {
            throw new EOFException("read past EOF: " + this);
        }
        final MemorySegment seg = getCacheBlockWithOffset(currentPos);
        final byte v = seg.get(LAYOUT_BYTE, lastOffsetInBlock);
        currentOffsetInBlock = lastOffsetInBlock + 1;
        curPosition = currentPos + 1;
        return v;
    }

    @Override
    public final void readBytes(byte[] b, int offset, int len) throws IOException {
        if (len == 0)
            return;

        final long startPos = curPosition;
        int remaining = len;
        int bufferOffset = offset;
        long currentPos = startPos;

        try {
            while (remaining > 0) {
                final MemorySegment seg = getCacheBlockWithOffset(currentPos);
                final int offInBlock = lastOffsetInBlock;
                // Use seg.byteSize() here — last block may be smaller than CACHE_BLOCK_SIZE
                final int avail = (int) (seg.byteSize() - offInBlock);

                // Fast path: full block copy
                if (offInBlock == 0 && remaining >= CACHE_BLOCK_SIZE && seg.byteSize() >= CACHE_BLOCK_SIZE) {
                    MemorySegment.copy(seg, LAYOUT_BYTE, 0L, b, bufferOffset, CACHE_BLOCK_SIZE);
                    remaining -= CACHE_BLOCK_SIZE;
                    bufferOffset += CACHE_BLOCK_SIZE;
                    currentPos += CACHE_BLOCK_SIZE;
                    continue;
                }

                // Partial block
                final int toRead = Math.min(remaining, avail);
                MemorySegment.copy(seg, LAYOUT_BYTE, offInBlock, b, bufferOffset, toRead);
                remaining -= toRead;
                bufferOffset += toRead;
                currentPos += toRead;
            }

            curPosition = startPos + len;
            // Sync currentOffsetInBlock with new position
            final long endFileOffset = absoluteBaseOffset + curPosition;
            final long endBlockOffset = endFileOffset & ~CACHE_BLOCK_MASK;
            if (endBlockOffset == currentBlockOffset && currentSegment != null) {
                currentOffsetInBlock = (int) (endFileOffset - endBlockOffset);
            } else {
                currentOffsetInBlock = CACHE_BLOCK_SIZE;
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        if (length == 0)
            return;

        final long startPos = curPosition;
        final long totalBytes = Integer.BYTES * (long) length;

        try {
            final MemorySegment segment = getCacheBlockWithOffset(startPos);
            final int offsetInBlock = lastOffsetInBlock;

            // Use seg.byteSize() — last block may be smaller than CACHE_BLOCK_SIZE
            if (offsetInBlock + totalBytes <= segment.byteSize()) {
                MemorySegment.copy(segment, LAYOUT_LE_INT, offsetInBlock, dst, offset, length);
                curPosition += totalBytes;
                currentOffsetInBlock = offsetInBlock + (int) totalBytes;
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
        if (length == 0)
            return;

        final long startPos = curPosition;
        final long totalBytes = Long.BYTES * (long) length;

        try {
            final MemorySegment segment = getCacheBlockWithOffset(startPos);
            final int offsetInBlock = lastOffsetInBlock;

            // Use seg.byteSize() — last block may be smaller than CACHE_BLOCK_SIZE
            if (offsetInBlock + totalBytes <= segment.byteSize()) {
                MemorySegment.copy(segment, LAYOUT_LE_LONG, offsetInBlock, dst, offset, length);
                curPosition += totalBytes;
                currentOffsetInBlock = offsetInBlock + (int) totalBytes;
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
        if (length == 0)
            return;

        final long startPos = curPosition;
        final long totalBytes = Float.BYTES * (long) length;

        try {
            final MemorySegment segment = getCacheBlockWithOffset(startPos);
            final int offsetInBlock = lastOffsetInBlock;

            // Use seg.byteSize() — last block may be smaller than CACHE_BLOCK_SIZE
            if (offsetInBlock + totalBytes <= segment.byteSize()) {
                MemorySegment.copy(segment, LAYOUT_LE_FLOAT, offsetInBlock, dst, offset, length);
                curPosition += totalBytes;
                currentOffsetInBlock = offsetInBlock + (int) totalBytes;
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
        try {
            final int off = currentOffsetInBlock;
            final short v = currentSegment.get(LAYOUT_LE_SHORT, off);
            currentOffsetInBlock = off + Short.BYTES;
            curPosition += Short.BYTES;
            return v;
        } catch (IndexOutOfBoundsException _) {
            return readShortSlow();
        } catch (NullPointerException _) {
            if (!isOpen) throw alreadyClosed(null);
            return readShortSlow();
        } catch (IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    private short readShortSlow() throws IOException {
        // Check if read spans a block boundary
        final long fileOffset = absoluteBaseOffset + curPosition;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        final int offInBlock = (int) (fileOffset - blockOffset);
        if (offInBlock + Short.BYTES > CACHE_BLOCK_SIZE) {
            return super.readShort();
        }
        final MemorySegment seg = getCacheBlockWithOffset(curPosition);
        final int offsetInBlock = lastOffsetInBlock;
        if (offsetInBlock + Short.BYTES > seg.byteSize()) {
            return super.readShort();
        }
        final short v = seg.get(LAYOUT_LE_SHORT, offsetInBlock);
        currentOffsetInBlock = offsetInBlock + Short.BYTES;
        curPosition += Short.BYTES;
        return v;
    }

    @Override
    public final int readInt() throws IOException {
        try {
            final int off = currentOffsetInBlock;
            final int v = currentSegment.get(LAYOUT_LE_INT, off);
            currentOffsetInBlock = off + Integer.BYTES;
            curPosition += Integer.BYTES;
            return v;
        } catch (IndexOutOfBoundsException _) {
            return readIntSlow();
        } catch (NullPointerException _) {
            if (!isOpen) throw alreadyClosed(null);
            return readIntSlow();
        } catch (IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    private int readIntSlow() throws IOException {
        // Check if read spans a block boundary
        final long fileOffset = absoluteBaseOffset + curPosition;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        final int offInBlock = (int) (fileOffset - blockOffset);
        if (offInBlock + Integer.BYTES > CACHE_BLOCK_SIZE) {
            return super.readInt();
        }
        final MemorySegment seg = getCacheBlockWithOffset(curPosition);
        final int offsetInBlock = lastOffsetInBlock;
        if (offsetInBlock + Integer.BYTES > seg.byteSize()) {
            return super.readInt();
        }
        final int v = seg.get(LAYOUT_LE_INT, offsetInBlock);
        currentOffsetInBlock = offsetInBlock + Integer.BYTES;
        curPosition += Integer.BYTES;
        return v;
    }

    @Override
    public final long readLong() throws IOException {
        try {
            final int off = currentOffsetInBlock;
            final long v = currentSegment.get(LAYOUT_LE_LONG, off);
            currentOffsetInBlock = off + Long.BYTES;
            curPosition += Long.BYTES;
            return v;
        } catch (IndexOutOfBoundsException _) {
            return readLongSlow();
        } catch (NullPointerException _) {
            if (!isOpen) throw alreadyClosed(null);
            return readLongSlow();
        } catch (IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    private long readLongSlow() throws IOException {
        // Check if read spans a block boundary
        final long fileOffset = absoluteBaseOffset + curPosition;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        final int offInBlock = (int) (fileOffset - blockOffset);
        if (offInBlock + Long.BYTES > CACHE_BLOCK_SIZE) {
            return super.readLong();
        }
        final MemorySegment seg = getCacheBlockWithOffset(curPosition);
        final int offsetInBlock = lastOffsetInBlock;
        if (offsetInBlock + Long.BYTES > seg.byteSize()) {
            return super.readLong();
        }
        final long v = seg.get(LAYOUT_LE_LONG, offsetInBlock);
        currentOffsetInBlock = offsetInBlock + Long.BYTES;
        curPosition += Long.BYTES;
        return v;
    }

    @Override
    public void readGroupVInt(int[] dst, int offset) throws IOException {
        try {
            final MemorySegment segment = getCacheBlockWithOffset(curPosition);
            final int offsetInBlock = lastOffsetInBlock;

            // Use seg.byteSize() — last block may be smaller than CACHE_BLOCK_SIZE
            final int len = GroupVIntUtil
                .readGroupVInt(
                    this,
                    segment.byteSize() - offsetInBlock,
                    p -> segment.get(LAYOUT_LE_INT, p),
                    offsetInBlock,
                    dst,
                    offset
                );
            curPosition += len;
            currentOffsetInBlock = offsetInBlock + len;
        } catch (IllegalStateException | NullPointerException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final int readVInt() throws IOException {
        // this can make JVM less confused (see LUCENE-10366)
        return super.readVInt();
    }

    @Override
    public final long readVLong() throws IOException {
        // this can make JVM less confused (see LUCENE-10366)
        return super.readVLong();
    }

    @Override
    public long getFilePointer() {
        ensureOpen();
        return curPosition;
    }

    /**
     * Returns the absolute file offset for the current position.
     * This is useful for cache keys, encryption, and other operations that need
     * the actual position in the original file.
     *
     * @return the absolute byte offset in the original file
     */
    public long getAbsoluteFileOffset() {
        return absoluteBaseOffset + getFilePointer();
    }

    /**
     * Returns the absolute file offset for a given position within this input.
     * This is useful for cache keys, encryption, and other operations that need
     * the actual position in the original file for random access operations.
     *
     * @param pos position relative to this input (0-based)
     * @return absolute position in the original file
     */
    public long getAbsoluteFileOffset(long pos) {
        return absoluteBaseOffset + pos;
    }

    @Override
    public void seek(long pos) throws IOException {
        ensureOpen();
        if (pos < 0 || pos > length) {
            throw handlePositionalIOOBE(null, "seek", pos);
        }
        this.curPosition = pos;
        // Recompute currentOffsetInBlock so the fast path stays in sync
        final long fileOffset = absoluteBaseOffset + pos;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        if (blockOffset == currentBlockOffset && currentSegment != null) {
            currentOffsetInBlock = (int) (fileOffset - blockOffset);
        } else {
            // Different block — mark exhausted so next read triggers getCacheBlockWithOffset
            currentOffsetInBlock = CACHE_BLOCK_SIZE;
        }
    }

    @Override
    public byte readByte(long pos) throws IOException {
        if (pos < 0 || pos >= length) {
            return 0;
        }

        try {
            // Inlined fast path using cached block + MemorySegment
            final long fileOffset = absoluteBaseOffset + pos;
            final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
            final int offInBlock = (int) (fileOffset - blockOffset);
            if (blockOffset == currentBlockOffset && currentSegment != null) {
                return currentSegment.get(LAYOUT_BYTE, offInBlock);
            }
            final MemorySegment seg = getCacheBlockWithOffset(pos);
            return seg.get(LAYOUT_BYTE, lastOffsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        try {
            final long fileOffset = absoluteBaseOffset + pos;
            final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
            final int offInBlock = (int) (fileOffset - blockOffset);
            MemorySegment seg = currentSegment;
            if (blockOffset == currentBlockOffset && seg != null) {
                // Use seg.byteSize() — last block may be smaller than CACHE_BLOCK_SIZE
                if (offInBlock + Short.BYTES <= seg.byteSize()) {
                    return seg.get(LAYOUT_LE_SHORT, offInBlock);
                }
                long savedPos = curPosition;
                try {
                    curPosition = pos;
                    return readShort();
                } finally {
                    curPosition = savedPos;
                }
            }
            seg = getCacheBlockWithOffset(pos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + Short.BYTES > seg.byteSize()) {
                long savedPos = curPosition;
                try {
                    curPosition = pos;
                    return readShort();
                } finally {
                    curPosition = savedPos;
                }
            }
            return seg.get(LAYOUT_LE_SHORT, offsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        try {
            final long fileOffset = absoluteBaseOffset + pos;
            final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
            final int offInBlock = (int) (fileOffset - blockOffset);
            MemorySegment seg = currentSegment;
            if (blockOffset == currentBlockOffset && seg != null) {
                if (offInBlock + Integer.BYTES <= seg.byteSize()) {
                    return seg.get(LAYOUT_LE_INT, offInBlock);
                }
                long savedPos = curPosition;
                try {
                    curPosition = pos;
                    return readInt();
                } finally {
                    curPosition = savedPos;
                }
            }
            seg = getCacheBlockWithOffset(pos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + Integer.BYTES > seg.byteSize()) {
                long savedPos = curPosition;
                try {
                    curPosition = pos;
                    return readInt();
                } finally {
                    curPosition = savedPos;
                }
            }
            return seg.get(LAYOUT_LE_INT, offsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        try {
            final long fileOffset = absoluteBaseOffset + pos;
            final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
            final int offInBlock = (int) (fileOffset - blockOffset);
            MemorySegment seg = currentSegment;
            if (blockOffset == currentBlockOffset && seg != null) {
                if (offInBlock + Long.BYTES <= seg.byteSize()) {
                    return seg.get(LAYOUT_LE_LONG, offInBlock);
                }
                long savedPos = curPosition;
                try {
                    curPosition = pos;
                    return readLong();
                } finally {
                    curPosition = savedPos;
                }
            }
            seg = getCacheBlockWithOffset(pos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + Long.BYTES > seg.byteSize()) {
                long savedPos = curPosition;
                try {
                    curPosition = pos;
                    return readLong();
                } finally {
                    curPosition = savedPos;
                }
            }
            return seg.get(LAYOUT_LE_LONG, offsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final long length() {
        return length;
    }

    @Override
    public final CachedMemorySegmentIndexInput clone() {
        final CachedMemorySegmentIndexInput clone = buildSlice((String) null, 0L, this.length);
        try {
            clone.seek(getFilePointer());
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
        return clone;
    }

    /**
     * Creates a slice of this index input, with the given description, offset, and length.
     * The slice is seeked to the beginning.
     */
    @Override
    public final CachedMemorySegmentIndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length
                    + ": "
                    + this
            );
        }
        var slice = buildSlice(sliceDescription, offset, length);
        slice.seek(0L);
        return slice;
    }

    /** Builds the actual sliced IndexInput. **/
    CachedMemorySegmentIndexInput buildSlice(String sliceDescription, long sliceOffset, long length) {
        ensureOpen();
        final long sliceAbsoluteBaseOffset = this.absoluteBaseOffset + sliceOffset;
        final String newResourceDescription = getFullSliceDescription(sliceDescription);

        CachedMemorySegmentIndexInput slice = new CachedMemorySegmentIndexInput(
            newResourceDescription,
            path,
            sliceAbsoluteBaseOffset,
            length,
            blockCache,
            readaheadManager,
            readaheadContext,
            true,
            blockSlotTinyCache
        );

        try {
            slice.seek(0L);
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
        return slice;
    }

    @Override
    @SuppressWarnings("ConvertToTryWithResources")
    public final void close() throws IOException {
        if (!isOpen) {
            return;
        }

        isOpen = false;

        // Unpin current block
        if (currentBlock != null) {
            currentBlock.unpin();
            currentBlock = null;
            currentSegment = null;
            currentOffsetInBlock = CACHE_BLOCK_SIZE;
        }

        if (!isSlice) {
            if (blockSlotTinyCache != null) {
                blockSlotTinyCache.clear();
            }
            readaheadManager.close();
        }
    }
}
