/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

/**
 * A sparse two-level table mapping block IDs to MemorySegments, using a
 * bitmap + popcount compressed representation.
 *
 * <p>Used by CachedMemorySegmentIndexInputV2 and DirectByteBufferIndexInput
 * as a per-file L1 lookup table.
 * Block ID = fileOffset &gt;&gt;&gt; CACHE_BLOCK_SIZE_POWER.
 *
 * <p>Directory structure: outer array of {@link Page} references. Each page
 * covers 64 consecutive block IDs using a 64-bit bitmap to indicate which
 * slots are present, and a compact {@code MemorySegment[]} array holding
 * only the populated entries. The position within the compact array is
 * computed via {@code Long.bitCount(bitmap &amp; (mask - 1))} — a single
 * CPU instruction on modern hardware.
 *
 * <p>Thread safety: pages are immutable value objects replaced atomically
 * via copy-on-write under a per-page synchronization guard. Reads are
 * lock-free via VarHandle acquire semantics on the page array. Writes
 * (put/remove) synchronize on {@code this} to serialize page mutations.
 *
 * <p>When a page's bitmap reaches zero (all entries removed), the page
 * reference is nulled out, reclaiming the compact array.
 */
public class SparseLongBlockTable {

    public static final int PAGE_SHIFT = 6; // 64 entries per page
    private static final int PAGE_MASK  = (1 << PAGE_SHIFT) - 1; // 63

    /**
     * An immutable page: a 64-bit bitmap indicating which of the 64 slots
     * are present, plus a compact array of only the populated values.
     * {@code values.length == Long.bitCount(bitmap)}.
     */
    static final class Page {
        final long bitmap;
        final MemorySegment[] values;

        Page(long bitmap, MemorySegment[] values) {
            this.bitmap = bitmap;
            this.values = values;
        }
    }

    /** VarHandle for acquire/release access to Page[] elements. */
    private static final VarHandle PAGE_ARRAY_HANDLE =
            MethodHandles.arrayElementVarHandle(Page[].class);

    private volatile Page[] directory;

    public SparseLongBlockTable(int initialPages) {
        directory = new Page[Math.max(initialPages, 1)];
    }

    /**
     * Returns the MemorySegment for the given blockId, or null if not populated.
     * Lock-free read path using VarHandle getAcquire.
     */
    public MemorySegment get(long blockId) {
        int outer = (int)(blockId >>> PAGE_SHIFT);
        int slot  = (int)(blockId & PAGE_MASK);
        long mask = 1L << slot;

        Page[] dir = directory; // volatile read
        if (outer >= dir.length) return null;

        Page page = (Page) PAGE_ARRAY_HANDLE.getAcquire(dir, outer);
        if (page == null) return null;

        if ((page.bitmap & mask) == 0) return null; // bit not set

        int index = Long.bitCount(page.bitmap & (mask - 1));
        return page.values[index];
    }

    /**
     * Stores a MemorySegment at the given blockId, growing the directory
     * and allocating/replacing pages as needed. Copy-on-write per page.
     */
    public void put(long blockId, MemorySegment segment) {
        int outer = (int)(blockId >>> PAGE_SHIFT);
        int slot  = (int)(blockId & PAGE_MASK);
        long mask = 1L << slot;

        synchronized (this) {
            Page[] dir = directory;
            if (outer >= dir.length) {
                dir = growDirectory(outer);
            }

            Page page = dir[outer];
            if (page == null) {
                // New page with single entry
                Page newPage = new Page(mask, new MemorySegment[]{ segment });
                PAGE_ARRAY_HANDLE.setRelease(dir, outer, newPage);
                return;
            }

            int index = Long.bitCount(page.bitmap & (mask - 1));

            if ((page.bitmap & mask) != 0) {
                // Slot already present — replace value in-place (new array)
                MemorySegment[] newValues = page.values.clone();
                newValues[index] = segment;
                PAGE_ARRAY_HANDLE.setRelease(dir, outer, new Page(page.bitmap, newValues));
            } else {
                // New slot — insert into compact array
                int oldLen = page.values.length;
                MemorySegment[] newValues = new MemorySegment[oldLen + 1];
                System.arraycopy(page.values, 0, newValues, 0, index);
                newValues[index] = segment;
                System.arraycopy(page.values, index, newValues, index + 1, oldLen - index);
                PAGE_ARRAY_HANDLE.setRelease(dir, outer, new Page(page.bitmap | mask, newValues));
            }
        }
    }

    /**
     * Removes the entry at blockId. Returns the previous value or null.
     * If the page becomes empty (bitmap == 0), the page reference is nulled.
     */
    public MemorySegment remove(long blockId) {
        int outer = (int)(blockId >>> PAGE_SHIFT);
        int slot  = (int)(blockId & PAGE_MASK);
        long mask = 1L << slot;

        synchronized (this) {
            Page[] dir = directory;
            if (outer >= dir.length) return null;

            Page page = dir[outer];
            if (page == null) return null;

            if ((page.bitmap & mask) == 0) return null; // not present

            int index = Long.bitCount(page.bitmap & (mask - 1));
            MemorySegment prev = page.values[index];

            long newBitmap = page.bitmap & ~mask;
            if (newBitmap == 0) {
                // Page is now empty — reclaim
                PAGE_ARRAY_HANDLE.setRelease(dir, outer, null);
            } else {
                int oldLen = page.values.length;
                MemorySegment[] newValues = new MemorySegment[oldLen - 1];
                System.arraycopy(page.values, 0, newValues, 0, index);
                System.arraycopy(page.values, index + 1, newValues, index, oldLen - index - 1);
                PAGE_ARRAY_HANDLE.setRelease(dir, outer, new Page(newBitmap, newValues));
            }
            return prev;
        }
    }

    /**
     * Clears all entries. Called on parent close.
     * Nulls all page references.
     */
    public void clear() {
        Page[] dir = directory;
        for (int i = 0; i < dir.length; i++) {
            PAGE_ARRAY_HANDLE.setRelease(dir, i, null);
        }
    }

    /**
     * Grows the directory to accommodate the given outer index.
     * Must be called under synchronized(this).
     * Returns the new directory.
     */
    private Page[] growDirectory(int outer) {
        Page[] dir = directory;
        int newSize = Math.max(outer + 1, dir.length * 2);
        Page[] newDir = Arrays.copyOf(dir, newSize);
        directory = newDir;
        return newDir;
    }

    /**
     * Returns the page population count (number of set bits in bitmap)
     * for the given outer index. Returns -1 if the page is not allocated.
     * Visible for testing.
     */
    public int getChunkPopCount(int outer) {
        Page[] dir = directory;
        if (outer >= dir.length) return -1;
        Page page = (Page) PAGE_ARRAY_HANDLE.getAcquire(dir, outer);
        if (page == null) return -1;
        return Long.bitCount(page.bitmap);
    }

    /**
     * Returns whether the page at the given outer index is allocated.
     * Visible for testing.
     */
    public boolean isChunkAllocated(int outer) {
        Page[] dir = directory;
        return outer < dir.length && PAGE_ARRAY_HANDLE.getAcquire(dir, outer) != null;
    }
}
