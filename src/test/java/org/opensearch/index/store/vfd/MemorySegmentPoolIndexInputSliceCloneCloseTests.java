/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;

import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link MemorySegmentPoolIndexInput} Task 14 methods:
 * prefetch, slice, clone, close, seek, getFilePointer, length.
 *
 * <p><b>Validates: Requirements 19, 20, 21, 22</b></p>
 */
public class MemorySegmentPoolIndexInputSliceCloneCloseTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE = StaticConfigs.CACHE_BLOCK_SIZE;

    private VfdRegistry registry;
    private MemoryPool pool;
    private BlockCache blockCache;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new VfdRegistry(10, 5);
        pool = new MemoryPool(64, BLOCK_SIZE, 0);
        blockCache = new BlockCache(1024, registry);
    }

    @Override
    public void tearDown() throws Exception {
        blockCache.close();
        pool.close();
        registry.close();
        super.tearDown();
    }

    private Path createTestFile(int numBlocks) throws IOException {
        byte[] data = new byte[numBlocks * BLOCK_SIZE];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Path file = createTempFile("task14-test", ".dat");
        Files.write(file, data);
        return file;
    }


    private VirtualFileChannel createChannel(VirtualFileDescriptor vfd) {
        return new VirtualFileChannel(
            vfd, new FileChannelIOBackend(), pool, blockCache,
            registry, new Semaphore(64),
            VirtualFileChannel.DEFAULT_INFLIGHT_TIMEOUT_MS,
            null, BLOCK_SIZE);
    }

    private MemorySegmentPoolIndexInput createIndexInput(
            VirtualFileDescriptor vfd, VirtualFileChannel channel, long fileLength) {
        return new MemorySegmentPoolIndexInput(vfd, channel, blockCache, fileLength);
    }

    // =========================================================================
    // seek / getFilePointer / length
    // =========================================================================

    public void testSeekAndGetFilePointer() throws IOException {
        Path file = createTestFile(3);
        long fileLength = 3L * BLOCK_SIZE;
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        assertEquals(0, input.getFilePointer());

        input.seek(100);
        assertEquals(100, input.getFilePointer());

        input.seek(BLOCK_SIZE + 50);
        assertEquals(BLOCK_SIZE + 50, input.getFilePointer());

        // Seek to end is valid
        input.seek(fileLength);
        assertEquals(fileLength, input.getFilePointer());

        // Seek back to start
        input.seek(0);
        assertEquals(0, input.getFilePointer());

        input.close();
    }

    public void testSeekThenRead() throws IOException {
        Path file = createTestFile(2);
        long fileLength = 2L * BLOCK_SIZE;
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Seek to middle of file and read
        long seekPos = BLOCK_SIZE + 42;
        input.seek(seekPos);
        byte expected = (byte) (seekPos & 0xFF);
        assertEquals(expected, input.readByte());
        assertEquals(seekPos + 1, input.getFilePointer());

        input.close();
    }

    public void testSeekPastEOFThrows() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        expectThrows(EOFException.class, () -> input.seek(BLOCK_SIZE + 1));
        input.close();
    }

    public void testSeekNegativeThrows() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        expectThrows(EOFException.class, () -> input.seek(-1));
        input.close();
    }

    public void testLengthReturnsFileLength() throws IOException {
        Path file = createTestFile(3);
        long fileLength = 3L * BLOCK_SIZE;
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        assertEquals(fileLength, input.length());
        input.close();
    }

    // =========================================================================
    // slice — bounds checking
    // =========================================================================

    public void testSliceReadsCorrectData() throws IOException {
        int numBlocks = 3;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * (long) BLOCK_SIZE;
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Slice from middle of file
        long sliceOffset = 100;
        long sliceLength = 200;
        IndexInput slice = input.slice("test-slice", sliceOffset, sliceLength);

        assertEquals(0, slice.getFilePointer());
        assertEquals(sliceLength, slice.length());

        // Read from slice and verify data matches original file content
        for (int i = 0; i < sliceLength; i++) {
            byte expected = (byte) ((sliceOffset + i) & 0xFF);
            assertEquals("Mismatch at slice position " + i, expected, slice.readByte());
        }

        assertEquals(sliceLength, slice.getFilePointer());
        input.close();
    }

    public void testSliceSpanningBlocks() throws IOException {
        int numBlocks = 3;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * (long) BLOCK_SIZE;
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Slice that spans a block boundary
        long sliceOffset = BLOCK_SIZE - 50;
        long sliceLength = 100;
        IndexInput slice = input.slice("cross-block-slice", sliceOffset, sliceLength);

        for (int i = 0; i < sliceLength; i++) {
            byte expected = (byte) ((sliceOffset + i) & 0xFF);
            assertEquals(expected, slice.readByte());
        }

        input.close();
    }

    public void testSliceOfSlice() throws IOException {
        int numBlocks = 3;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * (long) BLOCK_SIZE;
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Create a slice, then slice the slice
        IndexInput slice1 = input.slice("slice1", 100, 500);
        IndexInput slice2 = slice1.slice("slice2", 50, 100);

        assertEquals(100, slice2.length());
        // slice2 reads from file offset 100+50=150
        for (int i = 0; i < 100; i++) {
            byte expected = (byte) ((150 + i) & 0xFF);
            assertEquals(expected, slice2.readByte());
        }

        input.close();
    }

    public void testSliceInvalidBoundsThrows() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        // Negative offset
        expectThrows(IllegalArgumentException.class,
            () -> input.slice("bad", -1, 10));

        // Negative length
        expectThrows(IllegalArgumentException.class,
            () -> input.slice("bad", 0, -1));

        // offset + length > sliceLength
        expectThrows(IllegalArgumentException.class,
            () -> input.slice("bad", BLOCK_SIZE - 5, 10));

        input.close();
    }

    public void testSliceIsClone() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        IndexInput slice = input.slice("test", 0, 100);
        assertTrue(slice instanceof MemorySegmentPoolIndexInput);
        assertTrue(((MemorySegmentPoolIndexInput) slice).isClone());

        input.close();
    }

    // =========================================================================
    // clone — independence
    // =========================================================================

    public void testCloneHasIndependentPosition() throws IOException {
        Path file = createTestFile(2);
        long fileLength = 2L * BLOCK_SIZE;
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Read some bytes to advance position
        input.readByte();
        input.readByte();
        input.readByte();
        assertEquals(3, input.getFilePointer());

        // Clone inherits position
        MemorySegmentPoolIndexInput cloned = input.clone();
        assertEquals(3, cloned.getFilePointer());

        // Advance clone — original should not move
        cloned.readByte();
        cloned.readByte();
        assertEquals(5, cloned.getFilePointer());
        assertEquals(3, input.getFilePointer());

        // Advance original — clone should not move
        input.readByte();
        assertEquals(4, input.getFilePointer());
        assertEquals(5, cloned.getFilePointer());

        input.close();
    }

    public void testCloneReadsCorrectData() throws IOException {
        Path file = createTestFile(2);
        long fileLength = 2L * BLOCK_SIZE;
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Seek to a position and clone
        input.seek(100);
        MemorySegmentPoolIndexInput cloned = input.clone();

        // Both should read the same data
        byte fromOriginal = input.readByte();
        input.seek(100);
        cloned.seek(100);
        byte fromClone = cloned.readByte();
        assertEquals(fromOriginal, fromClone);
        assertEquals((byte) (100 & 0xFF), fromClone);

        input.close();
    }

    public void testCloneIsClone() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        MemorySegmentPoolIndexInput cloned = input.clone();
        assertTrue(cloned.isClone());
        assertFalse(input.isClone());

        input.close();
    }

    public void testCloneSharesLengthAndSliceProperties() throws IOException {
        Path file = createTestFile(2);
        long fileLength = 2L * BLOCK_SIZE;
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        MemorySegmentPoolIndexInput cloned = input.clone();
        assertEquals(input.length(), cloned.length());

        input.close();
    }

    // =========================================================================
    // close — only root decrements VFD ref, close idempotent
    // =========================================================================

    public void testCloseRootDecrementsVfdRef() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        int refBefore = vfd.refCount();
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        input.close();
        assertEquals(refBefore - 1, vfd.refCount());
    }

    public void testCloseCloneDoesNotDecrementVfdRef() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        MemorySegmentPoolIndexInput cloned = input.clone();
        int refBeforeCloneClose = vfd.refCount();

        // Close clone — should be no-op for VFD ref
        cloned.close();
        assertEquals(refBeforeCloneClose, vfd.refCount());

        // Close root — should decrement
        input.close();
        assertEquals(refBeforeCloneClose - 1, vfd.refCount());
    }

    public void testCloseSliceDoesNotDecrementVfdRef() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        IndexInput slice = input.slice("test", 0, 100);
        int refBeforeSliceClose = vfd.refCount();

        // Close slice — should be no-op for VFD ref
        slice.close();
        assertEquals(refBeforeSliceClose, vfd.refCount());

        input.close();
    }

    public void testCloseIsIdempotent() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        int refBefore = vfd.refCount();
        input.close();
        int refAfterFirst = vfd.refCount();
        assertEquals(refBefore - 1, refAfterFirst);

        // Second close should be no-op
        input.close();
        assertEquals(refAfterFirst, vfd.refCount());
    }

    public void testCloseNullsCurrentBlock() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        // Read to populate currentBlock
        input.readByte();
        input.close();
        assertTrue(input.isClosed());
    }

    // =========================================================================
    // prefetch — delegates to channel (smoke test)
    // =========================================================================

    public void testPrefetchDoesNotThrow() throws IOException {
        Path file = createTestFile(3);
        long fileLength = 3L * BLOCK_SIZE;
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Prefetch should not throw — it delegates to channel which may no-op
        // if no prefetch executor is configured
        input.prefetch(0, fileLength);
        input.prefetch(BLOCK_SIZE, BLOCK_SIZE);

        input.close();
    }

    // =========================================================================
    // Lucene contract: create root, clone, slice, close only root
    // =========================================================================

    public void testLuceneContractCloneSliceCloseRoot() throws IOException {
        Path file = createTestFile(3);
        long fileLength = 3L * BLOCK_SIZE;
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput root = createIndexInput(vfd, channel, fileLength);

        // Read some data from root
        root.readByte();

        // Create clone and slice
        MemorySegmentPoolIndexInput cloned = root.clone();
        IndexInput slice = root.slice("slice", 100, 200);

        // Read from clone and slice
        cloned.readByte();
        slice.readByte();

        // Close only root (Lucene contract: do NOT close clones/slices)
        int refBefore = vfd.refCount();
        root.close();
        assertEquals(refBefore - 1, vfd.refCount());

        // Verify root is closed
        assertTrue(root.isClosed());
    }
}
