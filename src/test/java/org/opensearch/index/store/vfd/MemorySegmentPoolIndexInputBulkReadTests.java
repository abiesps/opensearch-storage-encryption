/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;

import org.opensearch.index.store.bufferpoolfs.StaticConfigs;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link MemorySegmentPoolIndexInput} positional and bulk
 * read methods: readByte(long pos), readBytes(byte[], int, int),
 * positional readBytes(long, byte[], int, int), readInts, readLongs,
 * readFloats.
 *
 * <p><b>Validates: Requirements 16, 17, 18, 39</b></p>
 */
@SuppressWarnings("preview")
public class MemorySegmentPoolIndexInputBulkReadTests extends OpenSearchTestCase {

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

    /**
     * Create a test file with deterministic content: each byte = (offset & 0xFF).
     */
    private Path createTestFile(int numBlocks) throws IOException {
        byte[] data = new byte[numBlocks * BLOCK_SIZE];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Path file = createTempFile("bulkread-test", ".dat");
        Files.write(file, data);
        return file;
    }

    /**
     * Create a test file with specific byte content.
     */
    private Path createTestFileWithData(byte[] data) throws IOException {
        Path file = createTempFile("bulkread-test", ".dat");
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
    // readBytes — bulk read within a single block
    // =========================================================================

    public void testBulkReadWithinSingleBlock() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        byte[] buf = new byte[64];
        input.readBytes(buf, 0, 64);

        for (int i = 0; i < 64; i++) {
            assertEquals("Mismatch at index " + i, (byte) (i & 0xFF), buf[i]);
        }
        assertEquals(64, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // readBytes — bulk read spanning 2 blocks
    // =========================================================================

    public void testBulkReadSpanningTwoBlocks() throws IOException {
        int numBlocks = 2;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Position near end of block 0, read across boundary
        int startPos = BLOCK_SIZE - 32;
        // Advance to startPos
        byte[] skip = new byte[startPos];
        input.readBytes(skip, 0, startPos);

        byte[] buf = new byte[64]; // 32 bytes from block 0, 32 bytes from block 1
        input.readBytes(buf, 0, 64);

        for (int i = 0; i < 64; i++) {
            int fileOffset = startPos + i;
            assertEquals("Mismatch at offset " + fileOffset,
                (byte) (fileOffset & 0xFF), buf[i]);
        }
        assertEquals(startPos + 64, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // readBytes — bulk read spanning 3 blocks
    // =========================================================================

    public void testBulkReadSpanningThreeBlocks() throws IOException {
        int numBlocks = 3;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Position near end of block 0, read across blocks 0, 1, and 2
        int startPos = BLOCK_SIZE - 16;
        byte[] skip = new byte[startPos];
        input.readBytes(skip, 0, startPos);

        // Read enough to span into block 2
        int readLen = BLOCK_SIZE + 32; // 16 from block 0 + BLOCK_SIZE from block 1 + 16 from block 2... but we need more
        // Actually: 16 bytes left in block 0 + full block 1 + 16 into block 2 = BLOCK_SIZE + 32
        byte[] buf = new byte[readLen];
        input.readBytes(buf, 0, readLen);

        for (int i = 0; i < readLen; i++) {
            int fileOffset = startPos + i;
            assertEquals("Mismatch at offset " + fileOffset,
                (byte) (fileOffset & 0xFF), buf[i]);
        }

        registry.release(vfd);
    }

    // =========================================================================
    // readBytes — at exact block boundary
    // =========================================================================

    public void testBulkReadAtExactBlockBoundary() throws IOException {
        int numBlocks = 2;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Read exactly one full block
        byte[] block0 = new byte[BLOCK_SIZE];
        input.readBytes(block0, 0, BLOCK_SIZE);

        for (int i = 0; i < BLOCK_SIZE; i++) {
            assertEquals((byte) (i & 0xFF), block0[i]);
        }
        assertEquals(BLOCK_SIZE, input.getFilePointer());

        // Read exactly the second full block
        byte[] block1 = new byte[BLOCK_SIZE];
        input.readBytes(block1, 0, BLOCK_SIZE);

        for (int i = 0; i < BLOCK_SIZE; i++) {
            int fileOffset = BLOCK_SIZE + i;
            assertEquals((byte) (fileOffset & 0xFF), block1[i]);
        }

        registry.release(vfd);
    }

    // =========================================================================
    // Positional readByte — does not affect curPosition
    // =========================================================================

    public void testPositionalReadByteDoesNotAffectPosition() throws IOException {
        int numBlocks = 2;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Read first byte sequentially to initialize currentBlock
        byte first = input.readByte();
        assertEquals((byte) 0, first);
        assertEquals(1, input.getFilePointer());

        // Positional read at various offsets — should NOT change file pointer
        byte atZero = input.readByte(0);
        assertEquals((byte) 0, atZero);
        assertEquals(1, input.getFilePointer()); // unchanged

        byte atMiddle = input.readByte(BLOCK_SIZE / 2);
        assertEquals((byte) ((BLOCK_SIZE / 2) & 0xFF), atMiddle);
        assertEquals(1, input.getFilePointer()); // unchanged

        // Positional read in block 1
        byte inBlock1 = input.readByte(BLOCK_SIZE + 10);
        assertEquals((byte) ((BLOCK_SIZE + 10) & 0xFF), inBlock1);
        assertEquals(1, input.getFilePointer()); // unchanged

        registry.release(vfd);
    }

    // =========================================================================
    // Positional readByte — at various offsets
    // =========================================================================

    public void testPositionalReadByteAtVariousOffsets() throws IOException {
        int numBlocks = 3;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Test at block boundaries and various positions
        long[] testPositions = {
            0,                          // start of file
            BLOCK_SIZE - 1,             // last byte of block 0
            BLOCK_SIZE,                 // first byte of block 1
            BLOCK_SIZE + BLOCK_SIZE / 2, // middle of block 1
            2 * BLOCK_SIZE - 1,         // last byte of block 1
            2 * BLOCK_SIZE,             // first byte of block 2
            3 * BLOCK_SIZE - 1          // last byte of file
        };

        for (long pos : testPositions) {
            byte expected = (byte) (pos & 0xFF);
            byte actual = input.readByte(pos);
            assertEquals("Mismatch at position " + pos, expected, actual);
        }

        // File pointer should still be 0 (no sequential reads done)
        assertEquals(0, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // Positional readBytes — spanning blocks
    // =========================================================================

    public void testPositionalReadBytesSpanningBlocks() throws IOException {
        int numBlocks = 3;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Read across block 0 → block 1 boundary
        int readLen = 64;
        long startPos = BLOCK_SIZE - 32;
        byte[] buf = new byte[readLen];
        input.readBytes(startPos, buf, 0, readLen);

        for (int i = 0; i < readLen; i++) {
            long fileOffset = startPos + i;
            assertEquals("Mismatch at offset " + fileOffset,
                (byte) (fileOffset & 0xFF), buf[i]);
        }

        // File pointer should still be 0
        assertEquals(0, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // Positional readBytes — zero length
    // =========================================================================

    public void testPositionalReadBytesZeroLength() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        // Zero-length read should be a no-op
        byte[] buf = new byte[10];
        input.readBytes(0, buf, 0, 0);
        assertEquals(0, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // readInts — within single block
    // =========================================================================

    public void testReadIntsWithinSingleBlock() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int numInts = 16;
        for (int i = 0; i < numInts; i++) {
            bb.putInt(i * Integer.BYTES, i * 100);
        }
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, data.length);

        int[] result = new int[numInts];
        input.readInts(result, 0, numInts);

        for (int i = 0; i < numInts; i++) {
            assertEquals("Mismatch at index " + i, i * 100, result[i]);
        }
        assertEquals(numInts * Integer.BYTES, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // readInts — cross-block fallback
    // =========================================================================

    public void testReadIntsCrossBlock() throws IOException {
        int numBlocks = 2;
        byte[] data = new byte[numBlocks * BLOCK_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Write ints spanning the block boundary
        int startOffset = BLOCK_SIZE - 2 * Integer.BYTES; // 2 ints before boundary
        int numInts = 4; // 2 in block 0, 2 in block 1
        for (int i = 0; i < numInts; i++) {
            bb.putInt(startOffset + i * Integer.BYTES, 1000 + i);
        }
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, data.length);

        // Skip to startOffset
        byte[] skip = new byte[startOffset];
        input.readBytes(skip, 0, startOffset);

        int[] result = new int[numInts];
        input.readInts(result, 0, numInts);

        for (int i = 0; i < numInts; i++) {
            assertEquals("Mismatch at index " + i, 1000 + i, result[i]);
        }

        registry.release(vfd);
    }

    // =========================================================================
    // readLongs — within single block
    // =========================================================================

    public void testReadLongsWithinSingleBlock() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int numLongs = 8;
        for (int i = 0; i < numLongs; i++) {
            bb.putLong(i * Long.BYTES, (long) i * 1_000_000L);
        }
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, data.length);

        long[] result = new long[numLongs];
        input.readLongs(result, 0, numLongs);

        for (int i = 0; i < numLongs; i++) {
            assertEquals("Mismatch at index " + i, (long) i * 1_000_000L, result[i]);
        }
        assertEquals(numLongs * Long.BYTES, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // readLongs — cross-block fallback
    // =========================================================================

    public void testReadLongsCrossBlock() throws IOException {
        int numBlocks = 2;
        byte[] data = new byte[numBlocks * BLOCK_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int startOffset = BLOCK_SIZE - Long.BYTES; // 1 long before boundary
        int numLongs = 2; // 1 in block 0, 1 in block 1
        for (int i = 0; i < numLongs; i++) {
            bb.putLong(startOffset + i * Long.BYTES, 9999L + i);
        }
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, data.length);

        byte[] skip = new byte[startOffset];
        input.readBytes(skip, 0, startOffset);

        long[] result = new long[numLongs];
        input.readLongs(result, 0, numLongs);

        for (int i = 0; i < numLongs; i++) {
            assertEquals("Mismatch at index " + i, 9999L + i, result[i]);
        }

        registry.release(vfd);
    }

    // =========================================================================
    // readFloats — within single block
    // =========================================================================

    public void testReadFloatsWithinSingleBlock() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int numFloats = 8;
        for (int i = 0; i < numFloats; i++) {
            bb.putFloat(i * Float.BYTES, 1.5f + i);
        }
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, data.length);

        float[] result = new float[numFloats];
        input.readFloats(result, 0, numFloats);

        for (int i = 0; i < numFloats; i++) {
            assertEquals("Mismatch at index " + i, 1.5f + i, result[i], 0.0001f);
        }
        assertEquals(numFloats * Float.BYTES, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // readFloats — cross-block fallback
    // =========================================================================

    public void testReadFloatsCrossBlock() throws IOException {
        int numBlocks = 2;
        byte[] data = new byte[numBlocks * BLOCK_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int startOffset = BLOCK_SIZE - Float.BYTES; // 1 float before boundary
        int numFloats = 2;
        for (int i = 0; i < numFloats; i++) {
            bb.putFloat(startOffset + i * Float.BYTES, 3.14f + i);
        }
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, data.length);

        byte[] skip = new byte[startOffset];
        input.readBytes(skip, 0, startOffset);

        float[] result = new float[numFloats];
        input.readFloats(result, 0, numFloats);

        for (int i = 0; i < numFloats; i++) {
            assertEquals("Mismatch at index " + i, 3.14f + i, result[i], 0.0001f);
        }

        registry.release(vfd);
    }

    // =========================================================================
    // readBytes — entire file as bulk read
    // =========================================================================

    public void testBulkReadEntireFile() throws IOException {
        int numBlocks = 3;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        byte[] buf = new byte[(int) fileLength];
        input.readBytes(buf, 0, (int) fileLength);

        for (int i = 0; i < fileLength; i++) {
            assertEquals("Mismatch at offset " + i, (byte) (i & 0xFF), buf[i]);
        }
        assertEquals(fileLength, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // readBytes with offset in destination array
    // =========================================================================

    public void testBulkReadWithDestinationOffset() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        byte[] buf = new byte[100];
        // Fill with sentinel
        java.util.Arrays.fill(buf, (byte) 0xAA);

        // Read 32 bytes starting at offset 10 in the destination
        input.readBytes(buf, 10, 32);

        // Sentinel before offset 10 should be untouched
        for (int i = 0; i < 10; i++) {
            assertEquals((byte) 0xAA, buf[i]);
        }
        // Read data
        for (int i = 0; i < 32; i++) {
            assertEquals((byte) (i & 0xFF), buf[10 + i]);
        }
        // Sentinel after should be untouched
        for (int i = 42; i < 100; i++) {
            assertEquals((byte) 0xAA, buf[i]);
        }

        registry.release(vfd);
    }

    // =========================================================================
    // Positional readByte — currentBlock fast path hit
    // =========================================================================

    public void testPositionalReadByteCurrentBlockFastPath() throws IOException {
        Path file = createTestFile(2);
        long fileLength = 2 * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Trigger block 0 load
        input.readByte();

        // Positional read within the same block should hit fast path
        for (int i = 0; i < 100; i++) {
            byte val = input.readByte(i);
            assertEquals((byte) (i & 0xFF), val);
        }

        // File pointer should be 1 (only the initial readByte advanced it)
        assertEquals(1, input.getFilePointer());

        registry.release(vfd);
    }
}
