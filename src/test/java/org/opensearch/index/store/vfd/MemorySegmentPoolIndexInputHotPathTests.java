/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.EOFException;
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
 * Unit tests for {@link MemorySegmentPoolIndexInput} hot path reads:
 * readByte, readShort, readInt, readLong, switchBlock, cross-block
 * boundary reads, and EOF detection.
 *
 * <p><b>Validates: Requirements 14, 15, 38</b></p>
 */
@SuppressWarnings("preview")
public class MemorySegmentPoolIndexInputHotPathTests extends OpenSearchTestCase {

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
        Path file = createTempFile("hotpath-test", ".dat");
        Files.write(file, data);
        return file;
    }

    /**
     * Create a test file with specific byte content.
     */
    private Path createTestFileWithData(byte[] data) throws IOException {
        Path file = createTempFile("hotpath-test", ".dat");
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
    // readByte — sequential read through entire file
    // =========================================================================

    public void testSequentialReadByteEntireFile() throws IOException {
        int numBlocks = 3;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        for (long i = 0; i < fileLength; i++) {
            byte expected = (byte) (i & 0xFF);
            byte actual = input.readByte();
            assertEquals("Mismatch at position " + i, expected, actual);
        }

        assertEquals(fileLength, input.getFilePointer());
        registry.release(vfd);
    }

    public void testReadByteSingleBlock() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        // Read first few bytes
        assertEquals((byte) 0, input.readByte());
        assertEquals((byte) 1, input.readByte());
        assertEquals((byte) 2, input.readByte());
        assertEquals(3, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // readByte — block switch (crosses block boundary)
    // =========================================================================

    public void testReadByteCrossesBlockBoundary() throws IOException {
        int numBlocks = 2;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Read last byte of block 0
        for (int i = 0; i < BLOCK_SIZE - 1; i++) {
            input.readByte();
        }
        byte lastOfBlock0 = input.readByte();
        assertEquals((byte) ((BLOCK_SIZE - 1) & 0xFF), lastOfBlock0);

        // Read first byte of block 1 — triggers switchBlock
        byte firstOfBlock1 = input.readByte();
        assertEquals((byte) (BLOCK_SIZE & 0xFF), firstOfBlock1);

        registry.release(vfd);
    }

    // =========================================================================
    // readByte — EOF detection
    // =========================================================================

    public void testReadByteThrowsEOFAtEnd() throws IOException {
        Path file = createTestFile(1);
        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        // Read all bytes
        for (int i = 0; i < BLOCK_SIZE; i++) {
            input.readByte();
        }

        // Next read should throw EOFException
        expectThrows(EOFException.class, input::readByte);

        registry.release(vfd);
    }

    // =========================================================================
    // readShort — fast path (within block)
    // =========================================================================

    public void testReadShortWithinBlock() throws IOException {
        // Create file with known little-endian short at offset 0
        byte[] data = new byte[BLOCK_SIZE];
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        buf.putShort(0, (short) 0x1234);
        buf.putShort(2, (short) -1);
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        assertEquals((short) 0x1234, input.readShort());
        assertEquals((short) -1, input.readShort());
        assertEquals(4, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // readShort — cross-block boundary
    // =========================================================================

    public void testReadShortCrossBlockBoundary() throws IOException {
        int numBlocks = 2;
        byte[] data = new byte[numBlocks * BLOCK_SIZE];
        // Place a short that spans the block boundary:
        // low byte at end of block 0, high byte at start of block 1
        data[BLOCK_SIZE - 1] = (byte) 0x78; // low byte
        data[BLOCK_SIZE] = (byte) 0x56;     // high byte
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, data.length);

        // Advance to BLOCK_SIZE - 1
        for (int i = 0; i < BLOCK_SIZE - 1; i++) {
            input.readByte();
        }

        // Read short spanning boundary — little-endian: 0x78 | (0x56 << 8) = 0x5678
        short value = input.readShort();
        assertEquals((short) 0x5678, value);

        registry.release(vfd);
    }

    // =========================================================================
    // readInt — fast path (within block)
    // =========================================================================

    public void testReadIntWithinBlock() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(0, 0xDEADBEEF);
        buf.putInt(4, 42);
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        assertEquals(0xDEADBEEF, input.readInt());
        assertEquals(42, input.readInt());
        assertEquals(8, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // readInt — cross-block boundary
    // =========================================================================

    public void testReadIntCrossBlockBoundary() throws IOException {
        int numBlocks = 2;
        byte[] data = new byte[numBlocks * BLOCK_SIZE];
        // Place an int spanning the boundary: 2 bytes in block 0, 2 bytes in block 1
        // Little-endian 0x04030201: bytes are 01, 02, 03, 04
        data[BLOCK_SIZE - 2] = 0x01;
        data[BLOCK_SIZE - 1] = 0x02;
        data[BLOCK_SIZE]     = 0x03;
        data[BLOCK_SIZE + 1] = 0x04;
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, data.length);

        // Advance to BLOCK_SIZE - 2
        for (int i = 0; i < BLOCK_SIZE - 2; i++) {
            input.readByte();
        }

        int value = input.readInt();
        assertEquals(0x04030201, value);

        registry.release(vfd);
    }

    // =========================================================================
    // readLong — fast path (within block)
    // =========================================================================

    public void testReadLongWithinBlock() throws IOException {
        byte[] data = new byte[BLOCK_SIZE];
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(0, 0x123456789ABCDEF0L);
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, BLOCK_SIZE);

        assertEquals(0x123456789ABCDEF0L, input.readLong());
        assertEquals(8, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // readLong — cross-block boundary
    // =========================================================================

    public void testReadLongCrossBlockBoundary() throws IOException {
        int numBlocks = 2;
        byte[] data = new byte[numBlocks * BLOCK_SIZE];
        // Place a long spanning the boundary: 3 bytes in block 0, 5 bytes in block 1
        // Little-endian 0x0807060504030201
        data[BLOCK_SIZE - 3] = 0x01;
        data[BLOCK_SIZE - 2] = 0x02;
        data[BLOCK_SIZE - 1] = 0x03;
        data[BLOCK_SIZE]     = 0x04;
        data[BLOCK_SIZE + 1] = 0x05;
        data[BLOCK_SIZE + 2] = 0x06;
        data[BLOCK_SIZE + 3] = 0x07;
        data[BLOCK_SIZE + 4] = 0x08;
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, data.length);

        // Advance to BLOCK_SIZE - 3
        for (int i = 0; i < BLOCK_SIZE - 3; i++) {
            input.readByte();
        }

        long value = input.readLong();
        assertEquals(0x0807060504030201L, value);

        registry.release(vfd);
    }

    // =========================================================================
    // switchBlock — L1 cache hit
    // =========================================================================

    public void testSwitchBlockL1CacheHit() throws IOException {
        int numBlocks = 2;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Read first byte to trigger block 0 load into L1
        input.readByte();

        // Read all of block 0 and first byte of block 1
        for (int i = 1; i < BLOCK_SIZE; i++) {
            input.readByte();
        }
        // This triggers switchBlock for block 1
        byte firstOfBlock1 = input.readByte();
        assertEquals((byte) (BLOCK_SIZE & 0xFF), firstOfBlock1);

        // Now the L1 should have block 1. Verify by checking physical I/O count
        // is reasonable (at most 2 physical reads for 2 blocks)
        assertTrue(channel.metrics().physicalIOCount.sum() <= 2);

        registry.release(vfd);
    }

    // =========================================================================
    // switchBlock — L2 cache hit (promote to L1)
    // =========================================================================

    public void testSwitchBlockL2CacheHitPromotesToL1() throws IOException {
        int numBlocks = 2;
        Path file = createTestFile(numBlocks);
        long fileLength = numBlocks * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);

        // Pre-populate L2 cache for block 1 with known data
        byte[] block1Data = new byte[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            block1Data[i] = (byte) 0xBB;
        }
        ByteBuffer buf = ByteBuffer.allocateDirect(BLOCK_SIZE);
        buf.put(block1Data);
        buf.flip();
        MemorySegment preloaded = MemorySegment.ofBuffer(buf.asReadOnlyBuffer());
        long key = VirtualPage.encode(vfd.id(), 1);
        blockCache.put(key, preloaded);

        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        // Skip to block 1
        for (int i = 0; i < BLOCK_SIZE; i++) {
            input.readByte();
        }

        // Read from block 1 — should hit L2 and promote to L1
        byte val = input.readByte();
        assertEquals((byte) 0xBB, val);

        // Verify L1 now has block 1
        var l1 = registry.getRadixBlockTable(vfd.id());
        assertNotNull(l1.get(1));

        registry.release(vfd);
    }

    // =========================================================================
    // getFilePointer tracks position correctly
    // =========================================================================

    public void testGetFilePointerTracksPosition() throws IOException {
        Path file = createTestFile(2);
        long fileLength = 2 * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        assertEquals(0, input.getFilePointer());

        input.readByte();
        assertEquals(1, input.getFilePointer());

        input.readShort();
        assertEquals(3, input.getFilePointer());

        input.readInt();
        assertEquals(7, input.getFilePointer());

        input.readLong();
        assertEquals(15, input.getFilePointer());

        registry.release(vfd);
    }

    // =========================================================================
    // length() returns correct value
    // =========================================================================

    public void testLengthReturnsSliceLength() throws IOException {
        Path file = createTestFile(2);
        long fileLength = 2 * BLOCK_SIZE;

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, fileLength);

        assertEquals(fileLength, input.length());

        registry.release(vfd);
    }

    // =========================================================================
    // readInt/readLong at various cross-boundary offsets
    // =========================================================================

    public void testReadIntAtEveryBoundaryOffset() throws IOException {
        // Test readInt with the int starting at each of the 4 possible
        // offsets relative to the block boundary
        for (int offset = 1; offset <= 3; offset++) {
            int numBlocks = 2;
            byte[] data = new byte[numBlocks * BLOCK_SIZE];
            // Write a known int at BLOCK_SIZE - offset (little-endian)
            int pos = BLOCK_SIZE - offset;
            data[pos]     = 0x11;
            data[pos + 1] = 0x22;
            data[pos + 2] = 0x33;
            data[pos + 3] = 0x44;
            Path file = createTestFileWithData(data);

            VirtualFileDescriptor vfd = registry.acquire(file);
            VirtualFileChannel channel = createChannel(vfd);
            MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, data.length);

            // Advance to the position
            for (int i = 0; i < pos; i++) {
                input.readByte();
            }

            int value = input.readInt();
            assertEquals("Failed at boundary offset " + offset,
                0x44332211, value);

            registry.release(vfd);
        }
    }

    public void testReadLongAtEveryBoundaryOffset() throws IOException {
        // Test readLong with the long starting at each of the 7 possible
        // offsets relative to the block boundary
        for (int offset = 1; offset <= 7; offset++) {
            int numBlocks = 2;
            byte[] data = new byte[numBlocks * BLOCK_SIZE];
            int pos = BLOCK_SIZE - offset;
            // Little-endian 0x0807060504030201
            for (int b = 0; b < 8; b++) {
                data[pos + b] = (byte) (b + 1);
            }
            Path file = createTestFileWithData(data);

            VirtualFileDescriptor vfd = registry.acquire(file);
            VirtualFileChannel channel = createChannel(vfd);
            MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, data.length);

            for (int i = 0; i < pos; i++) {
                input.readByte();
            }

            long value = input.readLong();
            assertEquals("Failed at boundary offset " + offset,
                0x0807060504030201L, value);

            registry.release(vfd);
        }
    }

    // =========================================================================
    // Small file (less than one block)
    // =========================================================================

    public void testSmallFileLessThanOneBlock() throws IOException {
        byte[] data = new byte[100];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i * 3);
        }
        Path file = createTestFileWithData(data);

        VirtualFileDescriptor vfd = registry.acquire(file);
        VirtualFileChannel channel = createChannel(vfd);
        MemorySegmentPoolIndexInput input = createIndexInput(vfd, channel, data.length);

        for (int i = 0; i < data.length; i++) {
            assertEquals((byte) (i * 3), input.readByte());
        }

        expectThrows(EOFException.class, input::readByte);

        registry.release(vfd);
    }
}
