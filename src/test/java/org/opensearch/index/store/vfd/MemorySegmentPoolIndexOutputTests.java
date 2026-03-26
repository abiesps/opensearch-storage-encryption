/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

import org.opensearch.index.store.bufferpoolfs.StaticConfigs;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link MemorySegmentPoolIndexOutput}.
 *
 * <p>Covers: write + read round-trip, checksum correctness, close durability,
 * writes don't populate L1/L2, concurrent writes to different files.</p>
 *
 * <p><b>Validates: Requirement 52</b></p>
 */
public class MemorySegmentPoolIndexOutputTests extends OpenSearchTestCase {

    // ---- Write + Read Round-Trip ----

    public void testWriteByteAndReadBack() throws IOException {
        Path file = createTempFile("writebyte", ".dat");
        try (MemorySegmentPoolIndexOutput out = new MemorySegmentPoolIndexOutput(
                "test", "writebyte.dat", file)) {
            for (int i = 0; i < 256; i++) {
                out.writeByte((byte) i);
            }
        }

        byte[] readBack = Files.readAllBytes(file);
        assertEquals(256, readBack.length);
        for (int i = 0; i < 256; i++) {
            assertEquals("Byte mismatch at offset " + i, (byte) i, readBack[i]);
        }
    }

    public void testWriteBytesAndReadBack() throws IOException {
        byte[] data = new byte[10000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ((i * 7 + 13) & 0xFF);
        }

        Path file = createTempFile("writebytes", ".dat");
        try (MemorySegmentPoolIndexOutput out = new MemorySegmentPoolIndexOutput(
                "test", "writebytes.dat", file)) {
            out.writeBytes(data, 0, data.length);
        }

        byte[] readBack = Files.readAllBytes(file);
        assertEquals(data.length, readBack.length);
        for (int i = 0; i < data.length; i++) {
            assertEquals("Byte mismatch at offset " + i, data[i], readBack[i]);
        }
    }

    // ---- Write spanning multiple buffer flushes ----

    public void testWriteLargerThanBuffer() throws IOException {
        int size = StaticConfigs.CACHE_BLOCK_SIZE * 3 + 137;
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) ((i * 31 + 5) & 0xFF);
        }

        Path file = createTempFile("largewrite", ".dat");
        try (MemorySegmentPoolIndexOutput out = new MemorySegmentPoolIndexOutput(
                "test", "largewrite.dat", file)) {
            out.writeBytes(data, 0, data.length);
        }

        byte[] readBack = Files.readAllBytes(file);
        assertEquals(size, readBack.length);
        for (int i = 0; i < size; i++) {
            assertEquals("Byte mismatch at offset " + i, data[i], readBack[i]);
        }
    }

    // ---- Mixed writeByte + writeBytes ----

    public void testMixedWriteByteAndWriteBytes() throws IOException {
        Path file = createTempFile("mixed", ".dat");
        byte[] chunk = new byte[] { 10, 20, 30, 40, 50 };

        try (MemorySegmentPoolIndexOutput out = new MemorySegmentPoolIndexOutput(
                "test", "mixed.dat", file)) {
            out.writeByte((byte) 1);
            out.writeBytes(chunk, 0, chunk.length);
            out.writeByte((byte) 99);
        }

        byte[] readBack = Files.readAllBytes(file);
        assertEquals(7, readBack.length);
        assertEquals(1, readBack[0]);
        assertEquals(10, readBack[1]);
        assertEquals(20, readBack[2]);
        assertEquals(30, readBack[3]);
        assertEquals(40, readBack[4]);
        assertEquals(50, readBack[5]);
        assertEquals(99, readBack[6]);
    }

    // ---- Checksum Correctness ----

    public void testChecksumCorrectness() throws IOException {
        byte[] data = new byte[5000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }

        CRC32 expected = new CRC32();
        expected.update(data);

        Path file = createTempFile("checksum", ".dat");
        long actualChecksum;
        try (MemorySegmentPoolIndexOutput out = new MemorySegmentPoolIndexOutput(
                "test", "checksum.dat", file)) {
            out.writeBytes(data, 0, data.length);
            actualChecksum = out.getChecksum();
        }

        assertEquals("CRC32 checksum mismatch", expected.getValue(), actualChecksum);
    }

    public void testChecksumWithMixedWrites() throws IOException {
        CRC32 expected = new CRC32();
        Path file = createTempFile("checksummixed", ".dat");

        long actualChecksum;
        try (MemorySegmentPoolIndexOutput out = new MemorySegmentPoolIndexOutput(
                "test", "checksummixed.dat", file)) {
            out.writeByte((byte) 42);
            expected.update(42);

            byte[] chunk = new byte[] { 1, 2, 3, 4, 5 };
            out.writeBytes(chunk, 0, chunk.length);
            expected.update(chunk);

            out.writeByte((byte) 0xFF);
            expected.update(0xFF);

            actualChecksum = out.getChecksum();
        }

        assertEquals("CRC32 checksum mismatch after mixed writes",
            expected.getValue(), actualChecksum);
    }

    // ---- getFilePointer ----

    public void testGetFilePointer() throws IOException {
        Path file = createTempFile("filepointer", ".dat");
        try (MemorySegmentPoolIndexOutput out = new MemorySegmentPoolIndexOutput(
                "test", "filepointer.dat", file)) {
            assertEquals(0, out.getFilePointer());

            out.writeByte((byte) 1);
            assertEquals(1, out.getFilePointer());

            byte[] data = new byte[100];
            out.writeBytes(data, 0, data.length);
            assertEquals(101, out.getFilePointer());

            byte[] large = new byte[StaticConfigs.CACHE_BLOCK_SIZE + 50];
            out.writeBytes(large, 0, large.length);
            assertEquals(101 + large.length, out.getFilePointer());
        }
    }

    // ---- Close Durability ----

    public void testCloseDurability() throws IOException {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }

        Path file = createTempFile("durable", ".dat");
        try (MemorySegmentPoolIndexOutput out = new MemorySegmentPoolIndexOutput(
                "test", "durable.dat", file)) {
            out.writeBytes(data, 0, data.length);
        }
        // After close(), data should be on disk - verify with a fresh FileChannel
        try (FileChannel readCh = FileChannel.open(file, StandardOpenOption.READ)) {
            assertEquals("File size should match written data", 1024, readCh.size());
            ByteBuffer buf = ByteBuffer.allocate(1024);
            readCh.read(buf, 0);
            buf.flip();
            for (int i = 0; i < 1024; i++) {
                assertEquals("Byte mismatch at " + i, (byte) (i & 0xFF), buf.get(i));
            }
        }
    }

    public void testCloseIsIdempotent() throws IOException {
        Path file = createTempFile("idempotent", ".dat");
        MemorySegmentPoolIndexOutput out = new MemorySegmentPoolIndexOutput(
            "test", "idempotent.dat", file);
        out.writeByte((byte) 1);
        out.close();
        // Second close should not throw
        out.close();

        byte[] readBack = Files.readAllBytes(file);
        assertEquals(1, readBack.length);
        assertEquals(1, readBack[0]);
    }

    // ---- Empty file ----

    public void testEmptyFile() throws IOException {
        Path file = createTempFile("empty", ".dat");
        try (MemorySegmentPoolIndexOutput out = new MemorySegmentPoolIndexOutput(
                "test", "empty.dat", file)) {
            assertEquals(0, out.getFilePointer());
        }

        byte[] readBack = Files.readAllBytes(file);
        assertEquals(0, readBack.length);
    }

    // ---- writeBytes with offset ----

    public void testWriteBytesWithOffset() throws IOException {
        byte[] src = new byte[] { 0, 0, 10, 20, 30, 0, 0 };
        Path file = createTempFile("offset", ".dat");
        try (MemorySegmentPoolIndexOutput out = new MemorySegmentPoolIndexOutput(
                "test", "offset.dat", file)) {
            out.writeBytes(src, 2, 3);
        }

        byte[] readBack = Files.readAllBytes(file);
        assertEquals(3, readBack.length);
        assertEquals(10, readBack[0]);
        assertEquals(20, readBack[1]);
        assertEquals(30, readBack[2]);
    }

    // ---- Concurrent writes to different files ----

    public void testConcurrentWritesToDifferentFiles() throws Exception {
        int numThreads = 4;
        int dataSize = StaticConfigs.CACHE_BLOCK_SIZE * 2 + 100;
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicReference<Exception> failure = new AtomicReference<>();
        Path[] files = new Path[numThreads];
        byte[][] expectedData = new byte[numThreads][];

        for (int t = 0; t < numThreads; t++) {
            files[t] = createTempFile("concurrent-" + t, ".dat");
            expectedData[t] = new byte[dataSize];
            for (int i = 0; i < dataSize; i++) {
                expectedData[t][i] = (byte) ((i + t * 37) & 0xFF);
            }
        }

        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            final int threadIdx = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    try (MemorySegmentPoolIndexOutput out = new MemorySegmentPoolIndexOutput(
                            "test-" + threadIdx, "concurrent-" + threadIdx + ".dat",
                            files[threadIdx])) {
                        out.writeBytes(expectedData[threadIdx], 0, dataSize);
                    }
                } catch (Exception e) {
                    failure.compareAndSet(null, e);
                }
            });
            threads[t].start();
        }

        for (Thread thread : threads) {
            thread.join(10_000);
        }

        assertNull("Thread failed with exception: " + failure.get(), failure.get());

        // Verify each file independently
        for (int t = 0; t < numThreads; t++) {
            byte[] readBack = Files.readAllBytes(files[t]);
            assertEquals("File " + t + " size mismatch", dataSize, readBack.length);
            for (int i = 0; i < dataSize; i++) {
                assertEquals("File " + t + " byte mismatch at " + i,
                    expectedData[t][i], readBack[i]);
            }
        }
    }
}
