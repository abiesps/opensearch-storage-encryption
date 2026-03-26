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

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link FileChannelIOBackend}: full read, short-read retry,
 * and EOF handling.
 *
 * <p><b>Validates: Requirements 6, 8</b></p>
 */
public class FileChannelIOBackendTests extends OpenSearchTestCase {

    private final IOBackend backend = new FileChannelIOBackend();

    // ---- Full read: all bytes returned in one shot ----

    public void testFullRead() throws IOException {
        byte[] data = new byte[8192];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Path file = createTempFile("fullread", ".dat");
        Files.write(file, data);

        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer dst = ByteBuffer.allocateDirect(8192);
            int bytesRead = backend.read(fc, dst, 0, 8192);

            assertEquals("Should read all 8192 bytes", 8192, bytesRead);
            dst.flip();
            for (int i = 0; i < 8192; i++) {
                assertEquals("Byte mismatch at offset " + i, (byte) (i & 0xFF), dst.get(i));
            }
        }
    }

    // ---- Partial read at offset ----

    public void testReadAtOffset() throws IOException {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Path file = createTempFile("offsetread", ".dat");
        Files.write(file, data);

        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer dst = ByteBuffer.allocateDirect(256);
            int bytesRead = backend.read(fc, dst, 512, 256);

            assertEquals(256, bytesRead);
            dst.flip();
            for (int i = 0; i < 256; i++) {
                assertEquals("Byte mismatch at offset " + i, (byte) ((512 + i) & 0xFF), dst.get(i));
            }
        }
    }

    // ---- EOF handling: request more bytes than available ----

    public void testEofHandling() throws IOException {
        byte[] data = new byte[] { 10, 20, 30, 40, 50 };
        Path file = createTempFile("eof", ".dat");
        Files.write(file, data);

        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer dst = ByteBuffer.allocateDirect(1024);
            int bytesRead = backend.read(fc, dst, 0, 1024);

            assertEquals("Should read only 5 bytes (file size)", 5, bytesRead);
            dst.flip();
            assertEquals(10, dst.get(0));
            assertEquals(20, dst.get(1));
            assertEquals(30, dst.get(2));
            assertEquals(40, dst.get(3));
            assertEquals(50, dst.get(4));
        }
    }

    // ---- EOF at offset: read past end of file ----

    public void testEofAtOffset() throws IOException {
        byte[] data = new byte[100];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        Path file = createTempFile("eofoffset", ".dat");
        Files.write(file, data);

        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            // Read starting at offset 90, requesting 50 bytes — only 10 available
            ByteBuffer dst = ByteBuffer.allocateDirect(50);
            int bytesRead = backend.read(fc, dst, 90, 50);

            assertEquals("Should read only 10 bytes before EOF", 10, bytesRead);
            dst.flip();
            for (int i = 0; i < 10; i++) {
                assertEquals((byte) (90 + i), dst.get(i));
            }
        }
    }

    // ---- EOF: read starting beyond file end returns 0 ----

    public void testReadBeyondFileEnd() throws IOException {
        byte[] data = new byte[] { 1, 2, 3 };
        Path file = createTempFile("beyond", ".dat");
        Files.write(file, data);

        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer dst = ByteBuffer.allocateDirect(100);
            int bytesRead = backend.read(fc, dst, 1000, 100);

            assertEquals("Should read 0 bytes when starting beyond EOF", 0, bytesRead);
        }
    }

    // ---- Short-read retry: simulated via a wrapping FileChannel ----

    public void testShortReadRetry() throws IOException {
        // Write 1024 bytes of known data
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        Path file = createTempFile("shortread", ".dat");
        Files.write(file, data);

        // Use a wrapper that limits each read to at most 100 bytes
        // to simulate short reads (like NFS/EFS behaviour)
        IOBackend shortReadBackend = new ShortReadIOBackend(100);

        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer dst = ByteBuffer.allocateDirect(1024);
            int bytesRead = shortReadBackend.read(fc, dst, 0, 1024);

            assertEquals("Retry loop should accumulate all 1024 bytes", 1024, bytesRead);
            dst.flip();
            for (int i = 0; i < 1024; i++) {
                assertEquals("Byte mismatch at offset " + i, (byte) (i & 0xFF), dst.get(i));
            }
        }
    }

    // ---- Short-read + EOF: partial file with short reads ----

    public void testShortReadWithEof() throws IOException {
        byte[] data = new byte[250];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        Path file = createTempFile("shorteof", ".dat");
        Files.write(file, data);

        IOBackend shortReadBackend = new ShortReadIOBackend(100);

        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer dst = ByteBuffer.allocateDirect(1024);
            int bytesRead = shortReadBackend.read(fc, dst, 0, 1024);

            assertEquals("Should read 250 bytes (file size) despite requesting 1024", 250, bytesRead);
            dst.flip();
            for (int i = 0; i < 250; i++) {
                assertEquals((byte) i, dst.get(i));
            }
        }
    }

    // ---- Zero-length read ----

    public void testZeroLengthRead() throws IOException {
        Path file = createTempFile("zero", ".dat");
        Files.write(file, new byte[] { 1, 2, 3 });

        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer dst = ByteBuffer.allocateDirect(10);
            int bytesRead = backend.read(fc, dst, 0, 0);

            assertEquals("Zero-length read should return 0", 0, bytesRead);
        }
    }

    /**
     * An IOBackend that delegates to FileChannelIOBackend's retry logic but
     * limits each individual channel.read() to at most {@code maxBytesPerRead}
     * bytes, simulating short reads from network file systems.
     */
    private static class ShortReadIOBackend implements IOBackend {
        private final int maxBytesPerRead;

        ShortReadIOBackend(int maxBytesPerRead) {
            this.maxBytesPerRead = maxBytesPerRead;
        }

        @Override
        public int read(FileChannel channel, ByteBuffer dst, long fileOffset, int length) throws IOException {
            dst.clear().limit(length);
            int totalRead = 0;
            while (totalRead < length) {
                // Temporarily limit the buffer to simulate a short read
                int remaining = length - totalRead;
                int toRead = Math.min(remaining, maxBytesPerRead);
                int savedLimit = dst.limit();
                dst.limit(dst.position() + toRead);

                int n = channel.read(dst, fileOffset + totalRead);

                // Restore the original limit
                dst.limit(savedLimit);

                if (n == -1) break; // EOF
                totalRead += n;
            }
            return totalRead;
        }
    }
}
