/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

/**
 * Thin Panama FFI wrapper around POSIX {@code open(2)}, {@code pread(2)},
 * and {@code close(2)}. Bypasses Java's {@link java.nio.channels.FileChannel}
 * which enforces a 1&nbsp;MB max read size per call — wasteful for 8&nbsp;KB
 * block reads on EFS.
 *
 * <p>Requires Java 21+ with {@code --enable-preview} and
 * {@code --enable-native-access=ALL-UNNAMED}.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * int fd = NativeFileIO.open(path, NativeFileIO.O_RDONLY);
 * try {
 *     int n = NativeFileIO.pread(fd, buf, offset, length);
 * } finally {
 *     NativeFileIO.close(fd);
 * }
 * }</pre>
 */
public final class NativeFileIO {

    /** POSIX O_RDONLY — open for reading only. */
    public static final int O_RDONLY = 0;

    private static final Linker LINKER = Linker.nativeLinker();
    private static final MethodHandle OPEN_MH;
    private static final MethodHandle PREAD_MH;
    private static final MethodHandle CLOSE_MH;

    static {
        SymbolLookup stdlib = LINKER.defaultLookup();

        // int open(const char *path, int flags)
        OPEN_MH = LINKER.downcallHandle(
            stdlib.find("open").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)
        );

        // ssize_t pread(int fd, void *buf, size_t count, off_t offset)
        PREAD_MH = LINKER.downcallHandle(
            stdlib.find("pread").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        // int close(int fd)
        CLOSE_MH = LINKER.downcallHandle(
            stdlib.find("close").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT)
        );
    }

    private NativeFileIO() {}

    /**
     * Opens a file via POSIX {@code open(2)}.
     *
     * @param path  absolute file path
     * @param flags POSIX open flags (e.g. {@link #O_RDONLY})
     * @return file descriptor
     * @throws IOException if open returns -1
     */
    public static int open(String path, int flags) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cPath = arena.allocateUtf8String(path);
            int fd = (int) OPEN_MH.invokeExact(cPath, flags);
            if (fd == -1) {
                throw new IOException("open() failed for: " + path);
            }
            return fd;
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("open() invocation error for: " + path, t);
        }
    }

    /**
     * Reads via POSIX {@code pread(2)} directly into a direct {@link ByteBuffer}.
     *
     * @param fd     file descriptor from {@link #open}
     * @param dst    direct ByteBuffer to read into (from current position)
     * @param offset file offset to read from
     * @param length number of bytes to read
     * @return number of bytes actually read
     * @throws IOException if pread returns a negative value
     */
    public static int pread(int fd, ByteBuffer dst, long offset, int length) throws IOException {
        try {
            MemorySegment seg = MemorySegment.ofBuffer(dst);
            // Slice from current position so pread writes at the right spot
            MemorySegment target = seg.asSlice(dst.position(), length);
            long n = (long) PREAD_MH.invokeExact(fd, target, (long) length, offset);
            if (n < 0) {
                throw new IOException("pread() failed: fd=" + fd + " offset=" + offset + " length=" + length);
            }
            dst.position(dst.position() + (int) n);
            return (int) n;
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("pread() invocation error: fd=" + fd, t);
        }
    }

    /**
     * Closes a file descriptor via POSIX {@code close(2)}.
     *
     * @param fd file descriptor to close
     * @throws IOException if close returns -1
     */
    public static void close(int fd) throws IOException {
        try {
            int rc = (int) CLOSE_MH.invokeExact(fd);
            if (rc == -1) {
                throw new IOException("close() failed: fd=" + fd);
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("close() invocation error: fd=" + fd, t);
        }
    }
}
