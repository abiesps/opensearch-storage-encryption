/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Pluggable I/O backend interface for VirtualFileChannel. Abstracts the
 * physical read strategy so that FileChannel (POC) can be replaced with
 * io_uring or other backends without changing the read API.
 *
 * <p><b>Satisfies: Requirements 6, 8</b></p>
 */
public interface IOBackend {

    /**
     * Perform a positional read from the given channel into {@code dst}.
     *
     * <p>The implementation MUST handle short reads (where a single
     * {@code channel.read()} returns fewer bytes than requested) by
     * retrying until either {@code length} bytes have been read or EOF
     * is reached.</p>
     *
     * @param channel    the file channel to read from
     * @param dst        destination buffer; the implementation will
     *                   {@code clear()} and {@code limit(length)} before use
     * @param fileOffset the absolute byte offset in the file to start reading
     * @param length     the number of bytes to read
     * @return the total number of bytes actually read (may be less than
     *         {@code length} if EOF is reached)
     * @throws IOException if an I/O error occurs
     */
    int read(FileChannel channel, ByteBuffer dst, long fileOffset, int length) throws IOException;
}
