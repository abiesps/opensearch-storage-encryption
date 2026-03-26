/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Default {@link IOBackend} implementation that uses
 * {@link FileChannel#read(ByteBuffer, long)} positional reads with a
 * short-read retry loop.
 *
 * <p>FileChannel positional reads may return fewer bytes than requested
 * (short reads) — this is normal OS behaviour, especially on network
 * file systems like NFS/EFS. The retry loop continues reading until
 * either the requested {@code length} bytes have been read or EOF is
 * encountered.</p>
 *
 * <p><b>Satisfies: Requirements 6, 8</b></p>
 */
public class FileChannelIOBackend implements IOBackend {

    @Override
    public int read(FileChannel channel, ByteBuffer dst, long fileOffset, int length) throws IOException {
        dst.clear().limit(length);
        int totalRead = 0;
        while (totalRead < length) {
            int n = channel.read(dst, fileOffset + totalRead);
            if (n == -1) break; // EOF
            totalRead += n;
        }
        return totalRead;
    }
}
