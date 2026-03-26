/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/**
 * Common interface for read channels used by {@link MemorySegmentPoolIndexInput}.
 * {@link VirtualFileChannel} implements this interface and handles both plain
 * and encrypted reads via the pluggable {@link BlockDecryptor} callback.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
public interface ReadChannel {

    /**
     * Read a single block from the file.
     *
     * @param vfdId       the VFD identifier
     * @param blockId     the block identifier within the file
     * @param fileOffset  the absolute byte offset in the file
     * @param isDemandRead true for demand reads, false for prefetch
     * @return the MemorySegment containing the block data
     * @throws IOException if the read fails
     */
    MemorySegment readBlock(long vfdId, int blockId, long fileOffset,
                            boolean isDemandRead) throws IOException;

    /**
     * Prefetch blocks asynchronously.
     *
     * @param vfdId      the VFD identifier
     * @param fileOffset the starting file offset
     * @param blockCount number of blocks to prefetch
     */
    void prefetch(long vfdId, long fileOffset, int blockCount);
}
