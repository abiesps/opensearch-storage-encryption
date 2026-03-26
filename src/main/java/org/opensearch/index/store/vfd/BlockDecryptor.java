/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/**
 * Pluggable decryption callback invoked by {@link VirtualFileChannel}
 * after physical I/O completes but before the data is cached in L1/L2.
 *
 * <p>When encryption is enabled, the implementation decrypts the raw
 * ciphertext in-place on the writable MemorySegment. When encryption is
 * disabled, {@link #NOOP} is used (zero overhead — the JIT eliminates
 * the call entirely).</p>
 *
 * <p>This ensures blocks stored in L1 (RadixBlockTable) and L2
 * (BlockCache) are always plaintext — no post-cache decryption needed.</p>
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@FunctionalInterface
public interface BlockDecryptor {

    /** No-op decryptor for unencrypted files. */
    BlockDecryptor NOOP = (segment, fileOffset) -> {};

    /**
     * Decrypt the segment contents in-place.
     *
     * @param segment    writable MemorySegment containing ciphertext
     * @param fileOffset the absolute file offset this data was read from
     * @throws IOException if decryption fails
     */
    void decrypt(MemorySegment segment, long fileOffset) throws IOException;
}
