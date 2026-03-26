/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Pluggable failure injection interface for the VFD layer.
 *
 * <p>Provides hooks at key points in the I/O path so that tests can
 * simulate specific failure modes (GC delays, channel close mid-read,
 * partial reads, slow I/O, I/O queueing) and verify the system handles
 * each one correctly without data corruption or resource leaks.</p>
 *
 * <p>In production, the injector is {@link NoOpFaultInjector#INSTANCE}
 * and all injection points are guarded by
 * {@link FaultInjectionConfig#FAULT_INJECTION_ENABLED}. When that flag
 * is {@code false}, HotSpot C2 eliminates all injection-point code via
 * dead-code elimination — zero overhead.</p>
 *
 * <p><b>Satisfies: Requirement 50</b></p>
 *
 * @opensearch.internal
 */
public interface FaultInjector {

    /**
     * Called before a physical I/O read is performed.
     *
     * @param vfdId      the VFD identifier
     * @param fileOffset the file offset being read
     * @throws IOException to simulate I/O errors or hangs
     */
    void beforeRead(long vfdId, long fileOffset) throws IOException;

    /**
     * Called after a physical I/O read completes successfully.
     *
     * @param vfdId      the VFD identifier
     * @param fileOffset the file offset that was read
     * @param buffer     the buffer containing the read data
     * @throws IOException to simulate post-read failures
     */
    void afterRead(long vfdId, long fileOffset, ByteBuffer buffer) throws IOException;

    /**
     * Called before acquiring a FileChannel via VfdRegistry.
     *
     * @param vfdId the VFD identifier
     */
    void beforeChannelAcquire(long vfdId);

    /**
     * Called after a FileChannel is closed (RefCountedChannel refCount → 0).
     *
     * @param vfdId the VFD identifier
     */
    void afterChannelClose(long vfdId);

    /**
     * Called before acquiring a buffer from the MemoryPool.
     */
    void beforePoolAcquire();

    /**
     * Called after a Cleaner callback replenishes the pool.
     */
    void afterCleanerCallback();
}
