/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.nio.ByteBuffer;

/**
 * No-op implementation of {@link FaultInjector} for production use.
 *
 * <p>All methods are empty. When guarded by
 * {@link FaultInjectionConfig#FAULT_INJECTION_ENABLED} (which is
 * {@code false} by default), HotSpot C2 eliminates all call sites
 * via dead-code elimination — zero overhead in production.</p>
 *
 * <p><b>Satisfies: Requirement 50</b></p>
 *
 * @opensearch.internal
 */
public final class NoOpFaultInjector implements FaultInjector {

    /** Singleton instance. */
    public static final NoOpFaultInjector INSTANCE = new NoOpFaultInjector();

    private NoOpFaultInjector() {}

    @Override
    public void beforeRead(long vfdId, long fileOffset) {}

    @Override
    public void afterRead(long vfdId, long fileOffset, ByteBuffer buffer) {}

    @Override
    public void beforeChannelAcquire(long vfdId) {}

    @Override
    public void afterChannelClose(long vfdId) {}

    @Override
    public void beforePoolAcquire() {}

    @Override
    public void afterCleanerCallback() {}
}
