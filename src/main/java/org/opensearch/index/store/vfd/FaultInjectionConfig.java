/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

/**
 * Configuration for the fault injection subsystem.
 *
 * <p>The {@link #FAULT_INJECTION_ENABLED} flag is a {@code static final boolean}
 * read from the system property {@code vfd.fault.injection.enabled} at class-load
 * time. When {@code false} (the default), HotSpot C2 treats all injection-point
 * guards as dead code and eliminates them entirely — zero overhead in production.</p>
 *
 * <p>To enable fault injection for testing, start the JVM with:
 * {@code -Dvfd.fault.injection.enabled=true}</p>
 *
 * <p><b>Satisfies: Requirement 50</b></p>
 *
 * @opensearch.internal
 */
public final class FaultInjectionConfig {

    /**
     * When {@code false} (default), all fault injection call sites are
     * eliminated by the JIT via dead-code elimination. Set to {@code true}
     * via {@code -Dvfd.fault.injection.enabled=true} for testing.
     */
    public static final boolean FAULT_INJECTION_ENABLED =
        Boolean.getBoolean("vfd.fault.injection.enabled");

    private FaultInjectionConfig() {}
}
