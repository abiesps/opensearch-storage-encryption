/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for VirtualFileDescriptorRegistry edge cases.
 *
 * Feature: bufferpool-v2-memory-management
 */
class VirtualFileDescriptorRegistryTests {

    @AfterEach
    void cleanup() {
        VirtualFileDescriptorRegistry.getInstance().clear();
    }

    /**
     * Test singleton behavior — getInstance() always returns the same instance.
     * Validates: Requirement 1.1
     */
    @Test
    void singletonReturnsSameInstance() {
        VirtualFileDescriptorRegistry a = VirtualFileDescriptorRegistry.getInstance();
        VirtualFileDescriptorRegistry b = VirtualFileDescriptorRegistry.getInstance();
        assertSame(a, b, "getInstance() must return the same singleton instance");
    }

    /**
     * Test overflow detection: set counter near Integer.MAX_VALUE,
     * verify IllegalStateException on wrap.
     * Validates: Requirement 1.7
     */
    @Test
    void overflowThrowsIllegalStateException() throws Exception {
        VirtualFileDescriptorRegistry registry = VirtualFileDescriptorRegistry.getInstance();

        // Use reflection to set the AtomicInteger counter near MAX_VALUE
        Field nextVfdField = VirtualFileDescriptorRegistry.class.getDeclaredField("nextVfd");
        nextVfdField.setAccessible(true);
        AtomicInteger nextVfd = (AtomicInteger) nextVfdField.get(registry);

        // Set counter to MAX_VALUE so the next getAndIncrement() returns MAX_VALUE (valid),
        // and the one after wraps to negative (overflow).
        nextVfd.set(Integer.MAX_VALUE);

        // This call should succeed — returns Integer.MAX_VALUE which is positive
        Path path = Path.of("/tmp/overflow-test.dat");
        int vfd = registry.register(path);
        assertEquals(Integer.MAX_VALUE, vfd);

        // Next call should detect overflow (counter wrapped to negative)
        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> registry.register(path),
            "Should throw IllegalStateException on VFD counter overflow"
        );
        assertEquals("VFD counter overflow", ex.getMessage());
    }
}
