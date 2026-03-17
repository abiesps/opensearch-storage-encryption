/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for DirectMemoryAdmissionController edge cases.
 *
 * Feature: bufferpool-v2-memory-management
 */
class DirectMemoryAdmissionControllerTests {

    /**
     * Verify DirectMemoryExhaustedException is thrown when utilization exceeds
     * hard threshold and GC doesn't reclaim within timeout.
     *
     * Validates: Requirement 8.7
     */
    @Test
    void hardThresholdTimeoutThrowsException() {
        long maxMem = 10_000L;
        double softThreshold = 0.85;
        double hardThreshold = 0.95;
        long hardTimeoutMs = 200; // short timeout for test speed
        DirectMemoryAdmissionController controller = new DirectMemoryAdmissionController(maxMem, softThreshold, hardThreshold, hardTimeoutMs);

        // Fill to 94% — just below hard threshold (acquire won't block here)
        controller.acquire(9400);

        // This acquire pushes to 96% — above hard threshold, should block and timeout
        // (no other thread will release, so utilization stays above hard threshold)
        DirectMemoryExhaustedException ex = assertThrows(
            DirectMemoryExhaustedException.class,
            () -> controller.acquire(200),
            "Should throw DirectMemoryExhaustedException when hard threshold exceeded and timeout expires"
        );

        assertTrue(ex.getMessage().contains("Direct memory exhausted"), "Exception message should indicate exhaustion");
        assertTrue(ex.utilization() > hardThreshold, "Exception should report utilization above hard threshold");
        assertEquals(maxMem, ex.maxDirectMemory(), "Exception should report correct maxDirectMemory");

        // After the exception, the failed allocation's bytes should have been rolled back
        assertEquals(9400, controller.getOutstandingBytes(), "Failed allocation bytes should be rolled back");
    }

    /**
     * Verify that the exception contains meaningful diagnostic information.
     *
     * Validates: Requirement 8.7
     */
    @Test
    void exceptionContainsDiagnosticInfo() {
        long maxMem = 1000L;
        DirectMemoryAdmissionController controller = new DirectMemoryAdmissionController(maxMem, 0.85, 0.95, 100);

        // Fill to 94% — just below hard threshold
        controller.acquire(940);

        // This pushes to 96.5% — above hard threshold, triggers block + timeout
        DirectMemoryExhaustedException ex = assertThrows(
            DirectMemoryExhaustedException.class,
            () -> controller.acquire(25)
        );

        assertTrue(ex.outstandingBytes() > 0, "Should report outstanding bytes");
        assertEquals(1000, ex.maxDirectMemory(), "Should report max direct memory");
        assertTrue(ex.utilization() > 0.95, "Should report utilization above hard threshold");
    }
}
