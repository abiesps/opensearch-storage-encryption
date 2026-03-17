/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for VirtualFileDescriptorRegistry.
 *
 * Feature: bufferpool-v2-memory-management
 */
class VirtualFileDescriptorRegistryPropertyTests {

    /**
     * Property 1: VFD uniqueness under concurrent registration.
     * N concurrent register() calls → all returned vfds are distinct positive integers.
     *
     * Validates: Requirements 1.2, 1.3, 1.5, 1.6
     */
    @Property(tries = 50)
    void allVfdsAreDistinctPositiveUnderConcurrency(@ForAll @IntRange(min = 2, max = 64) int threadCount) throws Exception {
        VirtualFileDescriptorRegistry registry = VirtualFileDescriptorRegistry.getInstance();
        registry.clear();
        int registrationsPerThread = 100;
        Set<Integer> allVfds = Collections.newSetFromMap(new ConcurrentHashMap<>());
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < registrationsPerThread; i++) {
                        Path path = Path.of("/tmp/test-" + threadId + "-" + i + ".dat");
                        int vfd = registry.register(path);
                        assertTrue(vfd > 0, "VFD must be positive, got: " + vfd);
                        boolean added = allVfds.add(vfd);
                        assertTrue(added, "Duplicate VFD detected: " + vfd);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Threads did not complete in time");
        executor.shutdown();

        int expectedTotal = threadCount * registrationsPerThread;
        assertEquals(expectedTotal, allVfds.size(), "Expected " + expectedTotal + " unique VFDs");
    }

    /**
     * Property 2: VFD register/deregister lifecycle.
     * After deregister(vfd), getPath(vfd) returns null; registry size stays bounded across cycles.
     *
     * Validates: Requirements 1.4, 7.1, 7.2, 11.1, 11.4
     */
    @Property(tries = 100)
    void registerDeregisterLifecycleKeepsSizeBounded(@ForAll @IntRange(min = 1, max = 200) int cycleCount) {
        VirtualFileDescriptorRegistry registry = VirtualFileDescriptorRegistry.getInstance();
        registry.clear();

        for (int i = 0; i < cycleCount; i++) {
            Path path = Path.of("/tmp/lifecycle-" + i + ".dat");
            int vfd = registry.register(path);
            assertTrue(vfd > 0, "VFD must be positive");
            assertEquals(path.toAbsolutePath().normalize().toString(), registry.getPath(vfd));

            registry.deregister(vfd);
            assertNull(registry.getPath(vfd), "getPath should return null after deregister");
        }

        // After all register/deregister cycles, size should be 0
        assertEquals(0, registry.size(), "Registry size should be 0 after all deregistrations");
    }
}
