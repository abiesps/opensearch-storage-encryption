/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Property-based tests for {@link MemoryPool}.
 *
 * <p>Property 1: Pool size invariant — total buffers constant.
 * <p>Property 2: No buffer handed to two concurrent writers.
 *
 * <p><b>Validates: Requirements 11, 12, 13</b>
 */
public class MemoryPoolPropertyTests extends OpenSearchTestCase {

    /**
     * Property 1: Pool size invariant — the total number of buffers
     * (available in pool + acquired by callers) is always equal to
     * the configured totalBuffers.
     *
     * <p><b>Validates: Requirements 11.4, 12.4</b>
     */
    public void testPoolSizeInvariant() {
        for (int trial = 0; trial < 50; trial++) {
            int totalBuffers = randomIntBetween(4, 32);
            try (MemoryPool pool = new MemoryPool(totalBuffers, 1024, 0)) {
                int acquireCount = randomIntBetween(0, totalBuffers);
                List<ByteBuffer> acquired = new ArrayList<>();

                for (int i = 0; i < acquireCount; i++) {
                    acquired.add(pool.acquire(true, 1000));
                }

                int available = pool.availableBuffers();
                int held = acquired.size();
                assertEquals(
                    "Invariant violated: available(" + available + ") + held(" + held
                        + ") != total(" + totalBuffers + ")",
                    totalBuffers, available + held
                );

                // Return some buffers and re-check
                int returnCount = randomIntBetween(0, held);
                for (int i = 0; i < returnCount; i++) {
                    pool.returnToStripe(ByteBuffer.allocateDirect(1024));
                }

                // After returning, available should increase
                int newAvailable = pool.availableBuffers();
                assertTrue("Available should have increased after return",
                    newAvailable >= available);
            }
        }
    }

    /**
     * Property 2: No buffer is handed to two concurrent writers.
     * Each acquire() call returns a distinct ByteBuffer instance.
     *
     * <p><b>Validates: Requirements 13.8</b>
     */
    public void testNoBufferHandedToTwoConcurrentWriters() throws Exception {
        for (int trial = 0; trial < 10; trial++) {
            int totalBuffers = randomIntBetween(8, 32);
            int threadCount = randomIntBetween(2, 8);
            int perThread = totalBuffers / threadCount;
            if (perThread == 0) continue;

            try (MemoryPool pool = new MemoryPool(totalBuffers, 1024, 0)) {
                CyclicBarrier barrier = new CyclicBarrier(threadCount);
                // Use identity hash codes since ByteBuffer.equals compares content
                Set<Integer> identitySet = ConcurrentHashMap.newKeySet();
                ExecutorService exec = Executors.newFixedThreadPool(threadCount);

                try {
                    List<Future<?>> futures = new ArrayList<>();
                    for (int t = 0; t < threadCount; t++) {
                        futures.add(exec.submit(() -> {
                            try {
                                barrier.await(5, TimeUnit.SECONDS);
                                for (int i = 0; i < perThread; i++) {
                                    ByteBuffer buf = pool.acquire(true, 5000);
                                    assertNotNull("acquire returned null", buf);
                                    boolean added = identitySet.add(System.identityHashCode(buf));
                                    assertTrue(
                                        "DUPLICATE BUFFER! Same buffer handed to two writers",
                                        added
                                    );
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }));
                    }
                    for (Future<?> f : futures) {
                        f.get(15, TimeUnit.SECONDS);
                    }

                    int expectedTotal = threadCount * perThread;
                    assertEquals(
                        "All acquired buffers should be unique by identity",
                        expectedTotal, identitySet.size()
                    );
                } finally {
                    exec.shutdown();
                    exec.awaitTermination(5, TimeUnit.SECONDS);
                }
            }
        }
    }
}
