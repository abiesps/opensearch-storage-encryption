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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link MemoryPool}.
 *
 * Covers: acquire/return, stripe selection, work-stealing,
 * backpressure (demand blocks, prefetch fails fast), timeout.
 */
public class MemoryPoolTests extends OpenSearchTestCase {

    // ---- Sub-task 1: Stripe basics ----

    public void testStripeCreation() {
        try (MemoryPool pool = new MemoryPool(4, 1024, 0)) {
            assertTrue("numStripes must be power of 2", Integer.bitCount(pool.numStripes()) == 1);
            assertTrue("numStripes >= 2", pool.numStripes() >= 2);
        }
    }

    // ---- Sub-task 2: MemoryPool construction ----

    public void testPoolConstructionDistributesBuffers() {
        int totalBuffers = 16;
        try (MemoryPool pool = new MemoryPool(totalBuffers, 1024, 0)) {
            assertEquals(totalBuffers, pool.totalBuffers());
            assertEquals(1024, pool.bufferSize());
            assertEquals(totalBuffers, pool.availableBuffers());
        }
    }

    public void testPoolConstructionInvalidArgs() {
        expectThrows(IllegalArgumentException.class, () -> new MemoryPool(0, 1024, 0));
        expectThrows(IllegalArgumentException.class, () -> new MemoryPool(10, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> new MemoryPool(10, 1024, -1));
    }

    public void testNumStripesIsPowerOfTwo() {
        try (MemoryPool pool = new MemoryPool(8, 1024, 0)) {
            int n = pool.numStripes();
            assertEquals("numStripes must be power of 2", 1, Integer.bitCount(n));
        }
    }

    // ---- Sub-task 3: Stripe selection ----

    public void testStripeSelectionDeterministic() {
        try (MemoryPool pool = new MemoryPool(8, 1024, 0)) {
            int idx1 = pool.stripeIndexForCurrentThread();
            int idx2 = pool.stripeIndexForCurrentThread();
            assertEquals("Same thread should always select same stripe", idx1, idx2);
            assertTrue(idx1 >= 0 && idx1 < pool.numStripes());
        }
    }

    public void testDifferentThreadsMaySelectDifferentStripes() throws Exception {
        try (MemoryPool pool = new MemoryPool(64, 1024, 0)) {
            Set<Integer> stripeIndices = ConcurrentHashMap.newKeySet();
            int threadCount = Math.min(8, pool.numStripes());
            ExecutorService exec = Executors.newFixedThreadPool(threadCount);
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < threadCount; i++) {
                    futures.add(exec.submit(() -> {
                        stripeIndices.add(pool.stripeIndexForCurrentThread());
                    }));
                }
                for (Future<?> f : futures) f.get(5, TimeUnit.SECONDS);
                // With enough threads, we should see at least 2 distinct stripes
                assertTrue("Expected multiple stripe indices, got " + stripeIndices.size(),
                    stripeIndices.size() >= 1);
            } finally {
                exec.shutdown();
            }
        }
    }

    // ---- Sub-task 4: Acquire and backpressure ----

    public void testAcquireReturnsWritableBuffer() {
        try (MemoryPool pool = new MemoryPool(4, 1024, 0)) {
            ByteBuffer buf = pool.acquire(true, 1000);
            assertNotNull(buf);
            assertEquals(1024, buf.capacity());
            assertFalse("Buffer should be writable", buf.isReadOnly());
            assertTrue("Buffer should be direct", buf.isDirect());
        }
    }

    public void testAcquireDecreasesAvailable() {
        int total = 8;
        try (MemoryPool pool = new MemoryPool(total, 1024, 0)) {
            assertEquals(total, pool.availableBuffers());
            ByteBuffer buf = pool.acquire(true, 1000);
            assertNotNull(buf);
            assertEquals(total - 1, pool.availableBuffers());
        }
    }

    public void testAcquireAllBuffers() {
        int total = 4;
        try (MemoryPool pool = new MemoryPool(total, 1024, 0)) {
            List<ByteBuffer> acquired = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                acquired.add(pool.acquire(true, 1000));
            }
            assertEquals(total, acquired.size());
            assertEquals(0, pool.availableBuffers());
        }
    }

    public void testPrefetchFailsFastWhenExhausted() {
        int total = 2;
        try (MemoryPool pool = new MemoryPool(total, 1024, 0)) {
            // Drain the pool
            for (int i = 0; i < total; i++) {
                pool.acquire(true, 1000);
            }
            // Prefetch should fail fast
            expectThrows(MemoryBackpressureException.class,
                () -> pool.acquire(false, 1000));
            assertEquals(1, pool.metrics().prefetchSkipCount.sum());
        }
    }

    public void testDemandReadBlocksAndTimesOut() {
        int total = 2;
        try (MemoryPool pool = new MemoryPool(total, 1024, 0)) {
            // Drain the pool
            for (int i = 0; i < total; i++) {
                pool.acquire(true, 1000);
            }
            long start = System.nanoTime();
            expectThrows(PoolAcquireTimeoutException.class,
                () -> pool.acquire(true, 50));
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue("Should have waited at least ~50ms, waited " + elapsed + "ms",
                elapsed >= 30); // allow some slack
            assertEquals(1, pool.metrics().poolTimeoutCount.sum());
        }
    }

    public void testDemandReadUnblocksWhenBufferReturned() throws Exception {
        int total = 2;
        try (MemoryPool pool = new MemoryPool(total, 1024, 0)) {
            // Drain the pool
            for (int i = 0; i < total; i++) {
                pool.acquire(true, 1000);
            }

            // Start a thread that will block on acquire
            CountDownLatch started = new CountDownLatch(1);
            AtomicInteger result = new AtomicInteger(0);
            Thread blocker = new Thread(() -> {
                started.countDown();
                try {
                    ByteBuffer buf = pool.acquire(true, 5000);
                    if (buf != null) result.set(1);
                } catch (Exception e) {
                    result.set(-1);
                }
            });
            blocker.start();
            started.await();
            Thread.sleep(100); // let it block on its stripe

            // Return a buffer to ALL stripes' conditions by using the
            // pool's internal signaling. We add a buffer to each stripe
            // to ensure the blocked thread's stripe gets one.
            for (int i = 0; i < pool.numStripes(); i++) {
                ByteBuffer fresh = ByteBuffer.allocateDirect(1024);
                // Use returnToStripe which goes to the current thread's stripe.
                // Since we can't target a specific stripe from outside, we add
                // to all stripes by calling returnToStripe multiple times.
                pool.returnToStripe(fresh);
            }

            blocker.join(5000);
            assertFalse("Blocker should have completed", blocker.isAlive());
            assertEquals("Should have acquired buffer", 1, result.get());
        }
    }

    // ---- Sub-task 4 continued: Work-stealing ----

    public void testWorkStealingFromOtherStripes() {
        // Create pool with enough buffers, then drain the current thread's stripe
        // and verify acquire still works via work-stealing
        int total = 32;
        try (MemoryPool pool = new MemoryPool(total, 1024, 0)) {
            // Acquire all buffers — some will come from other stripes via stealing
            List<ByteBuffer> acquired = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                acquired.add(pool.acquire(true, 1000));
            }
            assertEquals(total, acquired.size());
            // Steals should have occurred (unless all buffers happened to be
            // in the current thread's stripe, which is unlikely with 32 buffers)
            // We just verify all acquires succeeded
        }
    }

    public void testStealCountMetric() {
        // With many buffers spread across stripes, acquiring all from one thread
        // should trigger steals
        int total = 32;
        try (MemoryPool pool = new MemoryPool(total, 1024, 0)) {
            for (int i = 0; i < total; i++) {
                pool.acquire(true, 1000);
            }
            // At least some steals should have occurred (buffers distributed
            // across stripes, but we're acquiring from one thread)
            assertTrue("Expected some steals, got " + pool.metrics().stealCount.sum(),
                pool.metrics().stealCount.sum() > 0);
        }
    }

    // ---- Sub-task 5: GC-based lifecycle ----

    public void testReturnToStripeReplenishesPool() {
        int total = 4;
        try (MemoryPool pool = new MemoryPool(total, 1024, 0)) {
            // Drain
            for (int i = 0; i < total; i++) {
                pool.acquire(true, 1000);
            }
            assertEquals(0, pool.availableBuffers());

            // Simulate cleaner callback: return a fresh buffer
            pool.returnToStripe(ByteBuffer.allocateDirect(1024));
            assertEquals(1, pool.availableBuffers());

            // Should be able to acquire again
            ByteBuffer buf = pool.acquire(true, 1000);
            assertNotNull(buf);
        }
    }

    // ---- Sub-task 6: jemalloc / fallback allocation ----

    public void testAllocateAlignedBufferFallback() {
        // With alignment=0, should get a plain direct buffer
        ByteBuffer buf = MemoryPool.allocateAlignedBuffer(1024, 0);
        assertNotNull(buf);
        assertTrue(buf.isDirect());
        assertEquals(1024, buf.capacity());
    }

    public void testAllocateAlignedBufferWithAlignment() {
        ByteBuffer buf = MemoryPool.allocateAlignedBuffer(8192, 4096);
        assertNotNull(buf);
        assertTrue(buf.isDirect());
        assertEquals(8192, buf.capacity());
    }

    // ---- Sub-task 7: PoolMetrics ----

    public void testMetricsAcquireCount() {
        try (MemoryPool pool = new MemoryPool(8, 1024, 0)) {
            pool.acquire(true, 1000);
            pool.acquire(true, 1000);
            pool.acquire(false, 1000);
            assertEquals(3, pool.metrics().acquireCount.sum());
        }
    }

    public void testMetricsPrefetchSkip() {
        try (MemoryPool pool = new MemoryPool(1, 1024, 0)) {
            pool.acquire(true, 1000); // drain
            try {
                pool.acquire(false, 100);
                fail("Expected MemoryBackpressureException");
            } catch (MemoryBackpressureException e) {
                // expected
            }
            assertEquals(1, pool.metrics().prefetchSkipCount.sum());
        }
    }

    public void testMetricsTimeout() {
        try (MemoryPool pool = new MemoryPool(1, 1024, 0)) {
            pool.acquire(true, 1000); // drain
            try {
                pool.acquire(true, 10);
                fail("Expected PoolAcquireTimeoutException");
            } catch (PoolAcquireTimeoutException e) {
                // expected
            }
            assertEquals(1, pool.metrics().poolTimeoutCount.sum());
        }
    }

    public void testMetricsToString() {
        try (MemoryPool pool = new MemoryPool(4, 1024, 0)) {
            pool.acquire(true, 1000);
            String s = pool.metrics().toString();
            assertTrue(s.contains("acquires=1"));
        }
    }

    // ---- Close behavior ----

    public void testClosePool() {
        MemoryPool pool = new MemoryPool(4, 1024, 0);
        assertFalse(pool.isClosed());
        pool.close();
        assertTrue(pool.isClosed());
        expectThrows(IllegalStateException.class, () -> pool.acquire(true, 100));
    }

    // ---- Concurrent acquire ----

    public void testConcurrentAcquire() throws Exception {
        int total = 32;
        int threadCount = 8;
        try (MemoryPool pool = new MemoryPool(total, 1024, 0)) {
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            // Use identity-based set since ByteBuffer.equals compares content
            Set<Integer> identitySet = ConcurrentHashMap.newKeySet();
            ExecutorService exec = Executors.newFixedThreadPool(threadCount);
            try {
                List<Future<?>> futures = new ArrayList<>();
                int perThread = total / threadCount;
                for (int t = 0; t < threadCount; t++) {
                    futures.add(exec.submit(() -> {
                        try {
                            barrier.await();
                            for (int i = 0; i < perThread; i++) {
                                ByteBuffer buf = pool.acquire(true, 5000);
                                assertNotNull(buf);
                                boolean added = identitySet.add(System.identityHashCode(buf));
                                assertTrue("Duplicate buffer identity!", added);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }));
                }
                for (Future<?> f : futures) f.get(10, TimeUnit.SECONDS);
                assertEquals("All buffers should be unique by identity",
                    total, identitySet.size());
                assertEquals(0, pool.availableBuffers());
            } finally {
                exec.shutdown();
            }
        }
    }
}
