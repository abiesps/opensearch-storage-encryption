/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Semaphore;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link RefCountedChannel} — acquire/release lifecycle,
 * acquire-after-close returns null, release-to-zero closes channel,
 * double-release assertion.
 *
 * <p><b>Validates: Requirements 2, 3, 5</b></p>
 */
public class RefCountedChannelTests extends OpenSearchTestCase {

    private Path tempFile;
    private Semaphore limiter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempFile = createTempFile("refcounted", ".dat");
        // Write some data so the file is non-empty
        Files.write(tempFile, new byte[] { 1, 2, 3, 4 });
        limiter = new Semaphore(10);
    }

    private FileChannel openChannel() throws IOException {
        return FileChannel.open(tempFile, StandardOpenOption.READ);
    }

    // ---- Acquire/release lifecycle ----

    public void testAcquireReturnsChannel() throws IOException {
        FileChannel fc = openChannel();
        RefCountedChannel rcc = new RefCountedChannel(fc, limiter);
        assertEquals(1, rcc.refCount());

        FileChannel acquired = rcc.acquire();
        assertNotNull(acquired);
        assertSame(fc, acquired);
        assertEquals(2, rcc.refCount());

        rcc.release(); // release I/O ref
        assertEquals(1, rcc.refCount());
        assertTrue("Channel should still be open after releasing I/O ref", fc.isOpen());

        rcc.release(); // release base ref → closes channel
        assertEquals(0, rcc.refCount());
        assertFalse("Channel should be closed after all refs released", fc.isOpen());
    }

    public void testMultipleAcquireRelease() throws IOException {
        FileChannel fc = openChannel();
        RefCountedChannel rcc = new RefCountedChannel(fc, limiter);

        // Simulate 3 concurrent I/O operations
        FileChannel a1 = rcc.acquire();
        FileChannel a2 = rcc.acquire();
        FileChannel a3 = rcc.acquire();
        assertNotNull(a1);
        assertNotNull(a2);
        assertNotNull(a3);
        assertEquals(4, rcc.refCount()); // 1 base + 3 I/O

        rcc.release(); // I/O 1 done
        assertEquals(3, rcc.refCount());
        assertTrue(fc.isOpen());

        rcc.release(); // I/O 2 done
        assertEquals(2, rcc.refCount());
        assertTrue(fc.isOpen());

        rcc.release(); // I/O 3 done
        assertEquals(1, rcc.refCount());
        assertTrue(fc.isOpen());

        rcc.release(); // base ref released (eviction)
        assertEquals(0, rcc.refCount());
        assertFalse(fc.isOpen());
    }

    // ---- Acquire after close returns null ----

    public void testAcquireAfterCloseReturnsNull() throws IOException {
        FileChannel fc = openChannel();
        RefCountedChannel rcc = new RefCountedChannel(fc, limiter);

        rcc.release(); // release base ref → refCount = 0, channel closed
        assertEquals(0, rcc.refCount());
        assertFalse(fc.isOpen());

        FileChannel acquired = rcc.acquire();
        assertNull("acquire() should return null after channel is closed", acquired);
    }

    // ---- Release to zero closes channel ----

    public void testReleaseToZeroClosesChannel() throws IOException {
        FileChannel fc = openChannel();
        int permitsBefore = limiter.availablePermits();
        RefCountedChannel rcc = new RefCountedChannel(fc, limiter);

        rcc.release(); // base ref → 0
        assertFalse("Channel must be closed when refCount reaches 0", fc.isOpen());
        assertEquals("FD limiter permit must be released", permitsBefore + 1, limiter.availablePermits());
    }

    // ---- Eviction with in-flight I/O: channel stays open ----

    public void testEvictionWithInflightIO() throws IOException {
        FileChannel fc = openChannel();
        RefCountedChannel rcc = new RefCountedChannel(fc, limiter);

        // Simulate I/O acquiring a ref
        FileChannel acquired = rcc.acquire();
        assertNotNull(acquired);
        assertEquals(2, rcc.refCount());

        // Simulate eviction: cache drops base ref
        rcc.release();
        assertEquals(1, rcc.refCount());
        assertTrue("Channel must stay open while I/O holds a ref", fc.isOpen());

        // I/O completes, releases its ref → channel closes
        rcc.release();
        assertEquals(0, rcc.refCount());
        assertFalse("Channel must close after last ref released", fc.isOpen());
    }

    // ---- Acquire returns null during closing (eviction dropped base, I/O finishing) ----

    public void testAcquireReturnsNullWhenFullyClosed() throws IOException {
        FileChannel fc = openChannel();
        RefCountedChannel rcc = new RefCountedChannel(fc, limiter);

        // Acquire I/O ref, then eviction drops base ref
        rcc.acquire();
        rcc.release(); // eviction drops base → refCount = 1
        rcc.release(); // I/O done → refCount = 0, channel closed

        assertNull("acquire() must return null on closed channel", rcc.acquire());
    }

    // ---- Double release assertion ----

    public void testDoubleReleaseAsserts() throws IOException {
        FileChannel fc = openChannel();
        RefCountedChannel rcc = new RefCountedChannel(fc, limiter);

        rcc.release(); // base ref → 0, channel closed
        assertEquals(0, rcc.refCount());

        // Second release should trigger assertion (refCount goes negative)
        AssertionError e = expectThrows(AssertionError.class, rcc::release);
        assertTrue(e.getMessage().contains("refCount went negative"));
    }

    // ---- FD limiter permit lifecycle ----

    public void testFdLimiterPermitReleasedOnClose() throws Exception {
        Semaphore tightLimiter = new Semaphore(1);
        // Simulate acquiring the permit before opening
        tightLimiter.acquire();
        assertEquals(0, tightLimiter.availablePermits());

        FileChannel fc = openChannel();
        RefCountedChannel rcc = new RefCountedChannel(fc, tightLimiter);

        rcc.release(); // base ref → 0, closes channel, releases permit
        assertEquals(1, tightLimiter.availablePermits());
    }

    // ---- refCount() observability ----

    public void testRefCountObservability() throws IOException {
        FileChannel fc = openChannel();
        RefCountedChannel rcc = new RefCountedChannel(fc, limiter);

        assertEquals(1, rcc.refCount());
        rcc.acquire();
        assertEquals(2, rcc.refCount());
        rcc.acquire();
        assertEquals(3, rcc.refCount());
        rcc.release();
        assertEquals(2, rcc.refCount());
        rcc.release();
        assertEquals(1, rcc.refCount());
        rcc.release();
        assertEquals(0, rcc.refCount());
    }

    // ---- Channel is usable between acquire and release ----

    public void testChannelUsableDuringAcquire() throws IOException {
        FileChannel fc = openChannel();
        RefCountedChannel rcc = new RefCountedChannel(fc, limiter);

        FileChannel acquired = rcc.acquire();
        assertNotNull(acquired);
        assertTrue(acquired.isOpen());

        // Read from the channel
        ByteBuffer buf = ByteBuffer.allocate(4);
        int bytesRead = acquired.read(buf, 0);
        assertEquals(4, bytesRead);
        buf.flip();
        assertEquals(1, buf.get());
        assertEquals(2, buf.get());
        assertEquals(3, buf.get());
        assertEquals(4, buf.get());

        rcc.release(); // I/O ref
        rcc.release(); // base ref
    }
}
