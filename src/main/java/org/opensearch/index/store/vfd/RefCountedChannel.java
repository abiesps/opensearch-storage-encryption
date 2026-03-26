/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A wrapper around {@link FileChannel} that uses an {@link AtomicInteger}
 * reference count to guarantee the channel is never closed while any I/O
 * operation holds a ref.
 *
 * <h2>Ref count sources</h2>
 * <ol>
 *   <li>The Caffeine cache holds one "base" ref (set to 1 at creation,
 *       released by the eviction listener).</li>
 *   <li>Each in-flight I/O operation holds one ref (acquired via
 *       {@link #acquire()}, released via {@link #release()} in a finally
 *       block).</li>
 * </ol>
 *
 * <p>When Caffeine evicts the entry, the eviction listener calls
 * {@link #release()} (dropping the base ref). If no I/O is in flight,
 * refCount hits 0 and the channel closes immediately + the
 * {@code fdOpenLimiter} permit is released. If I/O is in flight, the
 * channel stays open until the last I/O thread's {@code release()} brings
 * refCount to 0.</p>
 */
public class RefCountedChannel {

    private final FileChannel channel;
    private final AtomicInteger refCount;
    private final Semaphore fdOpenLimiter;

    /**
     * Creates a new RefCountedChannel with refCount = 1 (the cache base ref).
     *
     * @param channel       the open FileChannel to wrap
     * @param fdOpenLimiter the system-wide FD open limiter semaphore; a permit
     *                      is released when the channel is finally closed
     */
    public RefCountedChannel(FileChannel channel, Semaphore fdOpenLimiter) {
        this.channel = channel;
        this.fdOpenLimiter = fdOpenLimiter;
        this.refCount = new AtomicInteger(1); // cache holds base ref
    }

    /**
     * Acquire a ref for an I/O operation. Returns the raw FileChannel,
     * or {@code null} if the channel is closing (refCount &le; 0).
     *
     * <p>Uses a CAS loop: read current count, if &le; 0 return null
     * (channel closing/closed), otherwise CAS to current+1 and return
     * the FileChannel.</p>
     *
     * <p>Caller <b>MUST</b> call {@link #release()} in a finally block
     * after I/O completes.</p>
     *
     * @return the FileChannel, or {@code null} if the channel is closing
     */
    public FileChannel acquire() {
        while (true) {
            int current = refCount.get();
            if (current <= 0) {
                return null; // closing or closed
            }
            if (refCount.compareAndSet(current, current + 1)) {
                return channel;
            }
        }
    }

    /**
     * Release a ref. If refCount reaches 0, close the FileChannel quietly
     * and release the {@code fdOpenLimiter} permit.
     */
    public void release() {
        int newCount = refCount.decrementAndGet();
        assert newCount >= 0 : "RefCountedChannel refCount went negative: " + newCount;
        if (newCount == 0) {
            closeQuietly(channel);
            fdOpenLimiter.release();
        }
    }

    /**
     * Returns the current reference count for observability.
     *
     * @return the current ref count
     */
    public int refCount() {
        return refCount.get();
    }

    /**
     * Closes the given channel, swallowing any {@link IOException}.
     */
    private static void closeQuietly(FileChannel ch) {
        try {
            ch.close();
        } catch (IOException ignored) {
            // best-effort close
        }
    }
}
