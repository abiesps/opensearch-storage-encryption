/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A per-file wrapper that decouples the lifecycle of OS file descriptors
 * (Java FileChannel) from the VFD's own lifecycle. VFDs are reference-counted
 * and managed by the VFD_Registry. Only one VFD exists per unique file path
 * on a given node.
 *
 * <p>The reference count tracks only root IndexInput instances (created by
 * {@code Directory.openInput()}). Clones and slices do NOT participate in
 * VFD reference counting — Lucene does not call {@code close()} on clones
 * and slices.</p>
 *
 * <p>No FD state machine. FileChannel lifecycle is managed entirely by the
 * Caffeine cache holding RefCountedChannel values. The VFD does not maintain
 * FileChannel state or close FileChannels directly.</p>
 *
 * <p><b>Satisfies: Requirements 2, 4, 5</b></p>
 */
public class VirtualFileDescriptor {

    private static final Logger logger = Logger.getLogger(VirtualFileDescriptor.class.getName());

    private final long id;
    private final Path filePath;
    private final String normalizedPath;
    private final AtomicInteger refCount;
    /**
     * Back-reference to the registry (or release callback). Stored for use
     * by VfdRegistry when it is implemented (Task 4). Accepts Object to
     * avoid circular dependency until VfdRegistry exists.
     */
    private final Object registryRef;
    private final List<Runnable> onOpenCallbacks;
    private final List<Runnable> onCloseCallbacks;

    /**
     * Creates a new VirtualFileDescriptor with refCount = 1.
     *
     * @param id         unique VFD identifier (from AtomicLong generator)
     * @param filePath   the file path (will be normalized and stored)
     * @param registryRef back-reference to the registry (Object to avoid
     *                    circular dependency; will be cast to VfdRegistry later)
     */
    public VirtualFileDescriptor(long id, Path filePath, Object registryRef) {
        this.id = id;
        this.filePath = filePath.toAbsolutePath().normalize();
        this.normalizedPath = this.filePath.toString();
        this.refCount = new AtomicInteger(1);
        this.registryRef = registryRef;
        this.onOpenCallbacks = new CopyOnWriteArrayList<>();
        this.onCloseCallbacks = new CopyOnWriteArrayList<>();
    }

    /** Returns the unique VFD identifier. */
    public long id() {
        return id;
    }

    /** Returns the absolute normalized file path. */
    public Path filePath() {
        return filePath;
    }

    /** Returns the cached string form of the normalized path. */
    public String normalizedPath() {
        return normalizedPath;
    }

    /** Returns the registry back-reference (for use by VfdRegistry). */
    public Object registryRef() {
        return registryRef;
    }

    /** Returns the current reference count for observability. */
    public int refCount() {
        return refCount.get();
    }

    // ---- Reference counting ----

    /**
     * Attempt to increment the reference count. Uses a CAS loop: read current
     * count, if &le; 0 return false (already dead), else CAS to current+1.
     *
     * @return {@code true} if the ref count was successfully incremented,
     *         {@code false} if the VFD is already dead (refCount &le; 0)
     */
    public boolean tryIncRef() {
        while (true) {
            int current = refCount.get();
            if (current <= 0) {
                return false; // already dead
            }
            if (refCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * Decrement the reference count. Asserts the new count is &ge; 0.
     *
     * @return the new reference count after decrement
     */
    public int decRef() {
        int newCount = refCount.decrementAndGet();
        assert newCount >= 0 : "VFD ref count went negative: " + newCount;
        return newCount;
    }

    // ---- Lifecycle callbacks ----

    /**
     * Register a callback to be invoked when this VFD is opened (i.e., when
     * the underlying OS file descriptor is first opened).
     *
     * @param callback the callback to register
     */
    public void addOnOpenCallback(Runnable callback) {
        onOpenCallbacks.add(callback);
    }

    /**
     * Register a callback to be invoked when this VFD is closed (i.e., when
     * the reference count reaches 0 and the VFD is removed from the registry).
     *
     * @param callback the callback to register
     */
    public void addOnCloseCallback(Runnable callback) {
        onCloseCallbacks.add(callback);
    }

    /**
     * Invoke all registered "on open" callbacks. Exception-safe: if one
     * callback throws, the exception is logged and remaining callbacks
     * continue to execute.
     */
    public void invokeOnOpenCallbacks() {
        invokeCallbacksSafely(onOpenCallbacks, "onOpen");
    }

    /**
     * Invoke all registered "on close" callbacks. Exception-safe: if one
     * callback throws, the exception is logged and remaining callbacks
     * continue to execute.
     */
    public void invokeOnCloseCallbacks() {
        invokeCallbacksSafely(onCloseCallbacks, "onClose");
    }

    /**
     * Invoke a list of callbacks with exception isolation. Each callback
     * runs independently — a failure in one does not prevent others from
     * executing.
     */
    private void invokeCallbacksSafely(List<Runnable> callbacks, String callbackType) {
        for (Runnable callback : callbacks) {
            try {
                callback.run();
            } catch (Exception e) {
                logger.log(
                    Level.WARNING,
                    "Exception in VFD " + callbackType + " callback for [" + normalizedPath + "] (id=" + id + ")",
                    e
                );
            }
        }
    }

    @Override
    public String toString() {
        return "VirtualFileDescriptor{id=" + id + ", path=" + normalizedPath + ", refCount=" + refCount.get() + "}";
    }
}
