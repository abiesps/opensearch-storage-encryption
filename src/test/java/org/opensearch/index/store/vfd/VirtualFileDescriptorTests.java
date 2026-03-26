/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link VirtualFileDescriptor} — ref counting, tryIncRef on
 * dead VFD returns false, callback invocation, callback exception isolation.
 *
 * <p><b>Validates: Requirements 2, 4, 5</b></p>
 */
public class VirtualFileDescriptorTests extends OpenSearchTestCase {

    private static final Object NOOP_REGISTRY = new Object();

    private Path tempFile;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempFile = createTempFile("vfd-test", ".dat");
    }

    // ---- Construction ----

    public void testConstructorInitializesFields() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(42L, tempFile, NOOP_REGISTRY);

        assertEquals(42L, vfd.id());
        assertNotNull(vfd.filePath());
        assertTrue("filePath should be absolute", vfd.filePath().isAbsolute());
        assertNotNull(vfd.normalizedPath());
        assertEquals(vfd.filePath().toString(), vfd.normalizedPath());
        assertEquals(1, vfd.refCount());
    }

    public void testPathIsNormalized() {
        // Create a path with redundant components
        Path unnormalized = tempFile.getParent().resolve("..").resolve(tempFile.getParent().getFileName()).resolve(tempFile.getFileName());
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, unnormalized, NOOP_REGISTRY);

        // The stored path should be normalized (no ".." components)
        assertFalse("normalizedPath should not contain '..'", vfd.normalizedPath().contains(".."));
        assertEquals(tempFile.toAbsolutePath().normalize().toString(), vfd.normalizedPath());
    }

    // ---- Reference counting: basic increment/decrement ----

    public void testRefCountStartsAtOne() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);
        assertEquals(1, vfd.refCount());
    }

    public void testTryIncRefIncrementsCount() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);

        assertTrue(vfd.tryIncRef());
        assertEquals(2, vfd.refCount());

        assertTrue(vfd.tryIncRef());
        assertEquals(3, vfd.refCount());
    }

    public void testDecRefDecrementsCount() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);

        vfd.tryIncRef(); // refCount = 2
        assertEquals(2, vfd.refCount());

        int newCount = vfd.decRef();
        assertEquals(1, newCount);
        assertEquals(1, vfd.refCount());
    }

    public void testDecRefToZero() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);
        assertEquals(1, vfd.refCount());

        int newCount = vfd.decRef();
        assertEquals(0, newCount);
        assertEquals(0, vfd.refCount());
    }

    // ---- tryIncRef on dead VFD returns false ----

    public void testTryIncRefOnDeadVfdReturnsFalse() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);

        // Kill the VFD
        vfd.decRef(); // refCount = 0
        assertEquals(0, vfd.refCount());

        // tryIncRef should fail
        assertFalse("tryIncRef must return false on dead VFD (refCount=0)", vfd.tryIncRef());
        assertEquals(0, vfd.refCount());
    }

    public void testTryIncRefAfterMultipleDecRefsReturnsFalse() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);

        vfd.tryIncRef(); // refCount = 2
        vfd.tryIncRef(); // refCount = 3

        vfd.decRef(); // 2
        vfd.decRef(); // 1
        vfd.decRef(); // 0

        assertFalse("tryIncRef must return false after all refs released", vfd.tryIncRef());
    }

    // ---- decRef assertion on negative ----

    public void testDecRefBelowZeroAsserts() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);
        vfd.decRef(); // refCount = 0

        AssertionError e = expectThrows(AssertionError.class, vfd::decRef);
        assertTrue(e.getMessage().contains("ref count went negative"));
    }

    // ---- Lifecycle callbacks: onOpen ----

    public void testOnOpenCallbacksInvoked() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);

        AtomicBoolean called1 = new AtomicBoolean(false);
        AtomicBoolean called2 = new AtomicBoolean(false);

        vfd.addOnOpenCallback(() -> called1.set(true));
        vfd.addOnOpenCallback(() -> called2.set(true));

        vfd.invokeOnOpenCallbacks();

        assertTrue("First onOpen callback should have been invoked", called1.get());
        assertTrue("Second onOpen callback should have been invoked", called2.get());
    }

    // ---- Lifecycle callbacks: onClose ----

    public void testOnCloseCallbacksInvoked() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);

        AtomicBoolean called1 = new AtomicBoolean(false);
        AtomicBoolean called2 = new AtomicBoolean(false);

        vfd.addOnCloseCallback(() -> called1.set(true));
        vfd.addOnCloseCallback(() -> called2.set(true));

        vfd.invokeOnCloseCallbacks();

        assertTrue("First onClose callback should have been invoked", called1.get());
        assertTrue("Second onClose callback should have been invoked", called2.get());
    }

    // ---- Callback invocation order ----

    public void testCallbacksInvokedInRegistrationOrder() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);

        List<Integer> order = Collections.synchronizedList(new ArrayList<>());

        vfd.addOnCloseCallback(() -> order.add(1));
        vfd.addOnCloseCallback(() -> order.add(2));
        vfd.addOnCloseCallback(() -> order.add(3));

        vfd.invokeOnCloseCallbacks();

        assertEquals(List.of(1, 2, 3), order);
    }

    // ---- Callback exception isolation ----

    public void testOnOpenCallbackExceptionDoesNotPreventOthers() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);

        AtomicBoolean beforeCalled = new AtomicBoolean(false);
        AtomicBoolean afterCalled = new AtomicBoolean(false);

        vfd.addOnOpenCallback(() -> beforeCalled.set(true));
        vfd.addOnOpenCallback(() -> { throw new RuntimeException("boom"); });
        vfd.addOnOpenCallback(() -> afterCalled.set(true));

        // Should not throw — exception is logged and swallowed
        vfd.invokeOnOpenCallbacks();

        assertTrue("Callback before exception should have been invoked", beforeCalled.get());
        assertTrue("Callback after exception should have been invoked", afterCalled.get());
    }

    public void testOnCloseCallbackExceptionDoesNotPreventOthers() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);

        AtomicBoolean beforeCalled = new AtomicBoolean(false);
        AtomicBoolean afterCalled = new AtomicBoolean(false);

        vfd.addOnCloseCallback(() -> beforeCalled.set(true));
        vfd.addOnCloseCallback(() -> { throw new RuntimeException("kaboom"); });
        vfd.addOnCloseCallback(() -> afterCalled.set(true));

        // Should not throw
        vfd.invokeOnCloseCallbacks();

        assertTrue("Callback before exception should have been invoked", beforeCalled.get());
        assertTrue("Callback after exception should have been invoked", afterCalled.get());
    }

    public void testMultipleCallbackExceptionsAllIsolated() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);

        AtomicInteger successCount = new AtomicInteger(0);

        vfd.addOnCloseCallback(() -> { throw new RuntimeException("error 1"); });
        vfd.addOnCloseCallback(() -> successCount.incrementAndGet());
        vfd.addOnCloseCallback(() -> { throw new RuntimeException("error 2"); });
        vfd.addOnCloseCallback(() -> successCount.incrementAndGet());
        vfd.addOnCloseCallback(() -> { throw new RuntimeException("error 3"); });

        vfd.invokeOnCloseCallbacks();

        assertEquals("Both non-throwing callbacks should have executed", 2, successCount.get());
    }

    // ---- No callbacks registered ----

    public void testInvokeCallbacksWithNoneRegistered() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(1L, tempFile, NOOP_REGISTRY);

        // Should be a no-op, no exceptions
        vfd.invokeOnOpenCallbacks();
        vfd.invokeOnCloseCallbacks();
    }

    // ---- toString ----

    public void testToStringContainsKeyInfo() {
        VirtualFileDescriptor vfd = new VirtualFileDescriptor(99L, tempFile, NOOP_REGISTRY);
        String str = vfd.toString();

        assertTrue("toString should contain id", str.contains("99"));
        assertTrue("toString should contain refCount", str.contains("refCount=1"));
    }
}
