/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lock-striped, GC-lifecycle ByteBuffer pool for DirectIO-aligned I/O.
 *
 * <p>The pool manages a fixed-size set of DirectIO-aligned {@link ByteBuffer}s.
 * Lock contention is minimized via striping: each thread maps to a stripe
 * based on its thread ID. Work-stealing from adjacent stripes is used when
 * the selected stripe is empty.
 *
 * <h2>GC-Based Lifecycle</h2>
 * <p>Buffers are never returned to the pool manually. Instead, a
 * {@link Cleaner} registered at allocation time allocates a <em>fresh</em>
 * replacement buffer and adds it to the pool when the GC reclaims the old one.
 * This means the total buffer count is constant at steady state.
 *
 * <h2>Backpressure</h2>
 * <ul>
 *   <li>Demand reads: block with configurable timeout</li>
 *   <li>Prefetch reads: fail-fast (throw {@link MemoryBackpressureException})</li>
 * </ul>
 *
 * <h2>jemalloc Integration</h2>
 * <p>Uses FFM (Foreign Function &amp; Memory API) to call jemalloc's
 * {@code aligned_alloc} for DirectIO-aligned allocation. Falls back to
 * {@link ByteBuffer#allocateDirect(int)} if jemalloc is unavailable.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
public class MemoryPool implements AutoCloseable {

    /** Default buffer size: 8 KB (CACHE_BLOCK_SIZE). */
    public static final int DEFAULT_BUFFER_SIZE = 8192;

    /** Default alignment: 4096 bytes for DirectIO. */
    public static final int DEFAULT_ALIGNMENT = 4096;

    private final Stripe[] stripes;
    private final int numStripes;
    private final int stripeMask;
    private final int bufferSize;
    private final int alignmentBytes;
    private final int totalBuffers;
    private final PoolMetrics metrics;
    private final Cleaner cleaner;
    private final FaultInjector faultInjector;
    private volatile boolean closed;
    private volatile double directMemoryThreshold = 0.85;

    // jemalloc FFM handles (null if jemalloc unavailable)
    private static final MethodHandle JEMALLOC_ALIGNED_ALLOC;
    @SuppressWarnings("unused") // retained for future jemalloc free() on pool close
    private static final MethodHandle JEMALLOC_FREE;
    private static final boolean JEMALLOC_AVAILABLE;

    static {
        MethodHandle alignedAlloc = null;
        MethodHandle free = null;
        boolean available = false;
        try {
            SymbolLookup lookup = SymbolLookup.loaderLookup();
            // Try to find jemalloc symbols — they may be available if
            // libjemalloc is loaded via -Djava.library.path or LD_PRELOAD
            var alignedAllocSym = lookup.find("aligned_alloc");
            var freeSym = lookup.find("free");
            if (alignedAllocSym.isEmpty() || freeSym.isEmpty()) {
                // Fall back to default library lookup (libc)
                lookup = Linker.nativeLinker().defaultLookup();
                alignedAllocSym = lookup.find("aligned_alloc");
                freeSym = lookup.find("free");
            }
            if (alignedAllocSym.isPresent() && freeSym.isPresent()) {
                Linker linker = Linker.nativeLinker();
                alignedAlloc = linker.downcallHandle(
                    alignedAllocSym.get(),
                    FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
                );
                free = linker.downcallHandle(freeSym.get(), FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
                available = true;
            }
        } catch (Exception | UnsatisfiedLinkError e) {
            // jemalloc not available — will use fallback
        }
        JEMALLOC_ALIGNED_ALLOC = alignedAlloc;
        JEMALLOC_FREE = free;
        JEMALLOC_AVAILABLE = available;
    }

    /**
     * A single stripe in the lock-striped pool. Each stripe has its own
     * lock, free list, and condition variable for demand-read blocking.
     */
    static final class Stripe {
        final ReentrantLock lock;
        final ArrayDeque<ByteBuffer> freeList;
        final Condition notEmpty;

        Stripe() {
            this.lock = new ReentrantLock();
            this.freeList = new ArrayDeque<>();
            this.notEmpty = lock.newCondition();
        }

        /** Try to take a buffer without blocking. Returns null if empty. */
        ByteBuffer tryTake() {
            lock.lock();
            try {
                return freeList.pollFirst();
            } finally {
                lock.unlock();
            }
        }

        /** Add a buffer to this stripe and signal one waiting thread. */
        void returnBuffer(ByteBuffer buf) {
            lock.lock();
            try {
                freeList.addLast(buf);
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }

        /** Returns the current number of buffers in this stripe. */
        int size() {
            lock.lock();
            try {
                return freeList.size();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Creates a MemoryPool with the specified parameters and a custom fault injector.
     *
     * @param totalBuffers   total number of buffers in the pool (fixed)
     * @param bufferSize     size of each buffer in bytes
     * @param alignmentBytes alignment in bytes (0 for unaligned/NFS mode)
     * @param faultInjector  fault injector for testing; use {@link NoOpFaultInjector#INSTANCE} in production
     */
    public MemoryPool(int totalBuffers, int bufferSize, int alignmentBytes, FaultInjector faultInjector) {
        if (totalBuffers <= 0) {
            throw new IllegalArgumentException("totalBuffers must be positive: " + totalBuffers);
        }
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be positive: " + bufferSize);
        }
        if (alignmentBytes < 0) {
            throw new IllegalArgumentException("alignmentBytes must be non-negative: " + alignmentBytes);
        }

        this.totalBuffers = totalBuffers;
        this.bufferSize = bufferSize;
        this.alignmentBytes = alignmentBytes;
        this.faultInjector = faultInjector;
        this.metrics = new PoolMetrics();
        this.cleaner = Cleaner.create();
        this.closed = false;

        // NUM_STRIPES = 2 × availableProcessors, rounded up to power of 2
        int rawStripes = 2 * Runtime.getRuntime().availableProcessors();
        this.numStripes = Integer.highestOneBit(rawStripes - 1) << 1; // next power of 2
        this.stripeMask = numStripes - 1;

        this.stripes = new Stripe[numStripes];
        for (int i = 0; i < numStripes; i++) {
            stripes[i] = new Stripe();
        }

        // Pre-allocate all buffers and distribute across stripes
        for (int i = 0; i < totalBuffers; i++) {
            ByteBuffer buf = allocateAlignedBuffer(bufferSize, alignmentBytes);
            registerCleaner(buf);  // once, at creation
            stripes[i % numStripes].freeList.addLast(buf);
        }
    }

    /**
     * Creates a MemoryPool with the specified parameters and {@link NoOpFaultInjector}.
     *
     * @param totalBuffers   total number of buffers in the pool (fixed)
     * @param bufferSize     size of each buffer in bytes
     * @param alignmentBytes alignment in bytes (0 for unaligned/NFS mode)
     */
    public MemoryPool(int totalBuffers, int bufferSize, int alignmentBytes) {
        this(totalBuffers, bufferSize, alignmentBytes, NoOpFaultInjector.INSTANCE);
    }

    /**
     * Creates a MemoryPool with default alignment (4096 bytes).
     *
     * @param totalBuffers total number of buffers in the pool
     * @param bufferSize   size of each buffer in bytes
     */
    public MemoryPool(int totalBuffers, int bufferSize) {
        this(totalBuffers, bufferSize, DEFAULT_ALIGNMENT);
    }

    /**
     * Creates a MemoryPool with default buffer size (8192) and alignment (4096).
     *
     * @param totalBuffers total number of buffers in the pool
     */
    public MemoryPool(int totalBuffers) {
        this(totalBuffers, DEFAULT_BUFFER_SIZE, DEFAULT_ALIGNMENT);
    }

    // ---- Stripe selection ----

    /**
     * Select the stripe for the current thread.
     * Uses {@code Thread.currentThread().threadId() & stripeMask} for
     * zero-allocation, branch-free stripe selection.
     */
    private Stripe selectStripe() {
        return stripes[(int) (Thread.currentThread().threadId() & stripeMask)];
    }

    /**
     * Returns the stripe index for the current thread (visible for testing).
     */
    int stripeIndexForCurrentThread() {
        return (int) (Thread.currentThread().threadId() & stripeMask);
    }

    // ---- Acquire ----

    /**
     * Acquire a buffer from the pool.
     *
     * <p>Strategy:
     * <ol>
     *   <li>Try the thread's selected stripe</li>
     *   <li>Work-steal from adjacent stripes</li>
     *   <li>If demand read: block with timeout on selected stripe</li>
     *   <li>If prefetch: fail-fast with {@link MemoryBackpressureException}</li>
     * </ol>
     *
     * <p>Buffers already have a {@link Cleaner} registered at creation time,
     * so acquire() simply hands out the buffer as-is.
     *
     * @param isDemandRead true for demand reads (block), false for prefetch (fail-fast)
     * @param timeoutMs    timeout in milliseconds for demand reads (ignored for prefetch)
     * @return a writable ByteBuffer of {@link #bufferSize} bytes
     * @throws MemoryBackpressureException if prefetch and pool exhausted
     * @throws PoolAcquireTimeoutException if demand read times out
     */
    public ByteBuffer acquire(boolean isDemandRead, long timeoutMs) {
        if (closed) {
            throw new IllegalStateException("MemoryPool is closed");
        }

        // Fault injection: beforePoolAcquire
        if (FaultInjectionConfig.FAULT_INJECTION_ENABLED) {
            faultInjector.beforePoolAcquire();
        }

        metrics.acquireCount.increment();

        // 1. Try selected stripe
        Stripe stripe = selectStripe();
        ByteBuffer buf = stripe.tryTake();
        if (buf != null) {
            return buf;
        }

        // 2. Work-steal from adjacent stripes
        buf = stealFromOtherStripes(stripe);
        if (buf != null) {
            metrics.stealCount.increment();
            return buf;
        }

        // 3. Backpressure
        if (!isDemandRead) {
            metrics.prefetchSkipCount.increment();
            throw new MemoryBackpressureException("Pool exhausted, prefetch skipped");
        }

        // 4. Demand read: block with timeout on selected stripe
        long startNanos = System.nanoTime();
        stripe.lock.lock();
        try {
            long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (true) {
                // Check own stripe
                if (!stripe.freeList.isEmpty()) {
                    buf = stripe.freeList.pollFirst();
                    break;
                }
                // Try work-stealing while holding the lock (quick scan)
                stripe.lock.unlock();
                try {
                    buf = stealFromOtherStripes(stripe);
                    if (buf != null) {
                        metrics.stealCount.increment();
                        long elapsed = System.nanoTime() - startNanos;
                        metrics.acquireWaitTimeNanos.add(elapsed);
                        return buf;
                    }
                } finally {
                    stripe.lock.lock();
                }
                if (remainingNanos <= 0) {
                    metrics.poolTimeoutCount.increment();
                    throw new PoolAcquireTimeoutException(
                        "Pool acquire timeout after " + timeoutMs + "ms"
                    );
                }
                try {
                    remainingNanos = stripe.notEmpty.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    metrics.poolTimeoutCount.increment();
                    throw new PoolAcquireTimeoutException(
                        "Pool acquire interrupted after waiting"
                    );
                }
            }
        } finally {
            stripe.lock.unlock();
            long elapsed = System.nanoTime() - startNanos;
            metrics.acquireWaitTimeNanos.add(elapsed);
        }
        return buf;
    }

    /**
     * Work-steal: scan all stripes (except the caller's) looking for a buffer.
     */
    private ByteBuffer stealFromOtherStripes(Stripe callerStripe) {
        for (int i = 0; i < numStripes; i++) {
            Stripe s = stripes[i];
            if (s == callerStripe) continue;
            ByteBuffer buf = s.tryTake();
            if (buf != null) {
                return buf;
            }
        }
        return null;
    }

    // ---- GC-based lifecycle ----

    /**
     * Register a Cleaner on the given buffer. When the buffer becomes
     * phantom-reachable (GC'd), the cleaner allocates a <em>fresh</em>
     * buffer, registers a Cleaner on it, and returns it to the pool.
     */
    private ByteBuffer registerCleaner(ByteBuffer buf) {
        final MemoryPool pool = this;
        final int size = bufferSize;
        final int alignment = alignmentBytes;
        final FaultInjector injector = faultInjector;
        cleaner.register(buf, () -> {
            pool.metrics.cleanerCallbackCount.increment();
            if (!pool.closed) {
                ByteBuffer fresh = allocateAlignedBuffer(size, alignment);
                pool.registerCleaner(fresh);  // new buffer also gets a Cleaner
                pool.returnToStripe(fresh);
            }
            // Fault injection: afterCleanerCallback
            if (FaultInjectionConfig.FAULT_INJECTION_ENABLED) {
                injector.afterCleanerCallback();
            }
        });
        return buf;
    }

    /**
     * Return a buffer to a stripe (round-robin based on current thread).
     * Used by the Cleaner callback to replenish the pool.
     * Signals all stripes so blocked demand readers can work-steal.
     */
    void returnToStripe(ByteBuffer buf) {
        if (closed) return;
        Stripe stripe = selectStripe();
        stripe.returnBuffer(buf);
        // Signal all other stripes so blocked threads can wake up and work-steal
        for (Stripe s : stripes) {
            if (s != stripe) {
                s.lock.lock();
                try {
                    s.notEmpty.signal();
                } finally {
                    s.lock.unlock();
                }
            }
        }
    }

    /**
     * Recycle a buffer back to the pool. Clears the buffer and returns it
     * to a stripe. No Cleaner manipulation — the buffer already has one
     * from creation; the pool holds a strong reference so GC won't collect
     * it and the Cleaner stays dormant.
     *
     * <p>Used on I/O failure paths (Category A/B) where the buffer is safe
     * to reuse because the kernel has returned control.</p>
     *
     * @param buf the buffer to recycle
     */
    void recycle(ByteBuffer buf) {
        if (closed) return;
        metrics.recycleCount.increment();
        buf.clear();
        returnToStripe(buf);
    }

    /**
     * Allocate a one-time-use buffer with built-in direct memory pressure guard.
     * The buffer is NOT from the pool and has NO Cleaner registered — it becomes
     * GC-eligible naturally after use.
     *
     * <p>Used for coalesced I/O large buffers and timeout retry temp buffers.</p>
     *
     * @param size buffer size in bytes
     * @return a direct ByteBuffer of the requested size
     * @throws MemoryBackpressureException if direct memory usage would exceed threshold
     */
    public ByteBuffer allocateOneTimeBuffer(int size) {
        long used = getUsedDirectMemory();
        long max = getMaxDirectMemory();
        if (max > 0 && used + size > (long) (max * directMemoryThreshold)) {
            throw new MemoryBackpressureException(
                "Direct memory pressure: used=" + used
                    + ", requested=" + size
                    + ", max=" + max
                    + ", threshold=" + directMemoryThreshold);
        }
        return allocateAlignedBuffer(size, alignmentBytes);
    }

    /**
     * Set the direct memory threshold for one-time-use buffer allocation.
     *
     * @param t threshold as a fraction of max direct memory (0.0 exclusive, 1.0 inclusive)
     * @throws IllegalArgumentException if t is not in (0.0, 1.0]
     */
    public void setDirectMemoryThreshold(double t) {
        if (t <= 0.0 || t > 1.0) throw new IllegalArgumentException("threshold: " + t);
        directMemoryThreshold = t;
    }

    /** Returns the current direct memory threshold. */
    public double getDirectMemoryThreshold() {
        return directMemoryThreshold;
    }

    private static long getUsedDirectMemory() {
        for (var pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if ("direct".equals(pool.getName())) {
                return pool.getMemoryUsed();
            }
        }
        return 0;
    }

    private static long getMaxDirectMemory() {
        try {
            // Use reflection to access jdk.internal.misc.VM.maxDirectMemory()
            // to avoid compile-time dependency on internal API
            Class<?> vmClass = Class.forName("jdk.internal.misc.VM");
            java.lang.reflect.Method method = vmClass.getDeclaredMethod("maxDirectMemory");
            return (long) method.invoke(null);
        } catch (Exception e) {
            return Runtime.getRuntime().maxMemory();
        }
    }

    // ---- jemalloc integration ----

    /**
     * Allocate a DirectIO-aligned ByteBuffer. Uses jemalloc via FFM if
     * available, otherwise falls back to {@link ByteBuffer#allocateDirect}.
     *
     * @param size      buffer size in bytes
     * @param alignment alignment in bytes (0 for unaligned)
     * @return a direct ByteBuffer of the requested size
     */
    static ByteBuffer allocateAlignedBuffer(int size, int alignment) {
        if (JEMALLOC_AVAILABLE && alignment > 0) {
            return allocateViaJemalloc(size, alignment);
        }
        return allocateFallback(size, alignment);
    }

    /**
     * Allocate via jemalloc's aligned_alloc through FFM.
     */
    private static ByteBuffer allocateViaJemalloc(int size, int alignment) {
        try {
            MemorySegment ptr = (MemorySegment) JEMALLOC_ALIGNED_ALLOC.invokeExact(
                (long) alignment, (long) size
            );
            if (ptr.equals(MemorySegment.NULL)) {
                // jemalloc returned null — fall back
                return allocateFallback(size, alignment);
            }
            // Reinterpret the pointer to the requested size so we can
            // create a ByteBuffer from it
            MemorySegment segment = ptr.reinterpret(size);
            return segment.asByteBuffer();
        } catch (Throwable t) {
            // FFM call failed — fall back
            return allocateFallback(size, alignment);
        }
    }

    /**
     * Fallback allocation using {@link ByteBuffer#allocateDirect}.
     * When alignment is required, over-allocates and slices to an aligned
     * offset.
     */
    private static ByteBuffer allocateFallback(int size, int alignment) {
        if (alignment <= 0) {
            return ByteBuffer.allocateDirect(size);
        }
        // Over-allocate to guarantee alignment
        ByteBuffer raw = ByteBuffer.allocateDirect(size + alignment);
        long address = MemorySegment.ofBuffer(raw).address();
        int offset = (int) (alignment - (address % alignment)) % alignment;
        raw.position(offset).limit(offset + size);
        return raw.slice();
    }

    /** Returns true if jemalloc is available via FFM. */
    public static boolean isJemallocAvailable() {
        return JEMALLOC_AVAILABLE;
    }

    // ---- Metrics ----

    /**
     * Observable metrics for the MemoryPool.
     */
    public static final class PoolMetrics {
        /** Total number of acquire() calls. */
        public final LongAdder acquireCount = new LongAdder();
        /** Cumulative wait time in nanoseconds for demand-read blocking. */
        public final LongAdder acquireWaitTimeNanos = new LongAdder();
        /** Number of prefetch operations skipped due to pool exhaustion. */
        public final LongAdder prefetchSkipCount = new LongAdder();
        /** Number of demand reads that timed out waiting for a buffer. */
        public final LongAdder poolTimeoutCount = new LongAdder();
        /** Number of Cleaner callbacks that replenished the pool. */
        public final LongAdder cleanerCallbackCount = new LongAdder();
        /** Number of successful work-steals from adjacent stripes. */
        public final LongAdder stealCount = new LongAdder();
        /** Number of recycle() calls (buffers returned to pool on failure paths). */
        public final LongAdder recycleCount = new LongAdder();

        PoolMetrics() {}

        /** Returns the total acquire wait time in milliseconds. */
        public long acquireWaitTimeMs() {
            return TimeUnit.NANOSECONDS.toMillis(acquireWaitTimeNanos.sum());
        }

        @Override
        public String toString() {
            return "PoolMetrics[acquires=" + acquireCount.sum()
                + ", waitTimeMs=" + acquireWaitTimeMs()
                + ", prefetchSkips=" + prefetchSkipCount.sum()
                + ", timeouts=" + poolTimeoutCount.sum()
                + ", cleanerCallbacks=" + cleanerCallbackCount.sum()
                + ", steals=" + stealCount.sum() + "]";
        }
    }

    /** Returns the pool metrics. */
    public PoolMetrics metrics() {
        return metrics;
    }

    // ---- Accessors ----

    /** Returns the total number of buffers managed by this pool. */
    public int totalBuffers() {
        return totalBuffers;
    }

    /** Returns the buffer size in bytes. */
    public int bufferSize() {
        return bufferSize;
    }

    /** Returns the alignment in bytes (0 if unaligned). */
    public int alignmentBytes() {
        return alignmentBytes;
    }

    /** Returns the number of stripes. */
    public int numStripes() {
        return numStripes;
    }

    /** Returns the fault injector. */
    public FaultInjector faultInjector() {
        return faultInjector;
    }

    /** Returns the current number of available buffers across all stripes. */
    public int availableBuffers() {
        int count = 0;
        for (Stripe s : stripes) {
            count += s.size();
        }
        return count;
    }

    /** Returns true if the pool is closed. */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
        // Drain all stripes
        for (Stripe s : stripes) {
            s.lock.lock();
            try {
                s.freeList.clear();
                s.notEmpty.signalAll();
            } finally {
                s.lock.unlock();
            }
        }
    }
}
