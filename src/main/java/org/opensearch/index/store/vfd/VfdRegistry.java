/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Logger;

import org.opensearch.index.store.bufferpoolfs.RadixBlockTable;

/**
 * Central per-node registry that manages path-to-VFD mapping, per-VFD
 * RadixBlockTables (L1 cache), and the Caffeine FD cache holding
 * {@link RefCountedChannel} values.
 *
 * <p>The VfdRegistry enforces the unique-per-path invariant: at most one
 * {@link VirtualFileDescriptor} exists per normalized absolute file path.
 * Multiple root IndexInput instances sharing the same file reuse a single
 * VFD via reference counting.</p>
 *
 * <p>FileChannel lifecycle is managed by a Caffeine cache keyed by VFD id.
 * Eviction drops the cache's base ref on the {@link RefCountedChannel};
 * the channel closes when all refs (cache + in-flight I/O) are released.</p>
 *
 * <p><b>Satisfies: Requirements 1, 2, 3, 4, 25, 51</b></p>
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
public class VfdRegistry implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(VfdRegistry.class.getName());

    /** Base backoff for acquireChannel retry in nanoseconds (100 µs). */
    static final long BASE_BACKOFF_NANOS = 100_000L;
    /** Max backoff shift to cap exponential growth. */
    static final int MAX_BACKOFF_SHIFT = 10;
    /** Max backoff in nanoseconds (roughly 100 ms). */
    static final long MAX_BACKOFF_NANOS = 100_000_000L;
    /** Default timeout for FD open limiter acquire in milliseconds. */
    static final long DEFAULT_FD_OPEN_TIMEOUT_MS = 5_000L;

    // ---- Core data structures ----

    /**
     *
     * Normalized absolute path → VFD. Unique-per-path invariant enforced
     * by {@link ConcurrentHashMap#compute}.
     */
    private final ConcurrentHashMap<String, VirtualFileDescriptor> pathToVfd;

    /**
     * VFD numeric ID → per-file L1 cache (RadixBlockTable of MemorySegment).
     */
    private final ConcurrentHashMap<Long, RadixBlockTable<MemorySegment>> idToRadixBlockTable;

    /**
     * Node-level inflight I/O deduplication map. Keys are VirtualPage-encoded
     * (vfdId + blockId). Shared across all VirtualFileChannel instances so that
     * independent readers of the same file correctly deduplicate I/O.
     */
    private final ConcurrentHashMap<Long, CompletableFuture<MemorySegment>> inflightMap;

    /**
     * VFD numeric ID → RefCountedChannel. Caffeine cache with max size = fdLimit.
     * Eviction listener drops the cache's base ref on the RefCountedChannel.
     */
    private final Cache<Long, RefCountedChannel> fdCache;

    /**
     * Hard cap on concurrent open FileChannels. Permits = fdLimit + fdBurstCapacity.
     * Fair ordering (FIFO) to prevent starvation.
     */
    private final Semaphore fdOpenLimiter;

    /**
     * Monotonically increasing VFD ID generator. Starts at 1 (0 is reserved).
     */
    private final AtomicLong vfdIdGenerator;

    /** Timeout in ms for acquiring an FD open limiter permit. */
    private final long fdOpenTimeoutMs;

    /** The configured FD limit (base). */
    private final int fdLimit;

    /** The configured burst capacity. */
    private final int fdBurstCapacity;

    /** Dedicated executor for Caffeine eviction listener callbacks. */
    private final ExecutorService evictionExecutor;

    /** Observable metrics. */
    private final RegistryMetrics metrics;

    /** Fault injector for testing. */
    private final FaultInjector faultInjector;

    /**
     * Optional reference to the L2 BlockCache. Set after construction via
     * {@link #setBlockCache} to break the circular dependency (BlockCache
     * takes VfdRegistry in its constructor). When set, {@link #release}
     * eagerly evicts orphaned L2 entries for a VFD whose refCount drops to 0.
     */
    private volatile BlockCache blockCache;

    private volatile boolean closed;

    // ---- Constructor ----

    /**
     * Creates a new VfdRegistry.
     *
     * @param fdLimit          base limit for open FileChannels (Caffeine max size)
     * @param fdBurstCapacity  additional burst capacity beyond fdLimit
     * @param fdOpenTimeoutMs  timeout in ms for FD open limiter acquire
     */
    public VfdRegistry(int fdLimit, int fdBurstCapacity, long fdOpenTimeoutMs) {
        this(fdLimit, fdBurstCapacity, fdOpenTimeoutMs, NoOpFaultInjector.INSTANCE);
    }

    /**
     * Creates a new VfdRegistry with a custom FaultInjector.
     *
     * @param fdLimit          base limit for open FileChannels (Caffeine max size)
     * @param fdBurstCapacity  additional burst capacity beyond fdLimit
     * @param fdOpenTimeoutMs  timeout in ms for FD open limiter acquire
     * @param faultInjector    the fault injector for testing
     */
    public VfdRegistry(int fdLimit, int fdBurstCapacity, long fdOpenTimeoutMs,
                       FaultInjector faultInjector) {
        if (fdLimit <= 0) {
            throw new IllegalArgumentException("fdLimit must be positive: " + fdLimit);
        }
        if (fdBurstCapacity < 0) {
            throw new IllegalArgumentException("fdBurstCapacity must be non-negative: " + fdBurstCapacity);
        }
        this.fdLimit = fdLimit;
        this.fdBurstCapacity = fdBurstCapacity;
        this.fdOpenTimeoutMs = fdOpenTimeoutMs;
        this.pathToVfd = new ConcurrentHashMap<>();
        this.idToRadixBlockTable = new ConcurrentHashMap<>();
        this.inflightMap = new ConcurrentHashMap<>();
        this.vfdIdGenerator = new AtomicLong(0); // first id will be 1
        this.fdOpenLimiter = new Semaphore(fdLimit + fdBurstCapacity, true);
        this.metrics = new RegistryMetrics();
        this.faultInjector = faultInjector;
        this.closed = false;

        this.evictionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "vfd-eviction-listener");
            t.setDaemon(true);
            return t;
        });

        this.fdCache = Caffeine.newBuilder()
            .maximumSize(fdLimit)
            .removalListener((Long vfdId, RefCountedChannel refChannel, RemovalCause cause) -> {
                if (refChannel != null) {
                    refChannel.release(); // drop cache base ref
                    if (cause.wasEvicted()) {
                        metrics.fdEvictionCount.increment();
                    }
                    // Fault injection: afterChannelClose
                    if (FaultInjectionConfig.FAULT_INJECTION_ENABLED && vfdId != null) {
                        faultInjector.afterChannelClose(vfdId);
                    }
                }
            })
            .executor(evictionExecutor)
            .build();
    }

    /**
     * Creates a VfdRegistry with default FD open timeout.
     */
    public VfdRegistry(int fdLimit, int fdBurstCapacity) {
        this(fdLimit, fdBurstCapacity, DEFAULT_FD_OPEN_TIMEOUT_MS);
    }

    // ---- VFD Acquisition ----

    /**
     * Acquire a VFD for the given file path. If a VFD already exists for
     * this path and is still alive, its ref count is incremented and the
     * existing VFD is returned. Otherwise, a new VFD is created.
     *
     * <p>Uses {@link ConcurrentHashMap#compute} for atomic per-key
     * operation — no external locking needed.</p>
     *
     * @param filePath the file path to acquire a VFD for
     * @return the VFD (existing or newly created)
     */
    public VirtualFileDescriptor acquire(Path filePath) {
        String key = filePath.toAbsolutePath().normalize().toString();
        return pathToVfd.compute(key, (k, existing) -> {
            if (existing != null && existing.tryIncRef()) {
                metrics.vfdReuseCount.increment();
                return existing; // reuse, ref count incremented
            }
            // existing is null or ref count was 0 (race with close)
            long id = vfdIdGenerator.incrementAndGet();
            assert id <= VirtualPage.MAX_VFD_ID : "VFD_ID exhaustion — node restart required";
            VirtualFileDescriptor vfd = new VirtualFileDescriptor(id, filePath, this);
            idToRadixBlockTable.put(id, new RadixBlockTable<>());
            metrics.vfdCreateCount.increment();
            return vfd;
        });
    }

    // ---- VFD Release ----

    /**
     * Release a VFD. Decrements the ref count. If it reaches 0:
     * <ol>
     *   <li>Conditionally removes the VFD from pathToVfd</li>
     *   <li>Clears and removes the RadixBlockTable</li>
     *   <li>Invalidates the fdCache entry (triggers eviction listener)</li>
     *   <li>Invokes onClose callbacks</li>
     * </ol>
     *
     * @param vfd the VFD to release
     */
    public void release(VirtualFileDescriptor vfd) {
        if (vfd.decRef() == 0) {
            // Conditional remove: only removes if the value matches,
            // preventing a race where a new VFD was already created for the same path
            pathToVfd.remove(vfd.normalizedPath(), vfd);
            RadixBlockTable<MemorySegment> rbt = idToRadixBlockTable.remove(vfd.id());
            if (rbt != null) {
                rbt.clear();
            }
            // Eagerly evict orphaned L2 cache entries for this VFD.
            // This prevents pool buffer drain when VFD IDs are recycled:
            // without this, old L2 entries (keyed by old VFD ID) hold pool
            // buffers but no reader will ever hit them.
            long vfdId = vfd.id();
            BlockCache bc = this.blockCache; // volatile read once
            if (bc != null) {
                bc.invalidateForVfd(vfdId);
            }
            // Clear any inflight entries for this VFD
            //ToDo how do we handle completed IO if they are not present in inflightMap. Also inflightMap
            //Should be renamed to inflightIOMap
            inflightMap.entrySet().removeIf(e -> VirtualPage.extractVfdId(e.getKey()) == vfdId);
            fdCache.invalidate(vfd.id()); // triggers removalListener → refChannel.release()
            vfd.invokeOnCloseCallbacks();
            metrics.vfdCloseCount.increment();
        }
    }

    // ---- Channel Acquisition ----

    /**
     * Acquire a live FileChannel for the given VFD. Returns an
     * {@link AcquiredChannel} that the caller MUST release() in a finally
     * block after I/O.
     *
     * <p>Uses a while(true) retry loop with Caffeine compute-if-absent +
     * {@link RefCountedChannel#acquire()} CAS. If acquire() returns null
     * (eviction race), conditionally removes the dead entry and retries
     * with exponential backoff + jitter.</p>
     *
     * @param vfd the VFD to acquire a channel for
     * @return an AcquiredChannel wrapping the live FileChannel
     * @throws IOException if the channel cannot be opened
     * @throws FdLimitExhaustedException if the FD open limiter times out
     */
    public AcquiredChannel acquireChannel(VirtualFileDescriptor vfd) throws IOException {
        // Fault injection: beforeChannelAcquire
        if (FaultInjectionConfig.FAULT_INJECTION_ENABLED) {
            faultInjector.beforeChannelAcquire(vfd.id());
        }

        int retryCount = 0;
        while (true) {
            RefCountedChannel refChannel;
            try {
                refChannel = fdCache.get(vfd.id(), id -> {
                    // Cache miss: open new FileChannel, acquire FD_Open_Limiter permit
                    boolean permitAcquired = false;
                    try {
                        permitAcquired = fdOpenLimiter.tryAcquire(fdOpenTimeoutMs, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(
                            new FdLimitExhaustedException("FD_Open_Limiter acquire interrupted"));
                    }
                    if (!permitAcquired) {
                        throw new RuntimeException(
                            new FdLimitExhaustedException("FD_Open_Limiter timeout after " + fdOpenTimeoutMs + "ms"));
                    }
                    try {
                        FileChannel fc = FileChannel.open(vfd.filePath(), StandardOpenOption.READ);
                        metrics.fdOpenCount.incrementAndGet();
                        metrics.fdReopenCount.increment();
                        return new RefCountedChannel(fc, fdOpenLimiter);
                    } catch (Exception e) {
                        fdOpenLimiter.release(); // give back permit on open failure
                        throw new RuntimeException(e);
                    }
                });
            } catch (RuntimeException e) {
                // Unwrap checked exceptions from Caffeine's mapping function
                Throwable cause = e.getCause();
                if (cause instanceof FdLimitExhaustedException) {
                    throw (FdLimitExhaustedException) cause;
                }
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                throw e;
            }

            FileChannel ch = refChannel.acquire(); // CAS increment
            if (ch != null) {
                return new AcquiredChannel(refChannel, ch);
            }

            // acquire() returned null — eviction race. Remove dead entry and retry.
            fdCache.asMap().remove(vfd.id(), refChannel); // conditional remove
            metrics.acquireChannelRetryCount.increment();

            if (retryCount > 0) {
                long backoffNanos = Math.min(
                    BASE_BACKOFF_NANOS << Math.min(retryCount, MAX_BACKOFF_SHIFT),
                    MAX_BACKOFF_NANOS);
                long jitter = ThreadLocalRandom.current().nextLong(backoffNanos / 4 + 1);
                LockSupport.parkNanos(backoffNanos + jitter);
            }
            retryCount++;
        }
    }

    // ---- AcquiredChannel record ----

    /**
     * Returned by {@link #acquireChannel}. Wraps a live FileChannel and
     * the RefCountedChannel it was acquired from. Caller MUST call
     * {@link #release()} in a finally block after I/O completes.
     */
    public record AcquiredChannel(RefCountedChannel refChannel, FileChannel channel) {
        /** Release the I/O ref on the RefCountedChannel. */
        public void release() {
            refChannel.release();
        }
    }

    // ---- RadixBlockTable access ----

    /**
     * Returns the RadixBlockTable for the given VFD id, or null if not found.
     * Used by BlockCache eviction cascade to remove L1 entries.
     */
    public RadixBlockTable<MemorySegment> getRadixBlockTable(long vfdId) {
        return idToRadixBlockTable.get(vfdId);
    }

    /**
     * Returns the node-level inflight I/O deduplication map.
     * Shared across all VirtualFileChannel instances.
     */
    public ConcurrentHashMap<Long, CompletableFuture<MemorySegment>> inflightMap() {
        return inflightMap;
    }


    /**
     * Wire the L2 BlockCache into this registry. Must be called after
     * BlockCache construction to break the circular dependency.
     * When set, {@link #release} will eagerly evict orphaned L2 entries
     * for VFDs whose refCount drops to 0.
     *
     * @param blockCache the L2 block cache
     */
    public void setBlockCache(BlockCache blockCache) {
        this.blockCache = blockCache;
    }

    // ---- Accessors ----

    /** Returns the path-to-VFD map size (number of active VFDs). */
    public int activeVfdCount() {
        return pathToVfd.size();
    }

    /** Returns the FD cache estimated size. */
    public long fdCacheSize() {
        return fdCache.estimatedSize();
    }

    /** Returns the FD open limiter's available permits. */
    public int availableFdPermits() {
        return fdOpenLimiter.availablePermits();
    }

    /** Returns the total FD permits (fdLimit + fdBurstCapacity). */
    public int totalFdPermits() {
        return fdLimit + fdBurstCapacity;
    }

    /** Returns the configured FD limit. */
    public int fdLimit() {
        return fdLimit;
    }

    /** Returns the configured burst capacity. */
    public int fdBurstCapacity() {
        return fdBurstCapacity;
    }

    /** Returns the VFD ID generator's current value (last assigned ID). */
    public long lastAssignedVfdId() {
        return vfdIdGenerator.get();
    }

    /** Returns the FD open limiter semaphore (for RefCountedChannel). */
    public Semaphore fdOpenLimiter() {
        return fdOpenLimiter;
    }

    /** Returns the registry metrics. */
    public RegistryMetrics metrics() {
        return metrics;
    }

    /** Returns the fault injector. */
    public FaultInjector faultInjector() {
        return faultInjector;
    }

    /** Returns true if the registry is closed. */
    public boolean isClosed() {
        return closed;
    }

    /** Perform Caffeine maintenance (for testing). */
    public void cleanUp() {
        fdCache.cleanUp();
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        // Invalidate all cache entries — triggers eviction listeners
        fdCache.invalidateAll();
        fdCache.cleanUp();
        evictionExecutor.shutdown();
        try {
            if (!evictionExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                evictionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            evictionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        inflightMap.clear();
        pathToVfd.clear();
        idToRadixBlockTable.clear();
    }

    // ---- RegistryMetrics ----

    /**
     * Observable metrics for the VfdRegistry.
     * Uses {@link java.util.concurrent.atomic.LongAdder} for counters
     * and {@link AtomicLong} for gauges.
     */
    public static final class RegistryMetrics {
        /** Number of new VFDs created. */
        public final java.util.concurrent.atomic.LongAdder vfdCreateCount =
            new java.util.concurrent.atomic.LongAdder();
        /** Number of VFD reuses (existing VFD returned with incRef). */
        public final java.util.concurrent.atomic.LongAdder vfdReuseCount =
            new java.util.concurrent.atomic.LongAdder();
        /** Number of VFDs closed (refCount reached 0). */
        public final java.util.concurrent.atomic.LongAdder vfdCloseCount =
            new java.util.concurrent.atomic.LongAdder();
        /** Current number of open FDs (gauge). */
        public final AtomicLong fdOpenCount = new AtomicLong(0);
        /** Number of FD evictions from Caffeine cache. */
        public final java.util.concurrent.atomic.LongAdder fdEvictionCount =
            new java.util.concurrent.atomic.LongAdder();
        /** Number of FD reopens (cache miss → new FileChannel). */
        public final java.util.concurrent.atomic.LongAdder fdReopenCount =
            new java.util.concurrent.atomic.LongAdder();
        /** Current number of zombie RefCountedChannels (evicted but still in-flight). */
        public final AtomicLong fdZombieCount = new AtomicLong(0);
        /** Number of acquireChannel retry iterations (eviction race). */
        public final java.util.concurrent.atomic.LongAdder acquireChannelRetryCount =
            new java.util.concurrent.atomic.LongAdder();

        RegistryMetrics() {}

        @Override
        public String toString() {
            return "RegistryMetrics[creates=" + vfdCreateCount.sum()
                + ", reuses=" + vfdReuseCount.sum()
                + ", closes=" + vfdCloseCount.sum()
                + ", fdOpen=" + fdOpenCount.get()
                + ", fdEvictions=" + fdEvictionCount.sum()
                + ", fdReopens=" + fdReopenCount.sum()
                + ", fdZombies=" + fdZombieCount.get()
                + ", channelRetries=" + acquireChannelRetryCount.sum() + "]";
        }
    }
}
