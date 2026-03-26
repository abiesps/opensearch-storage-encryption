/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opensearch.index.store.bufferpoolfs.RadixBlockTable;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

/**
 * I/O coordination layer that owns the Inflight_Map for deduplication,
 * delegates to a pluggable {@link IOBackend}, and manages the buffer
 * lifecycle from {@link MemoryPool} acquisition through to L1/L2 cache
 * population.
 *
 * <p>This class implements the core read path:
 * L2 check → inflight join → become I/O owner → acquire inflight permit
 * → coalesced physical read → cache result → complete future.</p>
 *
 * <p>I/O coalescing merges nearby block reads into a single larger I/O
 * operation, reducing syscall count and improving sequential throughput.</p>
 *
 * <p><b>Satisfies: Requirements 5, 6, 7, 8, 30, 31, 41</b></p>
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
public class VirtualFileChannel implements ReadChannel {

    private static final Logger logger = Logger.getLogger(VirtualFileChannel.class.getName());

    private static final int CACHE_BLOCK_SIZE = StaticConfigs.CACHE_BLOCK_SIZE;
    private static final int CACHE_BLOCK_SIZE_POWER = StaticConfigs.CACHE_BLOCK_SIZE_POWER;

    private final VirtualFileDescriptor vfd;
    private final IOBackend ioBackend;
    private final MemoryPool memoryPool;
    private final BlockCache blockCache;
    private final VfdRegistry registry;
    private final ConcurrentHashMap<Long, CompletableFuture<MemorySegment>> inflightMap;
    private final Semaphore inflightLimiter;
    private final long inflightTimeoutMs;
    private final ExecutorService prefetchExecutor;
    private final ChannelMetrics metrics;
    private final int maxCoalescedIoSize;
    private final FaultInjector faultInjector;
    private final BlockDecryptor blockDecryptor;
    private final boolean coalescingEnabled;

    /** Default inflight timeout: 30 seconds. */
    public static final long DEFAULT_INFLIGHT_TIMEOUT_MS = 30_000L;

    /** Default inflight limiter permits. */
    public static final int DEFAULT_INFLIGHT_PERMITS = 64;

    /** Default pool acquire timeout for demand reads: 5 seconds. */
    public static final long DEFAULT_POOL_ACQUIRE_TIMEOUT_MS = 5_000L;

    /** Default max coalesced I/O size: 128KB (16 × CACHE_BLOCK_SIZE). */
    public static final int DEFAULT_MAX_COALESCED_IO_SIZE = 16 * CACHE_BLOCK_SIZE;

    /**
     * Creates a new VirtualFileChannel.
     *
     * @param vfd               the virtual file descriptor
     * @param ioBackend         the I/O backend for physical reads
     * @param memoryPool        the buffer pool
     * @param blockCache        the L2 block cache
     * @param registry          the VFD registry
     * @param inflightLimiter   semaphore limiting concurrent physical I/O
     * @param inflightTimeoutMs timeout in ms for inflight waits
     * @param prefetchExecutor  executor for async prefetch (may be null for core path)
     */
    public VirtualFileChannel(
            VirtualFileDescriptor vfd,
            IOBackend ioBackend,
            MemoryPool memoryPool,
            BlockCache blockCache,
            VfdRegistry registry,
            Semaphore inflightLimiter,
            long inflightTimeoutMs,
            ExecutorService prefetchExecutor) {
        this(vfd, ioBackend, memoryPool, blockCache, registry,
             inflightLimiter, inflightTimeoutMs, prefetchExecutor,
             DEFAULT_MAX_COALESCED_IO_SIZE, NoOpFaultInjector.INSTANCE);
    }

    /**
     * Creates a new VirtualFileChannel with configurable coalesced I/O size.
     *
     * @param vfd                the virtual file descriptor
     * @param ioBackend          the I/O backend for physical reads
     * @param memoryPool         the buffer pool
     * @param blockCache         the L2 block cache
     * @param registry           the VFD registry
     * @param inflightLimiter    semaphore limiting concurrent physical I/O
     * @param inflightTimeoutMs  timeout in ms for inflight waits
     * @param prefetchExecutor   executor for async prefetch (may be null for core path)
     * @param maxCoalescedIoSize maximum coalesced I/O size in bytes
     */
    public VirtualFileChannel(
            VirtualFileDescriptor vfd,
            IOBackend ioBackend,
            MemoryPool memoryPool,
            BlockCache blockCache,
            VfdRegistry registry,
            Semaphore inflightLimiter,
            long inflightTimeoutMs,
            ExecutorService prefetchExecutor,
            int maxCoalescedIoSize,
            FaultInjector faultInjector) {
        this(vfd, ioBackend, memoryPool, blockCache, registry,
             inflightLimiter, inflightTimeoutMs, prefetchExecutor,
             maxCoalescedIoSize, faultInjector, BlockDecryptor.NOOP);
    }

    /**
     * Creates a new VirtualFileChannel with all options including block decryptor.
     *
     * @param vfd                the virtual file descriptor
     * @param ioBackend          the I/O backend for physical reads
     * @param memoryPool         the buffer pool
     * @param blockCache         the L2 block cache
     * @param registry           the VFD registry
     * @param inflightLimiter    semaphore limiting concurrent physical I/O
     * @param inflightTimeoutMs  timeout in ms for inflight waits
     * @param prefetchExecutor   executor for async prefetch (may be null for core path)
     * @param maxCoalescedIoSize maximum coalesced I/O size in bytes
     * @param faultInjector      the fault injector for testing
     * @param blockDecryptor     decryption callback invoked after I/O, before caching
     */
    public VirtualFileChannel(
            VirtualFileDescriptor vfd,
            IOBackend ioBackend,
            MemoryPool memoryPool,
            BlockCache blockCache,
            VfdRegistry registry,
            Semaphore inflightLimiter,
            long inflightTimeoutMs,
            ExecutorService prefetchExecutor,
            int maxCoalescedIoSize,
            FaultInjector faultInjector,
            BlockDecryptor blockDecryptor) {
        this(vfd, ioBackend, memoryPool, blockCache, registry,
             inflightLimiter, inflightTimeoutMs, prefetchExecutor,
             maxCoalescedIoSize, faultInjector, blockDecryptor, true);
    }

    /**
     * Creates a new VirtualFileChannel with all options including coalescing control.
     *
     * @param vfd                the virtual file descriptor
     * @param ioBackend          the I/O backend for physical reads
     * @param memoryPool         the buffer pool
     * @param blockCache         the L2 block cache
     * @param registry           the VFD registry
     * @param inflightLimiter    semaphore limiting concurrent physical I/O
     * @param inflightTimeoutMs  timeout in ms for inflight waits
     * @param prefetchExecutor   executor for async prefetch (may be null for core path)
     * @param maxCoalescedIoSize maximum coalesced I/O size in bytes
     * @param faultInjector      the fault injector for testing
     * @param blockDecryptor     decryption callback invoked after I/O, before caching
     * @param coalescingEnabled  true to enable I/O coalescing, false for single-block reads only
     */
    public VirtualFileChannel(
            VirtualFileDescriptor vfd,
            IOBackend ioBackend,
            MemoryPool memoryPool,
            BlockCache blockCache,
            VfdRegistry registry,
            Semaphore inflightLimiter,
            long inflightTimeoutMs,
            ExecutorService prefetchExecutor,
            int maxCoalescedIoSize,
            FaultInjector faultInjector,
            BlockDecryptor blockDecryptor,
            boolean coalescingEnabled) {
        this.vfd = vfd;
        this.ioBackend = ioBackend;
        this.memoryPool = memoryPool;
        this.blockCache = blockCache;
        this.registry = registry;
        this.inflightMap = registry.inflightMap();
        this.inflightLimiter = inflightLimiter;
        this.inflightTimeoutMs = inflightTimeoutMs;
        this.prefetchExecutor = prefetchExecutor;
        this.metrics = new ChannelMetrics();
        this.maxCoalescedIoSize = maxCoalescedIoSize;
        this.faultInjector = faultInjector;
        this.blockDecryptor = blockDecryptor;
        this.coalescingEnabled = coalescingEnabled;
    }

    /**
     * Creates a new VirtualFileChannel with configurable coalesced I/O size
     * and default (no-op) fault injector.
     *
     * @param vfd                the virtual file descriptor
     * @param ioBackend          the I/O backend for physical reads
     * @param memoryPool         the buffer pool
     * @param blockCache         the L2 block cache
     * @param registry           the VFD registry
     * @param inflightLimiter    semaphore limiting concurrent physical I/O
     * @param inflightTimeoutMs  timeout in ms for inflight waits
     * @param prefetchExecutor   executor for async prefetch (may be null for core path)
     * @param maxCoalescedIoSize maximum coalesced I/O size in bytes
     */
    public VirtualFileChannel(
            VirtualFileDescriptor vfd,
            IOBackend ioBackend,
            MemoryPool memoryPool,
            BlockCache blockCache,
            VfdRegistry registry,
            Semaphore inflightLimiter,
            long inflightTimeoutMs,
            ExecutorService prefetchExecutor,
            int maxCoalescedIoSize) {
        this(vfd, ioBackend, memoryPool, blockCache, registry,
             inflightLimiter, inflightTimeoutMs, prefetchExecutor,
             maxCoalescedIoSize, NoOpFaultInjector.INSTANCE);
    }

    /**
     * Convenience constructor with default inflight timeout and no prefetch executor.
     */
    public VirtualFileChannel(
            VirtualFileDescriptor vfd,
            IOBackend ioBackend,
            MemoryPool memoryPool,
            BlockCache blockCache,
            VfdRegistry registry,
            Semaphore inflightLimiter) {
        this(vfd, ioBackend, memoryPool, blockCache, registry,
             inflightLimiter, DEFAULT_INFLIGHT_TIMEOUT_MS, null,
             DEFAULT_MAX_COALESCED_IO_SIZE, NoOpFaultInjector.INSTANCE);
    }

    // ---- Core Read Path ----

    /**
     * Read a single block. This is the core read path:
     * <ol>
     *   <li>Check L2 cache</li>
     *   <li>Check/join inflight</li>
     *   <li>Become I/O owner (putIfAbsent wins)</li>
     *   <li>Acquire inflight permit</li>
     *   <li>Physical read</li>
     *   <li>Cache result</li>
     *   <li>Complete future</li>
     * </ol>
     *
     * @param vfdId       the VFD identifier
     * @param blockId     the block identifier within the file
     * @param fileOffset  the absolute byte offset in the file
     * @param isDemandRead true for demand reads (block on backpressure),
     *                     false for prefetch (fail-fast)
     * @return the MemorySegment containing the block data
     * @throws IOException if the read fails
     */
    public MemorySegment readBlock(long vfdId, int blockId, long fileOffset,
                                    boolean isDemandRead) throws IOException {
        long key = VirtualPage.encode(vfdId, blockId);

        // 1. Check L2 cache first
        MemorySegment cached = blockCache.get(key);
        if (cached != null) {
            return cached;
        }

        // 2. Check/join inflight
        CompletableFuture<MemorySegment> existing = inflightMap.get(key);
        if (existing != null) {
            metrics.dedupedIOCount.increment();
            return joinInflight(existing, key);
        }

        // 3. Try to become the I/O owner
        CompletableFuture<MemorySegment> future = new CompletableFuture<>();
        existing = inflightMap.putIfAbsent(key, future);
        if (existing != null) {
            // Lost race — join the winner's future
            metrics.dedupedIOCount.increment();
            return joinInflight(existing, key);
        }

        // 4. We are the I/O owner — acquire Inflight_Limiter permit
        boolean permitAcquired = false;
        CompletableFuture<MemorySegment>[] coalescedFutures = null;
        CoalescedRead plan = null;
        try {
            if (isDemandRead) {
                try {
                    permitAcquired = inflightLimiter.tryAcquire(
                        inflightTimeoutMs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted acquiring inflight permit", e);
                }
                if (!permitAcquired) {
                    IOException ex = new IOException("Inflight_Limiter timeout");
                    future.completeExceptionally(ex);
                    throw ex;
                }
            } else {
                permitAcquired = inflightLimiter.tryAcquire();
                if (!permitAcquired) {
                    MemoryBackpressureException ex = new MemoryBackpressureException(
                        "Prefetch dropped — inflight limit");
                    future.completeExceptionally(ex);
                    throw ex;
                }
            }

            // 5a. Plan coalesced read — scan forward for nearby blocks
            if (coalescingEnabled) {
                plan = planCoalescedRead(vfdId, blockId);
            } else {
                plan = new CoalescedRead(blockId, 1, CACHE_BLOCK_SIZE, new boolean[]{true});
            }

            if (plan.totalBlocks() > 1) {
                // 5b. Insert futures for all owned blocks
                coalescedFutures = insertCoalescedFutures(vfdId, blockId, plan, future);

                // 5c. Coalesced physical read
                try {
                    doCoalescedPhysicalRead(plan, coalescedFutures, vfdId, isDemandRead);
                } catch (MemoryBackpressureException pressureEx) {
                    // Large buffer allocation failed — clean up coalesced futures
                    // and fall back to single-block read
                    cleanupCoalescedFutures(vfdId, blockId, coalescedFutures, pressureEx);
                    coalescedFutures = null;
                    plan = null;
                    MemorySegment result = doPhysicalRead(vfdId, blockId, fileOffset,
                                                           isDemandRead, key);
                    future.complete(result);
                    metrics.physicalIOCount.increment();
                    return result;
                }
                metrics.physicalIOCount.increment();

                // 5d. Return triggering block's result
                return future.get(0, TimeUnit.MILLISECONDS);
            } else {
                // Single-block read (no coalescing)
                MemorySegment result = doPhysicalRead(vfdId, blockId, fileOffset,
                                                       isDemandRead, key);
                future.complete(result);
                metrics.physicalIOCount.increment();
                return result;
            }

        } catch (Exception e) {
            // Complete future exceptionally for all waiters
            if (!future.isDone()) {
                future.completeExceptionally(e);
            }
            // Cleanup coalesced futures on error
            if (coalescedFutures != null && plan != null) {
                cleanupCoalescedFutures(vfdId, blockId, coalescedFutures, e);
            }
            if (e instanceof IOException) throw (IOException) e;
            if (e instanceof MemoryBackpressureException) throw (MemoryBackpressureException) e;
            throw new IOException(e);
        } finally {
            // 7. Conditional remove from Inflight_Map for trigger block
            inflightMap.remove(key, future);
            // Cleanup coalesced futures in finally (conditional remove)
            if (coalescedFutures != null && plan != null) {
                cleanupCoalescedFutures(vfdId, blockId, coalescedFutures, null);
            }
            // 8. Release Inflight_Limiter permit
            if (permitAcquired) {
                inflightLimiter.release();
            }
        }
    }

    // ---- Join Inflight ----

    /**
     * Join an existing inflight I/O operation by waiting on its future.
     * Uses bounded timeout to prevent stuck I/O from leaking entries.
     *
     * @param future the CompletableFuture to join
     * @param key    the VirtualPage key (for cleanup on timeout)
     * @return the MemorySegment result
     * @throws IOException if the wait fails
     */
    private MemorySegment joinInflight(CompletableFuture<MemorySegment> future,
                                       long key) throws IOException {
        try {
            long startNanos = System.nanoTime();
            MemorySegment result = future.get(inflightTimeoutMs, TimeUnit.MILLISECONDS);
            long elapsed = System.nanoTime() - startNanos;
            metrics.inflightWaitTimeNanos.add(elapsed);
            return result;
        } catch (TimeoutException e) {
            // Remove the timed-out entry — the I/O owner's buffer is stuck
            // in the kernel (Category C). The Cleaner will replenish the pool
            // when GC eventually collects the lost buffer.
            inflightMap.remove(key, future);
            metrics.inflightTimeoutCount.increment();

            // Become a new I/O owner and retry with a temp buffer
            int blockId = VirtualPage.extractBlockId(key);
            long vfdId = VirtualPage.extractVfdId(key);
            long fileOffset = (long) blockId * CACHE_BLOCK_SIZE;
            return doPhysicalReadWithTempBuffer(vfdId, blockId, fileOffset, key, 0);
        } catch (ExecutionException e) {
            throw new IOException("Inflight I/O failed for key " + key, e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for inflight I/O", e);
        }
    }

    // ---- Physical Read + Buffer Lifecycle ----

    /**
     * Perform the actual physical read:
     * <ol>
     *   <li>Acquire buffer from MemoryPool</li>
     *   <li>Acquire FileChannel via VfdRegistry</li>
     *   <li>Read via IOBackend</li>
     *   <li>Convert to read-only MemorySegment</li>
     *   <li>Put in L2 (BlockCache) and L1 (RadixBlockTable)</li>
     * </ol>
     */
    private MemorySegment doPhysicalRead(long vfdId, int blockId, long fileOffset,
                                          boolean isDemandRead, long key) throws IOException {
        // 1. Acquire buffer from MemoryPool
        ByteBuffer buf = memoryPool.acquire(isDemandRead, DEFAULT_POOL_ACQUIRE_TIMEOUT_MS);

        try {
            // Fault injection: beforeRead (after acquire, before kernel I/O)
            if (FaultInjectionConfig.FAULT_INJECTION_ENABLED) {
                faultInjector.beforeRead(vfdId, fileOffset);
            }

            // 2. Acquire live FileChannel via VfdRegistry
            VfdRegistry.AcquiredChannel acquired = registry.acquireChannel(vfd);
            int bytesRead;
            try {
                // 3. Physical read via IOBackend
                bytesRead = ioBackend.read(acquired.channel(), buf, fileOffset, CACHE_BLOCK_SIZE);
            } finally {
                acquired.release();
            }

            // Fault injection: afterRead
            if (FaultInjectionConfig.FAULT_INJECTION_ENABLED) {
                faultInjector.afterRead(vfdId, fileOffset, buf);
            }

            // 4. Flip buffer, decrypt in-place via MemorySegment (no-op for unencrypted)
            buf.flip();
            if (bytesRead < CACHE_BLOCK_SIZE) {
                buf.limit(bytesRead);
            }
            MemorySegment writable = MemorySegment.ofBuffer(buf);
            blockDecryptor.decrypt(writable, fileOffset);

            // 5. Buffer lifecycle: writable -> read-only -> MemorySegment for caching
            ByteBuffer readOnly = buf.asReadOnlyBuffer();
            MemorySegment segment = MemorySegment.ofBuffer(readOnly);

            // 6. Put decrypted block in L2 (BlockCache) and L1 (RadixBlockTable)
            blockCache.put(key, segment);
            RadixBlockTable<MemorySegment> rbt = registry.getRadixBlockTable(vfdId);
            if (rbt != null) {
                rbt.put(blockId, segment);
            }

            return segment;
        } catch (Exception e) {
            // Any exception here means the kernel has returned control.
            // FileChannel.read() exceptions (ClosedChannelException, AsynchronousCloseException,
            // ClosedByInterruptException, NonReadableChannelException, IOException) all indicate
            // the syscall completed. Pre-read exceptions (acquireChannel) mean the buffer never
            // reached the kernel. Either way, safe to recycle.
            memoryPool.recycle(buf);
            if (e instanceof IOException) throw (IOException) e;
            throw new IOException(e);
        }
    }

    // ---- Timeout Retry with Temp Buffer ----

    /**
     * Retry a physical read using a one-time-use temp buffer (Category C).
     * Called when a previous read timed out and the kernel may still hold
     * the original pool buffer's memory region.
     *
     * <p>The temp buffer is allocated via {@link MemoryPool#allocateOneTimeBuffer(int)}
     * (pressure-guarded, no Cleaner, not from pool). After successful I/O,
     * the decrypted content is copied into a freshly acquired pool buffer
     * for caching. The temp buffer becomes GC-eligible naturally.</p>
     *
     * @param vfdId       the VFD identifier
     * @param blockId     the block identifier
     * @param fileOffset  the absolute byte offset in the file
     * @param key         the VirtualPage key for caching
     * @param retryCount  retry attempt number (0-based, used for backoff)
     * @return the MemorySegment containing the block data
     * @throws IOException if the retry read fails
     */
    private MemorySegment doPhysicalReadWithTempBuffer(long vfdId, int blockId,
            long fileOffset, long key, int retryCount) throws IOException {

        // Exponential backoff before retry
        if (retryCount > 0) {
            long backoffMs = Math.min(10L << retryCount, 1000L);
            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during retry backoff", e);
            }
        }

        // Allocate one-time-use buffer — pressure-guarded, NO Cleaner, not from pool
        ByteBuffer tempBuf = memoryPool.allocateOneTimeBuffer(memoryPool.bufferSize());

        VfdRegistry.AcquiredChannel acquired = registry.acquireChannel(vfd);
        int bytesRead;
        try {
            bytesRead = ioBackend.read(acquired.channel(), tempBuf, fileOffset, CACHE_BLOCK_SIZE);
        } finally {
            acquired.release();
        }

        tempBuf.flip();
        if (bytesRead < CACHE_BLOCK_SIZE) {
            tempBuf.limit(bytesRead);
        }

        // Decrypt in temp buffer
        MemorySegment writable = MemorySegment.ofBuffer(tempBuf);
        blockDecryptor.decrypt(writable, fileOffset);

        // Acquire a pool buffer and copy the decrypted content into it
        ByteBuffer poolBuf = memoryPool.acquire(true, DEFAULT_POOL_ACQUIRE_TIMEOUT_MS);
        poolBuf.clear();
        tempBuf.position(0);
        poolBuf.put(tempBuf);
        poolBuf.flip();

        // Cache the pool buffer as usual
        ByteBuffer readOnly = poolBuf.asReadOnlyBuffer();
        MemorySegment segment = MemorySegment.ofBuffer(readOnly);

        blockCache.put(key, segment);
        RadixBlockTable<MemorySegment> rbt = registry.getRadixBlockTable(vfdId);
        if (rbt != null) {
            rbt.put(blockId, segment);
        }

        // tempBuf becomes GC-eligible — no Cleaner, no pool interaction
        return segment;
    }

    // ---- I/O Coalescing ----

    /**
     * Describes a coalesced read plan: the contiguous byte range to read
     * and which blocks within that range we "own" (will insert futures for).
     *
     * @param startBlockId first block in the contiguous range
     * @param totalBlocks  number of blocks in the contiguous range
     * @param totalBytes   total bytes to read (totalBlocks × CACHE_BLOCK_SIZE)
     * @param owned        boolean array; owned[i] is true if we will insert a
     *                     future for startBlockId + i
     */
    record CoalescedRead(int startBlockId, int totalBlocks, int totalBytes, boolean[] owned) {}

    /**
     * Scan forward from startBlockId up to maxBlocks. Skip blocks that are
     * already cached or already inflight — they are gaps in ownership, not
     * gaps in the I/O. The returned CoalescedRead describes the contiguous
     * byte range to read and which blocks we "own" (will insert futures for).
     *
     * @param vfdId        the VFD identifier
     * @param startBlockId the triggering block
     * @return the coalesced read plan
     */
    CoalescedRead planCoalescedRead(long vfdId, int startBlockId) {
        int maxBlocks = maxCoalescedIoSize / CACHE_BLOCK_SIZE;
        boolean[] owned = new boolean[maxBlocks];
        owned[0] = true; // the triggering block is always owned
        int endBlockId = startBlockId;

        for (int i = 1; i < maxBlocks; i++) {
            int candidateId = startBlockId + i;
            long candidateKey = VirtualPage.encode(vfdId, candidateId);

            if (blockCache.get(candidateKey) != null) {
                // Already cached — gap in ownership, not gap in I/O
                endBlockId = candidateId;
                continue;
            }
            if (inflightMap.containsKey(candidateKey)) {
                // Already inflight — gap in ownership, not gap in I/O
                endBlockId = candidateId;
                continue;
            }
            owned[i] = true;
            endBlockId = candidateId;
        }

        int totalBlocks = endBlockId - startBlockId + 1;
        int totalBytes = totalBlocks * CACHE_BLOCK_SIZE;
        return new CoalescedRead(startBlockId, totalBlocks, totalBytes, owned);
    }

    /**
     * Insert futures for all owned blocks into the Inflight_Map before
     * starting physical I/O. This ensures concurrent requests for any
     * block in the range can join the existing future.
     *
     * @param vfdId          the VFD identifier
     * @param startBlockId   the first block in the range
     * @param plan           the coalesced read plan
     * @param triggerFuture  the future already in inflightMap for the trigger block
     * @return array of futures; futures[i] is non-null if we own block startBlockId+i
     */
    @SuppressWarnings("unchecked")
    CompletableFuture<MemorySegment>[] insertCoalescedFutures(
            long vfdId, int startBlockId, CoalescedRead plan,
            CompletableFuture<MemorySegment> triggerFuture) {
        CompletableFuture<MemorySegment>[] futures = new CompletableFuture[plan.totalBlocks()];
        futures[0] = triggerFuture; // already in Inflight_Map from readBlock step 3

        for (int i = 1; i < plan.totalBlocks(); i++) {
            if (!plan.owned()[i]) continue;

            long candidateKey = VirtualPage.encode(vfdId, startBlockId + i);
            CompletableFuture<MemorySegment> future = new CompletableFuture<>();
            CompletableFuture<MemorySegment> existing = inflightMap.putIfAbsent(candidateKey, future);

            if (existing == null) {
                futures[i] = future; // we own this slot
            }
            // else: another thread won the race — we don't own this block
        }
        return futures;
    }

    /**
     * Perform a single contiguous physical read covering all blocks in the
     * coalesced plan, then split the result into per-block MemorySegments.
     *
     * @param plan           the coalesced read plan
     * @param futures        per-block futures (non-null for owned blocks)
     * @param vfdId          the VFD identifier
     * @param isDemandRead   true for demand reads
     * @throws IOException if the read fails
     */
    private void doCoalescedPhysicalRead(
            CoalescedRead plan, CompletableFuture<MemorySegment>[] futures,
            long vfdId, boolean isDemandRead) throws IOException {
        // 1. Allocate a large temporary buffer for the coalesced read (pressure-guarded)
        ByteBuffer largeBuf = memoryPool.allocateOneTimeBuffer(plan.totalBytes());

        // 2. Acquire live FileChannel via VfdRegistry
        long fileOffset = (long) plan.startBlockId() * CACHE_BLOCK_SIZE;
        VfdRegistry.AcquiredChannel acquired = registry.acquireChannel(vfd);
        int bytesRead;
        try {
            // 3. Single contiguous positional read
            bytesRead = ioBackend.read(acquired.channel(), largeBuf, fileOffset, plan.totalBytes());
        } finally {
            acquired.release();
        }

        // 4. Decrypt in-place via MemorySegment before caching (no-op for unencrypted)
        largeBuf.flip();
        if (bytesRead < plan.totalBytes()) {
            largeBuf.limit(bytesRead);
        }
        MemorySegment writableLarge = MemorySegment.ofBuffer(largeBuf);
        blockDecryptor.decrypt(writableLarge, fileOffset);
        largeBuf.position(0);

        // 5. Update coalescing metrics
        metrics.coalescedIOCount.increment();
        metrics.coalescedBlockCount.add(plan.totalBlocks());

        // 5. Split into individual blocks and complete futures
        splitAndCache(largeBuf, plan, futures, vfdId, bytesRead);
    }

    /**
     * Slice the large buffer into per-block MemorySegments, put each in
     * L2 (BlockCache) and L1 (RadixBlockTable), and complete owned futures.
     *
     * @param largeBuf       the large buffer containing all block data
     * @param plan           the coalesced read plan
     * @param futures        per-block futures (non-null for owned blocks)
     * @param vfdId          the VFD identifier
     * @param totalBytesRead total bytes actually read from disk
     */
    private void splitAndCache(
            ByteBuffer largeBuf, CoalescedRead plan,
            CompletableFuture<MemorySegment>[] futures,
            long vfdId, int totalBytesRead) {
        // Buffer is already flipped and positioned at 0 by caller
        RadixBlockTable<MemorySegment> rbt = registry.getRadixBlockTable(vfdId);

        for (int i = 0; i < plan.totalBlocks(); i++) {
            if (futures[i] == null) continue; // not owned by us

            int bufOffset = i * CACHE_BLOCK_SIZE;
            int blockBytes = Math.min(CACHE_BLOCK_SIZE, totalBytesRead - bufOffset);
            if (blockBytes <= 0) {
                // Beyond EOF — complete with an empty segment
                futures[i].complete(MemorySegment.ofBuffer(
                    ByteBuffer.allocateDirect(0).asReadOnlyBuffer()));
                continue;
            }

            // Slice a read-only view for this block
            ByteBuffer slice = largeBuf.duplicate();
            slice.position(bufOffset).limit(bufOffset + blockBytes);
            slice = slice.slice().asReadOnlyBuffer();

            // Wrap into MemorySegment
            MemorySegment segment = MemorySegment.ofBuffer(slice);

            // Insert into L2 cache and L1 RadixBlockTable
            long blockKey = VirtualPage.encode(vfdId, plan.startBlockId() + i);
            blockCache.put(blockKey, segment);
            if (rbt != null) {
                rbt.put(plan.startBlockId() + i, segment);
            }

            // Complete the future — all joiners get this MemorySegment
            futures[i].complete(segment);
        }
    }

    /**
     * Cleanup coalesced futures: completeExceptionally on error, and
     * conditional inflightMap.remove in finally.
     *
     * @param vfdId        the VFD identifier
     * @param startBlockId the first block in the coalesced range
     * @param futures      per-block futures
     * @param error        the error (null in finally path for conditional remove only)
     */
    private void cleanupCoalescedFutures(
            long vfdId, int startBlockId,
            CompletableFuture<MemorySegment>[] futures,
            Throwable error) {
        // Start at 1 — the trigger block (index 0) is handled by readBlock's own finally
        for (int i = 1; i < futures.length; i++) {
            if (futures[i] == null) continue;

            if (error != null && !futures[i].isDone()) {
                futures[i].completeExceptionally(error);
            }

            // Conditional remove: only if the map still holds OUR future
            long blockKey = VirtualPage.encode(vfdId, startBlockId + i);
            inflightMap.remove(blockKey, futures[i]);
        }
    }

    // ---- Async Prefetch ----

    /**
     * Async prefetch — submit block reads to the prefetch executor.
     * Best-effort: skips blocks already cached or inflight, catches
     * backpressure exceptions, and never throws to the caller.
     *
     * <p><b>Satisfies: Requirement 9</b></p>
     *
     * @param vfdId      the VFD identifier
     * @param fileOffset the starting file offset
     * @param blockCount number of blocks to prefetch
     */
    public void prefetch(long vfdId, long fileOffset, int blockCount) {
        if (prefetchExecutor == null) {
            return;
        }

        for (int i = 0; i < blockCount; i++) {
            long blockOffset = fileOffset + ((long) i << CACHE_BLOCK_SIZE_POWER);
            int blockId = (int) (blockOffset >>> CACHE_BLOCK_SIZE_POWER);
            long key = VirtualPage.encode(vfdId, blockId);

            // Skip if already cached or in-flight
            if (blockCache.get(key) != null || inflightMap.containsKey(key)) {
                continue;
            }

            final long capturedBlockOffset = blockOffset;
            final int capturedBlockId = blockId;
            try {
                prefetchExecutor.submit(() -> {
                    try {
                        readBlock(vfdId, capturedBlockId, capturedBlockOffset, false);
                        metrics.prefetchCount.increment();
                    } catch (MemoryBackpressureException e) {
                        metrics.prefetchSkipCount.increment();
                    } catch (Exception e) {
                        logger.log(Level.FINE, "Prefetch failed for block " + capturedBlockId + ": " + e.getMessage());
                    }
                });
            } catch (RejectedExecutionException e) {
                metrics.prefetchSkipCount.increment();
            }
        }
    }

    // ---- Accessors ----

    /** Returns the inflight map (visible for testing). */
    ConcurrentHashMap<Long, CompletableFuture<MemorySegment>> inflightMap() {
        return inflightMap;
    }

    /** Returns the channel metrics. */
    public ChannelMetrics metrics() {
        return metrics;
    }

    /** Returns the VFD associated with this channel. */
    public VirtualFileDescriptor vfd() {
        return vfd;
    }

    /** Returns the max coalesced I/O size in bytes. */
    public int maxCoalescedIoSize() {
        return maxCoalescedIoSize;
    }

    /** Returns true if I/O coalescing is enabled. */
    public boolean isCoalescingEnabled() {
        return coalescingEnabled;
    }

    /** Returns the fault injector. */
    public FaultInjector faultInjector() {
        return faultInjector;
    }

    // ---- ChannelMetrics ----

    /**
     * Observable metrics for VirtualFileChannel.
     */
    public static final class ChannelMetrics {
        /** Number of physical I/O operations performed. */
        public final LongAdder physicalIOCount = new LongAdder();
        /** Number of I/O operations avoided via deduplication. */
        public final LongAdder dedupedIOCount = new LongAdder();
        /** Number of inflight wait timeouts. */
        public final LongAdder inflightTimeoutCount = new LongAdder();
        /** Cumulative inflight wait time in nanoseconds. */
        public final LongAdder inflightWaitTimeNanos = new LongAdder();
        /** Number of prefetch operations completed. */
        public final LongAdder prefetchCount = new LongAdder();
        /** Number of prefetch operations skipped due to backpressure. */
        public final LongAdder prefetchSkipCount = new LongAdder();
        /** Number of coalesced I/O operations performed. */
        public final LongAdder coalescedIOCount = new LongAdder();
        /** Total number of blocks read via coalesced I/O. */
        public final LongAdder coalescedBlockCount = new LongAdder();

        ChannelMetrics() {}

        /** Returns the cumulative inflight wait time in milliseconds. */
        public long inflightWaitTimeMs() {
            return TimeUnit.NANOSECONDS.toMillis(inflightWaitTimeNanos.sum());
        }

        @Override
        public String toString() {
            return "ChannelMetrics[physicalIO=" + physicalIOCount.sum()
                + ", dedupedIO=" + dedupedIOCount.sum()
                + ", inflightTimeouts=" + inflightTimeoutCount.sum()
                + ", inflightWaitMs=" + inflightWaitTimeMs()
                + ", prefetch=" + prefetchCount.sum()
                + ", prefetchSkips=" + prefetchSkipCount.sum()
                + ", coalescedIO=" + coalescedIOCount.sum()
                + ", coalescedBlocks=" + coalescedBlockCount.sum() + "]";
        }
    }
}
