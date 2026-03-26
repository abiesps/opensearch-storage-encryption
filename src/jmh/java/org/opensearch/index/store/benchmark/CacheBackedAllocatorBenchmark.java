/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Realistic benchmark: DirectByteBuffer allocation through a Caffeine cache
 * with size-based eviction, simulating the actual block cache workload.
 *
 * Cache holds buffers until evicted. On eviction, reclamation strategy applies:
 *   - abandon:       drop ref, let GC reclaim native memory
 *   - with_cleaner:  custom Cleaner tracks when GC reclaims the evicted buffer
 *   - explicit_free: Unsafe.invokeCleaner on eviction (immediate native free)
 *
 * The cache is sized to cachePercent% of MaxDirectMemorySize. Key space is 4x
 * cache capacity to ensure steady eviction churn (~25% hit rate at random).
 *
 * Run examples:
 *   # ZGC, 4 threads, cache = 75% of 512MB direct memory
 *   java -Djmh.ignoreLock=true -jar build/distributions/storage-encryption-jmh.jar \
 *     "CacheBackedAllocatorBenchmark.cacheLookup_4T" \
 *     -f 1 -wi 1 -i 2 -r 3s -w 2s \
 *     -jvmArgs "-XX:+UseZGC -XX:MaxDirectMemorySize=512m" \
 *     -p bufferSizeBytes=8192 -p cachePercent=75 -p strategy=abandon
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {
    "--enable-native-access=ALL-UNNAMED",
    "--enable-preview",
    "--add-opens", "java.base/sun.misc=ALL-UNNAMED"
})
public class CacheBackedAllocatorBenchmark {

    // ======================== Parameters ========================

    @Param({"8192"})
    int bufferSizeBytes;

    /** Cache capacity as percentage of MaxDirectMemorySize */
    @Param({"50", "75", "90"})
    int cachePercent;

    /**
     * - abandon:             evicted buffer dropped, GC reclaims native memory
     * - with_cleaner:        Cleaner.register on alloc, tracks phantom-reachability
     * - explicit_free:       Unsafe.invokeCleaner called synchronously on eviction
     * - proactive_eviction:  shrink cache proactively when native memory pressure rises,
     *                        giving GC runway to reclaim before Bits.reserveMemory() stalls
     */
    @Param({"abandon", "with_cleaner", "explicit_free", "proactive_eviction"})
    String strategy;

    // ======================== State ========================

    private Cache<Long, ByteBuffer> cache;
    private int keySpace;
    private long originalMaxEntries;

    // ---- Proactive eviction state ----
    /** Tracks bytes we've asked the cache to hold (incremented on alloc, decremented on eviction) */
    private final AtomicLong trackedBytes = new AtomicLong(0);
    private volatile long maxDirectMemoryBytes;
    /** Zone thresholds as fractions of maxDirectMemory */
    private static final double GREEN_CEILING  = 0.70;
    private static final double YELLOW_CEILING = 0.85;
    private static final double RED_CEILING    = 0.95;
    /** Shrink ratios applied to current cache maximum */
    private static final double YELLOW_SHRINK  = 0.90;
    private static final double RED_SHRINK     = 0.75;
    /** Minimum time between successive shrinks (ns) */
    private static final long SHRINK_COOLDOWN_NS = 200_000_000L; // 200ms
    private volatile long lastShrinkNanos = 0;
    private volatile boolean cacheIsShrunk = false;
    private final LongAdder proactiveShrinkCount = new LongAdder();

    private static final Cleaner CLEANER = Cleaner.create();
    static final LongAdder allocCount = new LongAdder();
    static final LongAdder hitCount = new LongAdder();
    static final LongAdder evictionCount = new LongAdder();
    static final LongAdder stallCount = new LongAdder();
    static final LongAdder oomCount = new LongAdder();
    static final LongAdder cleanerFireCount = new LongAdder();
    static final AtomicLong outstandingBytes = new AtomicLong(0);
    static volatile long nativeMemoryHWM = 0;
    private volatile BufferPoolMXBean directPool;

    // ======================== Unsafe for explicit_free ========================

    private static final sun.misc.Unsafe UNSAFE;
    static {
        try {
            var f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) f.get(null);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ======================== Setup / Teardown ========================

    @Setup(Level.Trial)
    public void setup() {
        allocCount.reset();
        hitCount.reset();
        evictionCount.reset();
        stallCount.reset();
        oomCount.reset();
        cleanerFireCount.reset();
        outstandingBytes.set(0);
        nativeMemoryHWM = 0;
        trackedBytes.set(0);
        proactiveShrinkCount.reset();
        lastShrinkNanos = 0;
        cacheIsShrunk = false;

        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool : pools) {
            if ("direct".equals(pool.getName())) {
                directPool = pool;
                break;
            }
        }

        // Compute cache capacity: cachePercent% of MaxDirectMemorySize / bufferSize
        long maxDirect = getMaxDirectMemory();
        maxDirectMemoryBytes = maxDirect;
        long cacheBytes = maxDirect * cachePercent / 100;
        long maxEntries = cacheBytes / bufferSizeBytes;
        originalMaxEntries = maxEntries;

        // Key space = 4x cache capacity → ~25% hit rate under uniform random access
        keySpace = (int) Math.min(maxEntries * 4, Integer.MAX_VALUE);

        cache = Caffeine.newBuilder()
            .maximumSize(maxEntries)
            .removalListener((Long key, ByteBuffer buf, RemovalCause cause) -> {
                if (buf == null) return;
                evictionCount.increment();
                trackedBytes.addAndGet(-bufferSizeBytes);
                switch (strategy) {
                    case "explicit_free" -> UNSAFE.invokeCleaner(buf);
                    case "with_cleaner" -> { /* Cleaner callback fires when GC collects */ }
                    case "abandon", "proactive_eviction" -> { /* drop ref, GC handles it */ }
                }
            })
            .build();

        System.out.println();
        System.out.println("--- Cache setup ---");
        System.out.println("MaxDirectMemory:  " + fmtBytes(maxDirect));
        System.out.println("Cache capacity:   " + maxEntries + " entries (" + fmtBytes(cacheBytes) + ")");
        System.out.println("Key space:        " + keySpace + " (hit rate ~25%)");
        System.out.println("Strategy:         " + strategy);
        System.out.println("-------------------");
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        // Invalidate cache and let cleaners drain
        if (cache != null) {
            cache.invalidateAll();
            cache.cleanUp();
        }
        System.gc();
        try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        System.gc();
        try { Thread.sleep(500); } catch (InterruptedException ignored) {}

        long allocs = allocCount.sum();
        long hits = hitCount.sum();
        long evictions = evictionCount.sum();
        long stalls = stallCount.sum();
        long ooms = oomCount.sum();
        long fires = cleanerFireCount.sum();
        long outstanding = outstandingBytes.get();
        long hwm = nativeMemoryHWM;
        long mxUsed = directPool != null ? directPool.getMemoryUsed() : -1;
        long mxCount = directPool != null ? directPool.getCount() : -1;
        long totalOps = allocs + hits;

        System.out.println();
        System.out.println("========== CACHE-BACKED ALLOCATOR SUMMARY ==========");
        System.out.println("Strategy:              " + strategy);
        System.out.println("Buffer size:           " + bufferSizeBytes + " B");
        System.out.println("Cache percent:         " + cachePercent + "%");
        System.out.println("Total ops:             " + totalOps);
        System.out.println("  Cache hits:          " + hits + " (" + pct(hits, totalOps) + ")");
        System.out.println("  Allocations (miss):  " + allocs + " (" + pct(allocs, totalOps) + ")");
        System.out.println("Evictions:             " + evictions);
        System.out.println("Cleaner fires:         " + fires);
        System.out.println("Outstanding (tracked): " + fmtBytes(outstanding));
        System.out.println("Native HWM (MXBean):   " + fmtBytes(hwm));
        System.out.println("MXBean used (final):   " + fmtBytes(mxUsed));
        System.out.println("MXBean count (final):  " + mxCount);
        System.out.println("OOM recoveries:        " + ooms);
        System.out.println("Stalls (>1ms):         " + stalls);
        System.out.println("Stall ratio:           "
            + (allocs > 0 ? String.format("%.4f%%", 100.0 * stalls / allocs) : "N/A"));
        if ("proactive_eviction".equals(strategy)) {
            System.out.println("Proactive shrinks:     " + proactiveShrinkCount.sum());
        }
        System.out.println("Throughput (alloc):    "
            + fmtBytes((long) allocs * bufferSizeBytes) + " total allocated");
        System.out.println("====================================================");
        System.out.println();
    }

    // ======================== Benchmark methods ========================

    @Benchmark
    @Threads(4)
    public void cacheLookup_4T(Blackhole bh) {
        doCacheLookup(bh);
    }

    @Benchmark
    @Threads(32)
    public void cacheLookup_32T(Blackhole bh) {
        doCacheLookup(bh);
    }

    private void doCacheLookup(Blackhole bh) {
        long key = ThreadLocalRandom.current().nextLong(keySpace);

        ByteBuffer buf = cache.getIfPresent(key);
        if (buf != null) {
            // Cache hit — no allocation
            hitCount.increment();
            bh.consume(buf.get(0));
            return;
        }

        // Cache miss — allocate and insert (with OOM retry)
        // Proactive eviction: check memory pressure BEFORE allocating
        if ("proactive_eviction".equals(strategy)) {
            maybeProactiveEvict();
        }

        ByteBuffer newBuf = null;
        long t0 = System.nanoTime();
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                newBuf = ByteBuffer.allocateDirect(bufferSizeBytes);
                break;
            } catch (OutOfMemoryError oom) {
                // Shrink cache to free native memory, then retry
                oomCount.increment();
                long currentMax = cache.estimatedSize();
                long shrunk = Math.max(1, currentMax * 3 / 4);
                cache.policy().eviction().ifPresent(e -> e.setMaximum(shrunk));
                cache.cleanUp();
                System.gc();
                Thread.onSpinWait();
                // Restore original max after emergency eviction
                long maxDirect = getMaxDirectMemory();
                long originalMax = maxDirect * cachePercent / 100 / bufferSizeBytes;
                cache.policy().eviction().ifPresent(e -> e.setMaximum(originalMax));
            }
        }
        long elapsed = System.nanoTime() - t0;

        if (newBuf == null) {
            // All retries exhausted — count as stall, skip this op
            stallCount.increment();
            allocCount.increment();
            return;
        }

        allocCount.increment();
        if (elapsed > 1_000_000L) stallCount.increment();

        newBuf.put(0, (byte) 42);

        if ("with_cleaner".equals(strategy)) {
            outstandingBytes.addAndGet(bufferSizeBytes);
            CLEANER.register(newBuf, new CleanerAction(bufferSizeBytes));
        }

        trackedBytes.addAndGet(bufferSizeBytes);
        cache.put(key, newBuf);
        bh.consume(newBuf.get(0));

        // Sample HWM every ~4096 allocs
        if ((allocCount.sum() & 0xFFF) == 0 && directPool != null) {
            long used = directPool.getMemoryUsed();
            if (used > nativeMemoryHWM) nativeMemoryHWM = used;
        }
    }

    // ======================== Proactive eviction ========================

    /**
     * Three-zone proactive eviction. Uses BufferPoolMXBean (real native usage)
     * rather than our tracked counter, because tracked bytes don't account for
     * buffers that GC has already freed but we haven't seen eviction callbacks for.
     *
     * GREEN  (< 70%):  restore cache max if previously shrunk
     * YELLOW (70-85%): shrink cache to 90% of original max + System.gc() hint
     * RED    (> 85%):  shrink cache to 75% of original max + synchronous cleanUp
     */
    private void maybeProactiveEvict() {
        // Only check real native usage every ~256 ops (cheap bitmask)
        if ((allocCount.sum() & 0xFF) != 0) return;

        long nativeUsed = directPool != null ? directPool.getMemoryUsed() : trackedBytes.get();
        double utilization = (double) nativeUsed / maxDirectMemoryBytes;

        if (utilization < GREEN_CEILING) {
            // GREEN zone — restore cache if it was shrunk
            if (cacheIsShrunk) {
                cache.policy().eviction().ifPresent(e -> e.setMaximum(originalMaxEntries));
                cacheIsShrunk = false;
            }
            return;
        }

        // Cooldown check — don't shrink too frequently
        long now = System.nanoTime();
        if (now - lastShrinkNanos < SHRINK_COOLDOWN_NS) return;

        if (utilization > RED_CEILING) {
            // RED zone — aggressive shrink
            long shrunk = (long) (originalMaxEntries * RED_SHRINK);
            cache.policy().eviction().ifPresent(e -> e.setMaximum(Math.max(1, shrunk)));
            cache.cleanUp(); // synchronous eviction
            System.gc();
            cacheIsShrunk = true;
            lastShrinkNanos = now;
            proactiveShrinkCount.increment();
        } else if (utilization > YELLOW_CEILING) {
            // YELLOW zone — moderate shrink
            long shrunk = (long) (originalMaxEntries * YELLOW_SHRINK);
            cache.policy().eviction().ifPresent(e -> e.setMaximum(Math.max(1, shrunk)));
            cache.cleanUp();
            System.gc();
            cacheIsShrunk = true;
            lastShrinkNanos = now;
            proactiveShrinkCount.increment();
        } else {
            // Between GREEN_CEILING and YELLOW_CEILING (70-85%) — just hint
            System.gc();
        }
    }

    // ======================== Cleaner action ========================

    record CleanerAction(int size) implements Runnable {
        @Override
        public void run() {
            outstandingBytes.addAndGet(-size);
            cleanerFireCount.increment();
        }
    }

    // ======================== Helpers ========================

    private static long getMaxDirectMemory() {
        try {
            // Try sun.misc.VM.maxDirectMemory() via reflection
            Class<?> vm = Class.forName("sun.misc.VM");
            return (long) vm.getMethod("maxDirectMemory").invoke(null);
        } catch (Exception e) {
            // Fallback: parse -XX:MaxDirectMemorySize from input arguments
            for (String arg : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
                if (arg.startsWith("-XX:MaxDirectMemorySize=")) {
                    String val = arg.substring("-XX:MaxDirectMemorySize=".length()).trim().toLowerCase();
                    long multiplier = 1;
                    if (val.endsWith("g")) { multiplier = 1024L * 1024 * 1024; val = val.substring(0, val.length() - 1); }
                    else if (val.endsWith("m")) { multiplier = 1024L * 1024; val = val.substring(0, val.length() - 1); }
                    else if (val.endsWith("k")) { multiplier = 1024L; val = val.substring(0, val.length() - 1); }
                    return Long.parseLong(val) * multiplier;
                }
            }
            // Default: assume 256MB
            return 256L * 1024 * 1024;
        }
    }

    private static String fmtBytes(long b) {
        if (b < 0) return "N/A";
        if (b < 1024) return b + " B";
        if (b < 1024 * 1024) return String.format("%.1f KB", b / 1024.0);
        if (b < 1024L * 1024 * 1024) return String.format("%.1f MB", b / (1024.0 * 1024));
        return String.format("%.2f GB", b / (1024.0 * 1024 * 1024));
    }

    private static String pct(long num, long denom) {
        if (denom == 0) return "N/A";
        return String.format("%.1f%%", 100.0 * num / denom);
    }
}
