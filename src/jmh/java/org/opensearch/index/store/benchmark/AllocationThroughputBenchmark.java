/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Can GC-based DirectByteBuffer reclamation sustain the allocation rate
 * required by a 4 GB/s-per-core block cache read path?
 *
 * <h2>Throughput requirement derivation</h2>
 * <pre>
 *   Target read throughput:   4 GB/s per core
 *   Block size:               8 KB
 *   Block reads per core:     4 GB/s / 8 KB = 512K blocks/sec per core
 *
 *   At 100% cache miss (warmup burst):  512K allocs/sec per core
 *   At  90% hit rate (churn):            51K allocs/sec per core
 *   At  99% hit rate (steady state):      5K allocs/sec per core
 * </pre>
 *
 * The benchmark simulates a Caffeine block cache where:
 * <ul>
 *   <li>Cache holds {@code cachePercent}% of MaxDirectMemorySize worth of 8 KB buffers</li>
 *   <li>Key space is 4x cache capacity → ~25% hit rate (simulates warmup burst)</li>
 *   <li>Every cache miss triggers a buffer allocation (the bottleneck under test)</li>
 *   <li>Eviction triggers reclamation per the selected strategy</li>
 * </ul>
 *
 * <h2>Strategies</h2>
 * <ul>
 *   <li>{@code gc_only}: {@code allocateDirect()} on miss, drop ref on eviction.
 *       Pure GC reclamation. Baseline — measures whether ZGC's Cleaner thread
 *       can keep up without any assistance.</li>
 *   <li>{@code gc_assisted}: like gc_only, but a proactive allocator monitors
 *       native memory utilization (via BufferPoolMXBean, sampled every ~4096 ops)
 *       and shrinks the Caffeine cache max size when pressure rises:
 *       <ul>
 *         <li>&lt; 70%: restore original cache max (GREEN zone)</li>
 *         <li>70-85%: shrink cache to 90% of original max (YELLOW — mild eviction)</li>
 *         <li>85-95%: shrink cache to 75% + synchronous cleanUp (ORANGE)</li>
 *         <li>&gt; 95%: shrink cache to 50% + cleanUp + System.gc() (RED — last resort)</li>
 *       </ul>
 *       This triggers evictions that drop ByteBuffer references, giving ZGC more
 *       reclaimable objects without calling System.gc() on the hot path.
 *       The goal: keep native memory utilization in a zone where
 *       {@code Bits.reserveMemory()} never needs to spin-wait for Cleaners.</li>
 *   <li>{@code explicit_free}: {@code Unsafe.invokeCleaner()} on eviction.
 *       Upper bound — immediate native free, zero GC dependency.
 *       Establishes the ceiling: if gc_assisted matches this, the proactive
 *       approach eliminates the GC bottleneck.</li>
 * </ul>
 *
 * <h2>Success criteria</h2>
 * <pre>
 *   Per-core throughput ≥ 512K ops/sec  (sustains 4 GB/s at 100% miss)
 *   Stall ratio         &lt; 0.01%        (fewer than 1 in 10,000 allocs &gt; 1ms)
 *   OOM count           = 0             (no OutOfMemoryError under any strategy)
 *   Native HWM          ≤ MaxDirectMemorySize  (no overcommit)
 * </pre>
 *
 * <h2>Run examples</h2>
 * <pre>
 *   # Full matrix: all strategies × cache pressures, ZGC, 512m direct memory
 *   java -Djmh.ignoreLock=true -jar build/distributions/storage-encryption-jmh.jar \
 *     "AllocationThroughputBenchmark" \
 *     -f 2 -wi 2 -i 3 -r 10s -w 5s \
 *     -jvmArgs "-XX:+UseZGC -XX:MaxDirectMemorySize=512m -Xmx256m" \
 *     -p bufferSizeBytes=8192
 *
 *   # Quick smoke test: single strategy, single thread
 *   java -Djmh.ignoreLock=true -jar build/distributions/storage-encryption-jmh.jar \
 *     "AllocationThroughputBenchmark.throughput_1T" \
 *     -f 1 -wi 1 -i 1 -r 5s -w 3s \
 *     -jvmArgs "-XX:+UseZGC -XX:MaxDirectMemorySize=512m -Xmx256m" \
 *     -p bufferSizeBytes=8192 -p cachePercent=75 -p strategy=gc_only
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2, jvmArgsAppend = {
    "--enable-native-access=ALL-UNNAMED",
    "--enable-preview",
    "--add-opens", "java.base/sun.misc=ALL-UNNAMED"
})
public class AllocationThroughputBenchmark {

    // ======================== Parameters ========================

    @Param({"8192"})
    int bufferSizeBytes;

    /** Cache capacity as percentage of MaxDirectMemorySize. Higher = more GC pressure. */
    @Param({"75", "90"})
    int cachePercent;

    /**
     * gc_only:        allocateDirect on miss, drop on eviction, pure GC reclamation
     * gc_assisted:    like gc_only, but proactively shrinks cache max size when native
     *                 memory utilization rises, triggering evictions that give ZGC more
     *                 reclaimable references without calling System.gc()
     * explicit_free:  Unsafe.invokeCleaner on eviction (upper bound, no GC dependency)
     */
    @Param({"gc_only", "gc_assisted", "explicit_free"})
    String strategy;

    // ======================== State ========================

    private Cache<Long, ByteBuffer> cache;
    private int keySpace;

    // ---- Instrumentation ----
    private static final LongAdder allocCount = new LongAdder();
    private static final LongAdder hitCount = new LongAdder();
    private static final LongAdder evictionCount = new LongAdder();
    private static final LongAdder stallCount = new LongAdder();
    private static final LongAdder oomCount = new LongAdder();
    private static final LongAdder gcHintCount = new LongAdder();
    private static volatile long nativeMemoryHWM = 0;
    private volatile BufferPoolMXBean directPool;
    private static final LongAdder totalOps = new LongAdder();

    // ---- gc_assisted zone thresholds (fraction of MaxDirectMemorySize) ----
    private static final double ZONE_GREEN_CEILING  = 0.70;  // below: no action
    private static final double ZONE_YELLOW_CEILING = 0.85;  // 70-85%: shrink cache to 90% of original max
    private static final double ZONE_RED_CEILING    = 0.95;  // above 85%: shrink cache to 75% of original max
    // above 95%: shrink to 50% + System.gc()
    private volatile long maxDirectMemoryBytes;
    private volatile long originalMaxEntries;

    // ======================== Unsafe ========================

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
        gcHintCount.reset();
        totalOps.reset();
        nativeMemoryHWM = 0;

        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool : pools) {
            if ("direct".equals(pool.getName())) {
                directPool = pool;
                break;
            }
        }

        long maxDirect = getMaxDirectMemory();
        maxDirectMemoryBytes = maxDirect;
        long cacheBytes = maxDirect * cachePercent / 100;
        long maxEntries = cacheBytes / bufferSizeBytes;

        // Key space = 4x cache → ~25% hit rate, simulates warmup burst
        keySpace = (int) Math.min(maxEntries * 4, Integer.MAX_VALUE);
        originalMaxEntries = maxEntries;

        cache = Caffeine.newBuilder()
            .maximumSize(maxEntries)
            .removalListener((Long key, ByteBuffer buf, RemovalCause cause) -> {
                if (buf == null) return;
                evictionCount.increment();
                handleEviction(buf);
            })
            .build();

        // Derived throughput targets for reference
        long targetPerCore = (4L * 1024 * 1024 * 1024) / bufferSizeBytes; // 512K for 8KB

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║         ALLOCATION THROUGHPUT BENCHMARK              ║");
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.println("║ Target: 4 GB/s per core = " + String.format("%,d", targetPerCore) + " allocs/sec/core ║");
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.println("║ MaxDirectMemory:  " + padRight(fmtBytes(maxDirect), 34) + "║");
        System.out.println("║ Cache capacity:   " + padRight(maxEntries + " entries (" + fmtBytes(cacheBytes) + ")", 34) + "║");
        System.out.println("║ Key space:        " + padRight(keySpace + " (~25% hit rate)", 34) + "║");
        System.out.println("║ Strategy:         " + padRight(strategy, 34) + "║");
        System.out.println("║ Buffer size:      " + padRight(bufferSizeBytes + " B", 34) + "║");
        System.out.println("╚══════════════════════════════════════════════════════╝");
    }

    @TearDown(Level.Trial)
    public void tearDown() {
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
        long total = allocs + hits;
        long hwm = nativeMemoryHWM;
        long mxUsed = directPool != null ? directPool.getMemoryUsed() : -1;
        long maxDirect = getMaxDirectMemory();
        long targetPerCore = (4L * 1024 * 1024 * 1024) / bufferSizeBytes;

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    RESULTS SUMMARY                               ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        System.out.println("║ Strategy:              " + padRight(strategy, 42) + "║");
        System.out.println("║ Buffer size:           " + padRight(bufferSizeBytes + " B", 42) + "║");
        System.out.println("║ Cache fill:            " + padRight(cachePercent + "% of MaxDirectMemory", 42) + "║");
        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        System.out.println("║ Total ops:             " + padRight(String.format("%,d", total), 42) + "║");
        System.out.println("║   Cache hits:          " + padRight(String.format("%,d (%s)", hits, pct(hits, total)), 42) + "║");
        System.out.println("║   Allocations (miss):  " + padRight(String.format("%,d (%s)", allocs, pct(allocs, total)), 42) + "║");
        System.out.println("║ Evictions:             " + padRight(String.format("%,d", evictions), 42) + "║");
        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        System.out.println("║ Stalls (>1ms):         " + padRight(String.format("%,d", stalls), 42) + "║");
        System.out.println("║ Stall ratio:           " + padRight(stallRatio(stalls, allocs), 42) + "║");
        System.out.println("║ OOM recoveries:        " + padRight(String.format("%,d", ooms), 42) + "║");
        if ("gc_assisted".equals(strategy)) {
            System.out.println("║ GC hints fired:        " + padRight(String.format("%,d", gcHintCount.sum()), 42) + "║");
        }
        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        System.out.println("║ Native HWM:            " + padRight(fmtBytes(hwm) + " / " + fmtBytes(maxDirect)
            + " (" + pct(hwm, maxDirect) + ")", 42) + "║");
        System.out.println("║ MXBean used (final):   " + padRight(fmtBytes(mxUsed), 42) + "║");
        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        System.out.println("║ SUCCESS CRITERIA                                                 ║");
        System.out.println("║   Per-core ≥ " + String.format("%,d", targetPerCore) + " ops/sec:  "
            + padRight("(check JMH throughput / thread count)", 30) + "║");
        System.out.println("║   Stall ratio < 0.01%:     " + padRight(stalls == 0 ? "✓ PASS (zero stalls)" :
            (stallRatioPct(stalls, allocs) < 0.01 ? "✓ PASS" : "✗ FAIL"), 37) + "║");
        System.out.println("║   OOM count = 0:           " + padRight(ooms == 0 ? "✓ PASS" : "✗ FAIL (" + ooms + " OOMs)", 37) + "║");
        System.out.println("║   HWM ≤ MaxDirect:         " + padRight(hwm <= maxDirect ? "✓ PASS" : "✗ FAIL", 37) + "║");
        System.out.println("╚══════════════════════════════════════════════════════════════════╝");
    }


    // ======================== Benchmark methods ========================

    @Benchmark
    @Threads(1)
    public void throughput_1T(Blackhole bh) {
        doCacheLookup(bh);
    }

    @Benchmark
    @Threads(4)
    public void throughput_4T(Blackhole bh) {
        doCacheLookup(bh);
    }

    @Benchmark
    @Threads(8)
    public void throughput_8T(Blackhole bh) {
        doCacheLookup(bh);
    }

    @Benchmark
    @Threads(32)
    public void throughput_32T(Blackhole bh) {
        doCacheLookup(bh);
    }

    @Benchmark
    @Threads(72)
    public void throughput_72T(Blackhole bh) {
        doCacheLookup(bh);
    }

    private void doCacheLookup(Blackhole bh) {
        long key = ThreadLocalRandom.current().nextLong(keySpace);

        ByteBuffer buf = cache.getIfPresent(key);
        if (buf != null) {
            hitCount.increment();
            totalOps.increment();
            bh.consume(buf.get(0));
            return;
        }

        // Cache miss — allocate via strategy
        ByteBuffer newBuf = allocateBuffer();
        if (newBuf == null) {
            // All retries exhausted
            return;
        }

        newBuf.put(0, (byte) 42);
        cache.put(key, newBuf);
        totalOps.increment();
        bh.consume(newBuf.get(0));

        // Sample HWM every ~4096 ops
        if ((totalOps.sum() & 0xFFF) == 0 && directPool != null) {
            long used = directPool.getMemoryUsed();
            if (used > nativeMemoryHWM) nativeMemoryHWM = used;
        }
    }

    // ======================== Allocation strategies ========================

    /**
     * Proactive cache-shrink based on native memory utilization.
     * Instead of calling System.gc() (which is a global safepoint and kills throughput),
     * we shrink the Caffeine cache max size. This triggers synchronous eviction →
     * drops ByteBuffer references → ZGC's normal concurrent cycle reclaims them.
     *
     * Only samples BufferPoolMXBean every ~4096 allocs to keep overhead near zero.
     */
    private void maybeAdjustCacheSize() {
        // Cheap check: only sample every ~4096 allocs
        if ((allocCount.sum() & 0xFFF) != 0) return;

        long nativeUsed = directPool != null ? directPool.getMemoryUsed() : 0;
        double utilization = (double) nativeUsed / maxDirectMemoryBytes;

        if (utilization > ZONE_RED_CEILING) {
            // > 95%: aggressive shrink to 50% + one System.gc() as last resort
            long shrunk = originalMaxEntries / 2;
            cache.policy().eviction().ifPresent(e -> e.setMaximum(Math.max(1, shrunk)));
            cache.cleanUp();
            System.gc();
            gcHintCount.increment();
        } else if (utilization > ZONE_YELLOW_CEILING) {
            // 85-95%: moderate shrink to 75%
            long shrunk = originalMaxEntries * 3 / 4;
            cache.policy().eviction().ifPresent(e -> e.setMaximum(Math.max(1, shrunk)));
            cache.cleanUp();
            gcHintCount.increment();
        } else if (utilization > ZONE_GREEN_CEILING) {
            // 70-85%: mild shrink to 90%
            long shrunk = originalMaxEntries * 9 / 10;
            cache.policy().eviction().ifPresent(e -> e.setMaximum(Math.max(1, shrunk)));
            gcHintCount.increment();
        } else {
            // GREEN: restore original max if it was shrunk
            cache.policy().eviction().ifPresent(e -> e.setMaximum(originalMaxEntries));
        }
    }

    private ByteBuffer allocateBuffer() {
        // gc_assisted: proactively shrink cache to keep native memory in safe zone
        if ("gc_assisted".equals(strategy)) {
            maybeAdjustCacheSize();
        }

        // allocateDirect with OOM retry
        for (int attempt = 0; attempt < 3; attempt++) {
            long t0 = System.nanoTime();
            try {
                ByteBuffer buf = ByteBuffer.allocateDirect(bufferSizeBytes);
                long elapsed = System.nanoTime() - t0;
                allocCount.increment();
                if (elapsed > 1_000_000L) stallCount.increment();

                return buf;
            } catch (OutOfMemoryError oom) {
                oomCount.increment();
                // Emergency: shrink cache to free native memory
                long currentSize = cache.estimatedSize();
                long shrunk = Math.max(1, currentSize * 3 / 4);
                cache.policy().eviction().ifPresent(e -> e.setMaximum(shrunk));
                cache.cleanUp();
                System.gc();
                Thread.onSpinWait();
                // Restore original max
                long maxDirect = getMaxDirectMemory();
                long originalMax = maxDirect * cachePercent / 100 / bufferSizeBytes;
                cache.policy().eviction().ifPresent(e -> e.setMaximum(originalMax));
            }
        }
        stallCount.increment();
        allocCount.increment();
        return null;
    }

    private void handleEviction(ByteBuffer buf) {
        switch (strategy) {
            case "gc_only", "gc_assisted" -> {
                // Drop ref — JDK's internal Cleaner frees native memory when GC collects
            }
            case "explicit_free" -> {
                UNSAFE.invokeCleaner(buf);
            }
        }
    }

    // ======================== Helpers ========================

    private static long getMaxDirectMemory() {
        try {
            Class<?> vm = Class.forName("sun.misc.VM");
            return (long) vm.getMethod("maxDirectMemory").invoke(null);
        } catch (Exception e) {
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

    private static String stallRatio(long stalls, long allocs) {
        if (allocs == 0) return "N/A";
        return String.format("%.4f%% (%d / %,d)", 100.0 * stalls / allocs, stalls, allocs);
    }

    private static double stallRatioPct(long stalls, long allocs) {
        if (allocs == 0) return 0;
        return 100.0 * stalls / allocs;
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) return s;
        return s + " ".repeat(width - s.length());
    }
}
