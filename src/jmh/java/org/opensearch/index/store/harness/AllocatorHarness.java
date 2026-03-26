/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.harness;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.block_cache_v2.CacheController;
import org.opensearch.index.store.block_cache_v2.DirectMemoryAllocator;
import org.opensearch.index.store.block_cache_v2.MemoryBackPressureException;

/**
 * Standalone harness to stress-test {@link DirectMemoryAllocator} with gc_assisted
 * reclamation under resource constraints. Proves that GC-managed direct memory
 * (no pool, no reference counting) can sustain block cache workloads.
 *
 * <h2>Phases</h2>
 * <ol>
 *   <li><b>Fill</b> — populate cache to {@code fillPct}, establishing baseline</li>
 *   <li><b>Age</b> — idle + explicit GC to promote buffers to old generation</li>
 *   <li><b>Churn</b> — sustained allocation at target rate with zipfian key distribution</li>
 * </ol>
 *
 * <h2>Usage</h2>
 * <pre>
 * java -XX:MaxDirectMemorySize=4g -XX:+UseZGC -XX:+ZGenerational \
 *   -cp harness.jar org.opensearch.index.store.harness.AllocatorHarness \
 *   --block-size 65536 --cache-fraction 0.75 --resident-fraction 0.70 \
 *   --fill-pct 0.95 --age-duration 120 --mode rate-limited --target-rate 500000 \
 *   --threads 8 --churn-duration 300 --output results/run1/
 * </pre>
 */
public final class AllocatorHarness {

    // ======================== Config ========================

    enum Mode { OPEN_LOOP, RATE_LIMITED }
    enum ReclaimStrategy { GC_ASSISTED, EXPLICIT_FREE }

    private final int blockSize;
    private final double cacheFraction;
    private final double residentFraction;
    private final double fillPct;
    private final Duration ageDuration;
    private final Mode mode;
    private final long targetRate;
    private final int threads;
    private final Duration churnDuration;
    private final double zipfianSkew;
    private final int nettyDirectMb;
    private final Path outputDir;
    private final ReclaimStrategy reclaimStrategy;

    // ======================== Wired components ========================

    private Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> cache;
    private DirectMemoryAllocator allocator;
    private HarnessCacheController cacheController;
    private MetricsCollector metrics;
    private NettyPressureSimulator nettyPressure;

    // ======================== Runtime state ========================

    private long maxBlocks;
    private long residentBlocks;
    private final LongAdder totalOps = new LongAdder();
    private final LongAdder backPressureCount = new LongAdder();
    private final Path syntheticPath = Path.of("/harness/synthetic");

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

    private AllocatorHarness(Builder b) {
        this.blockSize = b.blockSize;
        this.cacheFraction = b.cacheFraction;
        this.residentFraction = b.residentFraction;
        this.fillPct = b.fillPct;
        this.ageDuration = b.ageDuration;
        this.mode = b.mode;
        this.targetRate = b.targetRate;
        this.threads = b.threads;
        this.churnDuration = b.churnDuration;
        this.zipfianSkew = b.zipfianSkew;
        this.nettyDirectMb = b.nettyDirectMb;
        this.outputDir = b.outputDir;
        this.reclaimStrategy = b.reclaimStrategy;
    }

    // ======================== Lifecycle ========================

    public void run() throws Exception {
        setup();
        try {
            phaseFill();
            phaseAge();
            phaseChurn();
        } finally {
            teardown();
        }
    }

    private void setup() {
        long maxDirect = resolveMaxDirectMemory();
        long cacheBytes = (long) (maxDirect * cacheFraction);
        maxBlocks = cacheBytes / blockSize;
        residentBlocks = (long) (maxBlocks * residentFraction);

        System.out.println("=== AllocatorHarness Setup ===");
        System.out.printf("MaxDirectMemory: %,d bytes%n", maxDirect);
        System.out.printf("Cache: %,d blocks × %,d B = %,d bytes (%.0f%%)%n",
            maxBlocks, blockSize, cacheBytes, cacheFraction * 100);
        System.out.printf("Resident: %,d blocks (%.0f%%)%n", residentBlocks, residentFraction * 100);
        System.out.printf("Mode: %s, Threads: %d, Churn: %ds%n",
            mode, threads, churnDuration.toSeconds());
        System.out.printf("Reclaim: %s%n", reclaimStrategy);

        // Netty pressure (before cache, so allocator sees external usage)
        if (nettyDirectMb > 0) {
            nettyPressure = new NettyPressureSimulator(nettyDirectMb);
        }

        // Build Caffeine cache with removal listener that retires the value
        ExecutorService removalExec = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "cache-removal");
            t.setDaemon(true);
            return t;
        });
        cache = Caffeine.newBuilder()
            .maximumSize(maxBlocks)
            .recordStats()
            .removalListener((BlockCacheKey key, BlockCacheValue<RefCountedMemorySegment> value, RemovalCause cause) -> {
                if (value != null) {
                    removalExec.execute(() -> {
                        try {
                            if (reclaimStrategy == ReclaimStrategy.EXPLICIT_FREE) {
                                // Baseline: immediately free native memory on eviction
                                java.nio.ByteBuffer bb = value.value().segment().asByteBuffer();
                                value.close();
                                UNSAFE.invokeCleaner(bb);
                            } else {
                                // gc_assisted: just drop the ref, let GC/Cleaner reclaim
                                value.close();
                            }
                        } catch (Exception ignored) {}
                    });
                }
            })
            .build();

        cacheController = new HarnessCacheController(cache, blockSize, maxBlocks);

        allocator = new DirectMemoryAllocator(maxDirect, blockSize);
        allocator.registerCacheController(cacheController);

        metrics = new MetricsCollector(allocator, outputDir.resolve("metrics.csv"));
    }

    // ======================== Phase 1: Fill ========================

    private void phaseFill() {
        long target = (long) (maxBlocks * fillPct);
        System.out.printf("%n=== Phase 1: Fill (%,d blocks) ===%n", target);
        long filled = 0;
        for (long i = 0; i < target; i++) {
            try {
                ByteBuffer buf = allocator.allocate(blockSize);
                buf.put(0, (byte) 42);
                MemorySegment seg = MemorySegment.ofBuffer(buf);
                RefCountedMemorySegment rcms = new RefCountedMemorySegment(seg, blockSize, r -> {});
                cache.put(new FileBlockCacheKey(syntheticPath, i * blockSize), rcms);
                filled++;
            } catch (MemoryBackPressureException e) {
                System.out.printf("  Fill back-pressure at block %,d — stopping fill%n", i);
                break;
            }
        }
        System.out.printf("  Filled %,d blocks, cache size: %,d%n", filled, cache.estimatedSize());
    }

    // ======================== Phase 2: Age ========================

    private void phaseAge() throws InterruptedException {
        System.out.printf("%n=== Phase 2: Age (%ds) ===%n", ageDuration.toSeconds());
        System.gc();
        Thread.sleep(ageDuration.toMillis() / 2);
        System.gc();
        Thread.sleep(ageDuration.toMillis() / 2);
        System.out.printf("  Aging complete, cache size: %,d%n", cache.estimatedSize());
    }

    // ======================== Phase 3: Churn ========================

    private void phaseChurn() throws InterruptedException {
        System.out.printf("%n=== Phase 3: Churn (%ds, %d threads, %s) ===%n",
            churnDuration.toSeconds(), threads, mode);

        metrics.start();

        // Key space: 4x cache for churn range (above resident blocks)
        long churnKeySpace = maxBlocks * 4;
        ZipfianKeyGenerator keyGen = new ZipfianKeyGenerator(churnKeySpace, zipfianSkew);

        RateLimiter limiter = mode == Mode.RATE_LIMITED
            ? new RateLimiter(targetRate, threads)
            : null;

        long deadlineNanos = System.nanoTime() + churnDuration.toNanos();
        CountDownLatch done = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                try {
                    while (System.nanoTime() < deadlineNanos) {
                        if (limiter != null) limiter.acquire();

                        // Generate key offset above resident range so resident blocks stay old-gen
                        long key = residentBlocks + keyGen.nextKey();
                        long offset = key * blockSize;

                        try {
                            ByteBuffer buf = allocator.allocate(blockSize);
                            buf.put(0, (byte) 42);
                            MemorySegment seg = MemorySegment.ofBuffer(buf);
                            RefCountedMemorySegment rcms = new RefCountedMemorySegment(seg, blockSize, r -> {});
                            cache.put(new FileBlockCacheKey(syntheticPath, offset), rcms);
                            totalOps.increment();
                        } catch (MemoryBackPressureException e) {
                            backPressureCount.increment();
                        }
                    }
                } finally {
                    done.countDown();
                }
            }, "churn-" + t);
            worker.setDaemon(true);
            worker.start();
        }

        done.await();
        System.out.printf("  Churn complete: %,d ops, %,d back-pressure events%n",
            totalOps.sum(), backPressureCount.sum());
    }

    // ======================== Teardown ========================

    private void teardown() {
        if (metrics != null) metrics.close();

        // Print final snapshot
        var snap = allocator.snapshot();
        System.out.println("\n========== FINAL DIAGNOSTICS ==========");
        System.out.printf("Total allocations:   %,d%n", snap.totalAllocations());
        System.out.printf("Allocated by us:     %,d bytes%n", snap.allocatedByUs());
        System.out.printf("Reclaimed by GC:     %,d bytes%n", snap.reclaimedByGc());
        System.out.printf("Stalls:              %,d (total %,d ns)%n", snap.stallCount(), snap.stallNanosTotal());
        System.out.printf("GC hints:            %,d (skipped: %,d)%n", snap.gcHintCount(), snap.gcHintSkippedCooldown());
        System.out.printf("Cache shrinks:       %,d (%,d blocks total)%n", snap.cacheShrinkCount(), snap.cacheShrinkBlocksTotal());
        System.out.printf("Cache restores:      %,d (%,d blocks total)%n", snap.cacheRestoreCount(), snap.cacheRestoreBlocksTotal());
        System.out.printf("Tier 1/2/3:          %,d / %,d / %,d%n", snap.tier1Count(), snap.tier2Count(), snap.tier3Count());
        System.out.printf("Pressure level:      %.4f%n", snap.pressureLevel());
        System.out.printf("Back-pressure evts:  %,d%n", backPressureCount.sum());
        System.out.println("========================================\n");

        if (cache != null) { cache.invalidateAll(); cache.cleanUp(); }
        if (nettyPressure != null) nettyPressure.close();
        allocator.deregisterCacheController();
    }

    // ======================== Builder ========================

    static class Builder {
        int blockSize = 32768;
        double cacheFraction = 0.75;
        double residentFraction = 0.70;
        double fillPct = 0.95;
        Duration ageDuration = Duration.ofSeconds(120);
        Mode mode = Mode.OPEN_LOOP;
        long targetRate = 500_000;
        int threads = 8;
        Duration churnDuration = Duration.ofSeconds(300);
        double zipfianSkew = 0.99;
        int nettyDirectMb = 0;
        Path outputDir = Path.of("results/default");
        ReclaimStrategy reclaimStrategy = ReclaimStrategy.GC_ASSISTED;

        AllocatorHarness build() { return new AllocatorHarness(this); }
    }

    // ======================== CLI ========================

    public static void main(String[] args) throws Exception {
        Builder b = new Builder();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--block-size" -> b.blockSize = parseSize(args[++i]);
                case "--cache-fraction" -> b.cacheFraction = Double.parseDouble(args[++i]);
                case "--resident-fraction" -> b.residentFraction = Double.parseDouble(args[++i]);
                case "--fill-pct" -> b.fillPct = Double.parseDouble(args[++i]);
                case "--age-duration" -> b.ageDuration = Duration.ofSeconds(Long.parseLong(args[++i]));
                case "--mode" -> b.mode = "rate-limited".equals(args[++i]) ? Mode.RATE_LIMITED : Mode.OPEN_LOOP;
                case "--target-rate" -> b.targetRate = Long.parseLong(args[++i]);
                case "--threads" -> b.threads = Integer.parseInt(args[++i]);
                case "--churn-duration" -> b.churnDuration = Duration.ofSeconds(Long.parseLong(args[++i]));
                case "--zipfian-skew" -> b.zipfianSkew = Double.parseDouble(args[++i]);
                case "--netty-direct-mb" -> b.nettyDirectMb = Integer.parseInt(args[++i]);
                case "--reclaim" -> b.reclaimStrategy = "explicit-free".equals(args[++i])
                    ? ReclaimStrategy.EXPLICIT_FREE : ReclaimStrategy.GC_ASSISTED;
                case "--output" -> b.outputDir = Path.of(args[++i]);
                case "--help" -> { printUsage(); return; }
                default -> { System.err.println("Unknown arg: " + args[i]); printUsage(); return; }
            }
        }
        b.build().run();
    }

    private static int parseSize(String s) {
        s = s.toLowerCase();
        if (s.endsWith("k")) return Integer.parseInt(s.replace("k", "")) * 1024;
        if (s.endsWith("m")) return Integer.parseInt(s.replace("m", "")) * 1024 * 1024;
        return Integer.parseInt(s);
    }

    private static long resolveMaxDirectMemory() {
        try {
            Class<?> vm = Class.forName("sun.misc.VM");
            long result = (long) vm.getMethod("maxDirectMemory").invoke(null);
            if (result > 0) return result;
        } catch (Exception ignored) {}
        // JDK 25+ removed sun.misc.VM — parse from JVM input arguments
        for (String arg : java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            if (arg.startsWith("-XX:MaxDirectMemorySize=")) {
                String val = arg.substring("-XX:MaxDirectMemorySize=".length()).trim().toLowerCase();
                long multiplier = 1;
                if (val.endsWith("g")) { multiplier = 1024L * 1024 * 1024; val = val.substring(0, val.length() - 1); }
                else if (val.endsWith("m")) { multiplier = 1024L * 1024; val = val.substring(0, val.length() - 1); }
                else if (val.endsWith("k")) { multiplier = 1024L; val = val.substring(0, val.length() - 1); }
                return Long.parseLong(val) * multiplier;
            }
        }
        return Runtime.getRuntime().maxMemory();
    }

    /** Simple sleep-based rate limiter — no Guava dependency. */
    static final class RateLimiter {
        private final long intervalNanos;

        RateLimiter(long totalRate, int threads) {
            long perThread = Math.max(1, totalRate / threads);
            this.intervalNanos = 1_000_000_000L / perThread;
        }

        void acquire() {
            if (intervalNanos > 1000) {
                long target = System.nanoTime() + intervalNanos;
                while (System.nanoTime() < target) Thread.onSpinWait();
            }
        }
    }

    private static void printUsage() {
        System.out.println("""
            AllocatorHarness — DirectMemoryAllocator stress test
            
            Options:
              --block-size <N>         Block size (default 32k, supports k/m suffix)
              --cache-fraction <f>     Fraction of MaxDirectMemory for cache (default 0.75)
              --resident-fraction <f>  Fraction of cache that stays old-gen (default 0.70)
              --fill-pct <f>           Fill cache to this fraction before churn (default 0.95)
              --age-duration <secs>    Idle time for GC promotion (default 120)
              --mode <mode>            open-loop | rate-limited (default open-loop)
              --target-rate <N>        Allocs/sec for rate-limited mode (default 500000)
              --threads <N>            Churn threads (default 8)
              --churn-duration <secs>  Churn phase duration (default 300)
              --zipfian-skew <f>       Zipfian exponent (default 0.99)
              --netty-direct-mb <N>    Simulate Netty direct memory pressure (default 0)
              --reclaim <strategy>     gc-assisted (default) | explicit-free (baseline)
              --output <dir>           Output directory for metrics CSV (default results/default/)
            """);
    }
}
