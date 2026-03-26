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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmark: DirectByteBuffer allocation + reclamation under high throughput.
 *
 * Answers empirically:
 *   1. Can the single-threaded Cleaner keep up at ~1M buffers/sec?
 *   2. What is the native memory high-water mark under sustained churn?
 *   3. Which GC (ZGC, Shenandoah, G1) delivers predictable reclamation?
 *   4. What does allocation latency look like (stalls > 1ms)?
 *
 * Strategies tested:
 *   - allocate_abandon:       allocateDirect + drop ref (pure GC pressure)
 *   - allocate_with_cleaner:  allocateDirect + register java.lang.ref.Cleaner callback
 *   - allocate_explicit_free: allocateDirect + sun.misc.Unsafe.invokeCleaner (upper bound)
 *
 * Run with different GCs:
 *   java -Djmh.ignoreLock=true -jar build/distributions/storage-encryption-jmh.jar \
 *     "DirectByteBufferAllocatorBenchmark" \
 *     -f 1 -wi 1 -i 3 -r 5s -w 3s \
 *     -jvmArgs "-XX:+UseZGC -XX:MaxDirectMemorySize=2g" \
 *     -p bufferSizeBytes=8192 -p strategy=allocate_abandon -t 4
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {
    "--enable-native-access=ALL-UNNAMED",
    "--enable-preview",
    "--add-opens", "java.base/sun.misc=ALL-UNNAMED"
})
public class DirectByteBufferAllocatorBenchmark {

    @Param({"8192", "32768"})
    int bufferSizeBytes;

    /**
     * - allocate_abandon:       allocateDirect + drop (baseline GC pressure)
     * - allocate_with_cleaner:  allocateDirect + Cleaner.register (measures Cleaner throughput)
     * - allocate_explicit_free: allocateDirect + Unsafe.invokeCleaner (upper bound, no GC needed)
     */
    @Param({"allocate_abandon", "allocate_with_cleaner", "allocate_explicit_free"})
    String strategy;

    // ---- Shared instrumentation ----
    private static final Cleaner CLEANER = Cleaner.create();
    static final LongAdder cleanerFireCount = new LongAdder();
    static final AtomicLong outstandingBytes = new AtomicLong(0);
    static final LongAdder allocCount = new LongAdder();
    static final LongAdder stallCount = new LongAdder();
    static volatile long nativeMemoryHWM = 0;
    private volatile BufferPoolMXBean directPool;


    @Setup(Level.Trial)
    public void setup() {
        cleanerFireCount.reset();
        outstandingBytes.set(0);
        allocCount.reset();
        stallCount.reset();
        nativeMemoryHWM = 0;
        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool : pools) {
            if ("direct".equals(pool.getName())) {
                directPool = pool;
                break;
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        // Let cleaners drain
        System.gc();
        try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        System.gc();
        try { Thread.sleep(500); } catch (InterruptedException ignored) {}

        long outstanding = outstandingBytes.get();
        long fires = cleanerFireCount.sum();
        long allocs = allocCount.sum();
        long stalls = stallCount.sum();
        long hwm = nativeMemoryHWM;
        long mxUsed = directPool != null ? directPool.getMemoryUsed() : -1;
        long mxCount = directPool != null ? directPool.getCount() : -1;

        System.out.println();
        System.out.println("========== ALLOCATOR BENCHMARK SUMMARY ==========");
        System.out.println("Strategy:              " + strategy);
        System.out.println("Buffer size:           " + bufferSizeBytes + " B");
        System.out.println("Total allocations:     " + allocs);
        System.out.println("Cleaner fires:         " + fires);
        System.out.println("Cleaner backlog:       " + (allocs - fires));
        System.out.println("Outstanding (tracked): " + fmtBytes(outstanding));
        System.out.println("Native HWM (MXBean):   " + fmtBytes(hwm));
        System.out.println("MXBean used (final):   " + fmtBytes(mxUsed));
        System.out.println("MXBean count (final):  " + mxCount);
        System.out.println("Stalls (>1ms):         " + stalls);
        System.out.println("Stall ratio:           "
            + (allocs > 0 ? String.format("%.4f%%", 100.0 * stalls / allocs) : "N/A"));
        System.out.println("=================================================");
        System.out.println();
    }

    // ======================== Core benchmark ========================

    @Benchmark
    @Threads(1)
    public void allocate_1T(Blackhole bh) { doAllocate(bh); }

    @Benchmark
    @Threads(4)
    public void allocate_4T(Blackhole bh) { doAllocate(bh); }

    @Benchmark
    @Threads(8)
    public void allocate_8T(Blackhole bh) { doAllocate(bh); }

    @Benchmark
    @Threads(32)
    public void allocate_32T(Blackhole bh) { doAllocate(bh); }

    private void doAllocate(Blackhole bh) {
        long t0 = System.nanoTime();
        ByteBuffer buf = ByteBuffer.allocateDirect(bufferSizeBytes);
        long elapsed = System.nanoTime() - t0;

        allocCount.increment();
        if (elapsed > 1_000_000L) stallCount.increment(); // > 1ms

        switch (strategy) {
            case "allocate_abandon" -> {
                buf.put(0, (byte) 42);
                bh.consume(buf);
                // drop ref — GC reclaims native memory via DirectByteBuffer's internal Cleaner
            }
            case "allocate_with_cleaner" -> {
                buf.put(0, (byte) 42);
                outstandingBytes.addAndGet(bufferSizeBytes);
                // Custom Cleaner: does NOT free native memory (JDK's Cleaner does that).
                // Only tracks when the object becomes phantom-reachable.
                CLEANER.register(buf, new CleanerAction(bufferSizeBytes));
                bh.consume(buf);
            }
            case "allocate_explicit_free" -> {
                buf.put(0, (byte) 42);
                bh.consume(buf.get(0));
                // Immediately free native memory — upper bound throughput (no GC dependency)
                UNSAFE.invokeCleaner(buf);
            }
            default -> throw new IllegalStateException("Unknown strategy: " + strategy);
        }

        // Sample HWM every ~4096 allocs (cheap bitmask check)
        if ((allocCount.sum() & 0xFFF) == 0 && directPool != null) {
            long used = directPool.getMemoryUsed();
            if (used > nativeMemoryHWM) nativeMemoryHWM = used;
        }
    }

    // ======================== Cleaner action ========================

    /** Must NOT capture ByteBuffer reference — only holds size for accounting. */
    record CleanerAction(int size) implements Runnable {
        @Override
        public void run() {
            outstandingBytes.addAndGet(-size);
            cleanerFireCount.increment();
        }
    }

    // ======================== Unsafe for explicit free ========================

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

    private static String fmtBytes(long b) {
        if (b < 0) return "N/A";
        if (b < 1024) return b + " B";
        if (b < 1024 * 1024) return String.format("%.1f KB", b / 1024.0);
        if (b < 1024L * 1024 * 1024) return String.format("%.1f MB", b / (1024.0 * 1024));
        return String.format("%.2f GB", b / (1024.0 * 1024 * 1024));
    }
}
