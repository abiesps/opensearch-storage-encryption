/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.bufferpoolfs.RadixBlockTable;

/**
 * Measures the cost of tryPinIfGeneration (VarHandle CAS) on top of a plain
 * RadixBlockTable.get() lookup.
 *
 * <h2>Benchmark A: plainGet</h2>
 * {@code RadixBlockTable.get(blockId)} — two plain array loads, no atomics.
 *
 * <h2>Benchmark B: getWithTryPin</h2>
 * {@code RadixBlockTable.get(blockId)} followed by
 * {@code RefCountedMemorySegment.tryPinIfGeneration(gen)} — adds a volatile
 * read + CAS on the packed (generation, refCount) state word.
 *
 * <p>No unpin is performed — we intentionally leak pins so the benchmark
 * isolates the pure acquisition cost without the decRef CAS overhead.
 * RefCount will climb but never overflow in a short benchmark run.
 *
 * <h2>Run</h2>
 * <pre>
 *   # Single-threaded baseline
 *   java -jar build/distributions/storage-encryption-jmh.jar \
 *     "RadixLookupPinCostBenchmark" -f 2 -wi 3 -i 5 -r 5s -w 3s -t 1
 *
 *   # 32-thread contention test
 *   java -jar build/distributions/storage-encryption-jmh.jar \
 *     "RadixLookupPinCostBenchmark" -f 2 -wi 3 -i 5 -r 5s -w 3s -t 32
 *
 *   # Sweep: run at 1, 4, 8, 16, 32 threads
 *   for t in 1 4 8 16 32; do
 *     java -jar build/distributions/storage-encryption-jmh.jar \
 *       "RadixLookupPinCostBenchmark" -f 2 -wi 3 -i 5 -r 5s -w 3s -t $t
 *   done
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED" })
public class RadixLookupPinCostBenchmark {

    /** Number of distinct blocks pre-loaded into the table. */
    private static final int NUM_BLOCKS = 100_000;

    /** Tiny segment size — content doesn't matter, we never read it. */
    private static final int SEG_BYTES = 64;

    private Arena arena;
    private RadixBlockTable<RefCountedMemorySegment> table;
    private int[] generations;

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        table = new RadixBlockTable<>();
        generations = new int[NUM_BLOCKS];

        for (int i = 0; i < NUM_BLOCKS; i++) {
            MemorySegment seg = arena.allocate(SEG_BYTES);
            RefCountedMemorySegment ref = new RefCountedMemorySegment(seg, SEG_BYTES, s -> { /* no-op releaser */ });
            table.put(i, ref);
            generations[i] = ref.getGeneration();
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        table.clear();
        arena.close();
    }

    /**
     * Baseline: plain RadixBlockTable.get() — two array loads, no atomics.
     */
    @Benchmark
    public void plainGet(Blackhole bh) {
        long blockId = ThreadLocalRandom.current().nextInt(NUM_BLOCKS);
        RefCountedMemorySegment v = table.get(blockId);
        bh.consume(v);
    }

    /**
     * RadixBlockTable.get() + tryPinIfGeneration() — adds VarHandle CAS.
     * No unpin — isolates pure pin acquisition cost.
     */
    @Benchmark
    public void getWithTryPin(Blackhole bh) {
        int blockIdx = ThreadLocalRandom.current().nextInt(NUM_BLOCKS);
        RefCountedMemorySegment v = table.get(blockIdx);
        if (v != null) {
            boolean pinned = v.tryPinIfGeneration(generations[blockIdx]);
            bh.consume(pinned);
        }
        bh.consume(v);
    }
}
