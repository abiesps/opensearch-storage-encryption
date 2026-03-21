/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * JMH benchmark comparing Caffeine cache lookup throughput between:
 * <ul>
 *   <li>{@code Cache<FileBlockCacheKey, Object>} — current object-based key (allocates per lookup)</li>
 *   <li>{@code Cache<Long, Object>} — primitive long key via VirtualPage encoding (zero allocation)</li>
 * </ul>
 *
 * <p>This benchmark measures the hot-path cache hit scenario: all keys are pre-populated,
 * every lookup is a hit. The goal is to isolate the key creation + hashCode + equals cost
 * difference between the two approaches.
 *
 * <p>Run:
 * <pre>
 *   ./gradlew jmhJar
 *   java --enable-native-access=ALL-UNNAMED --enable-preview \
 *        -jar build/distributions/storage-encryption-jmh.jar CacheKeyBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 2, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
public class CacheKeyBenchmark {

    // ── Bit layout constants (mirrors VirtualPage, inlined here to avoid dependency) ──

    private static final int BLOCK_ID_BITS = 22;
    private static final long BLOCK_ID_MASK = (1L << BLOCK_ID_BITS) - 1;

    private static long encode(long vfdId, int blockId) {
        return (vfdId << BLOCK_ID_BITS) | (blockId & BLOCK_ID_MASK);
    }

    // ── Shared state: pre-populated caches ──────────────────────────

    @State(Scope.Benchmark)
    public static class CacheState {

        /** Number of distinct files (VFD IDs) in the cache. */
        @Param({ "100", "1000" })
        int fileCount;

        /** Number of blocks per file in the cache. */
        @Param({ "1000" })
        int blocksPerFile;

        /** Caffeine cache keyed by FileBlockCacheKey (current approach). */
        Cache<FileBlockCacheKey, Object> objectKeyCache;

        /** Caffeine cache keyed by Long (VirtualPage approach). */
        Cache<Long, Object> longKeyCache;

        /** Pre-built file paths for lookup (avoids Path.of in the benchmark loop). */
        Path[] filePaths;

        /** Total number of entries. */
        int totalEntries;

        /** Block size for offset computation. */
        static final int BLOCK_SIZE = 8192;

        /** Sentinel value stored in cache (content doesn't matter for key benchmarks). */
        private static final Object SENTINEL = new Object();

        @Setup(Level.Trial)
        public void setup() {
            totalEntries = fileCount * blocksPerFile;

            objectKeyCache = Caffeine.newBuilder().maximumSize(totalEntries + 1000).build();

            longKeyCache = Caffeine.newBuilder().maximumSize(totalEntries + 1000).build();

            filePaths = new Path[fileCount];

            // Populate both caches with identical logical entries
            for (int f = 0; f < fileCount; f++) {
                Path path = Path.of("/data/nodes/0/indices/idx_" + f + "/_0.cfs").toAbsolutePath().normalize();
                filePaths[f] = path;
                long vfdId = f + 1; // VFD IDs start at 1

                for (int b = 0; b < blocksPerFile; b++) {
                    long offset = (long) b * BLOCK_SIZE;

                    // Object key cache
                    objectKeyCache.put(new FileBlockCacheKey(path, offset), SENTINEL);

                    // Long key cache
                    longKeyCache.put(encode(vfdId, b), SENTINEL);
                }
            }
        }
    }

    // ── Per-thread state: random lookup indices ─────────────────────

    @State(Scope.Thread)
    public static class ThreadState {

        /** Pre-computed random file indices for this iteration. */
        int[] fileIndices;

        /** Pre-computed random block indices for this iteration. */
        int[] blockIndices;

        /** Current position in the pre-computed arrays. */
        int pos;

        /** Number of lookups per iteration batch. */
        static final int BATCH = 4096;

        @Setup(Level.Iteration)
        public void setup(CacheState shared) {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            fileIndices = new int[BATCH];
            blockIndices = new int[BATCH];
            for (int i = 0; i < BATCH; i++) {
                fileIndices[i] = rng.nextInt(shared.fileCount);
                blockIndices[i] = rng.nextInt(shared.blocksPerFile);
            }
            pos = 0;
        }

        /** Returns next file index, wrapping around. */
        int nextFile() {
            if (pos >= BATCH)
                pos = 0;
            return fileIndices[pos];
        }

        /** Returns next block index and advances position. */
        int nextBlock() {
            int b = blockIndices[pos];
            pos++;
            return b;
        }
    }

    // ── Benchmarks ──────────────────────────────────────────────────

    /**
     * Baseline: Caffeine lookup with FileBlockCacheKey.
     * Each invocation allocates a new FileBlockCacheKey (Path + String + object).
     */
    @Benchmark
    @Threads(1)
    public void objectKey_singleThread(CacheState shared, ThreadState ts, Blackhole bh) {
        int f = ts.nextFile();
        int b = ts.nextBlock();
        long offset = (long) b * CacheState.BLOCK_SIZE;
        FileBlockCacheKey key = new FileBlockCacheKey(shared.filePaths[f], offset);
        bh.consume(shared.objectKeyCache.getIfPresent(key));
    }

    /**
     * VirtualPage: Caffeine lookup with primitive long key.
     * Zero allocation — encode is pure arithmetic.
     */
    @Benchmark
    @Threads(1)
    public void longKey_singleThread(CacheState shared, ThreadState ts, Blackhole bh) {
        int f = ts.nextFile();
        int b = ts.nextBlock();
        long vfdId = f + 1;
        long key = encode(vfdId, b);
        bh.consume(shared.longKeyCache.getIfPresent(key));
    }

    /**
     * Multi-threaded: FileBlockCacheKey lookup under contention.
     */
    @Benchmark
    @Threads(Threads.MAX)
    public void objectKey_multiThread(CacheState shared, ThreadState ts, Blackhole bh) {
        int f = ts.nextFile();
        int b = ts.nextBlock();
        long offset = (long) b * CacheState.BLOCK_SIZE;
        FileBlockCacheKey key = new FileBlockCacheKey(shared.filePaths[f], offset);
        bh.consume(shared.objectKeyCache.getIfPresent(key));
    }

    /**
     * Multi-threaded: long key lookup under contention.
     */
    @Benchmark
    @Threads(Threads.MAX)
    public void longKey_multiThread(CacheState shared, ThreadState ts, Blackhole bh) {
        int f = ts.nextFile();
        int b = ts.nextBlock();
        long vfdId = f + 1;
        long key = encode(vfdId, b);
        bh.consume(shared.longKeyCache.getIfPresent(key));
    }

    // ── Key creation cost isolation (no cache lookup) ───────────────

    /**
     * Measures pure key creation cost for FileBlockCacheKey during random access.
     * Simulates the block-switch hot path: file path is constant per IndexInput,
     * but blockId changes on every block boundary crossing, so a new key is
     * unavoidable every time.
     */
    @Benchmark
    @Threads(1)
    public void objectKeyCreation(CacheState shared, ThreadState ts, Blackhole bh) {
        int f = ts.nextFile();
        int b = ts.nextBlock();
        long offset = (long) b * CacheState.BLOCK_SIZE;
        bh.consume(new FileBlockCacheKey(shared.filePaths[f], offset));
    }

    /**
     * Measures pure key creation cost for VirtualPage.encode() during random access.
     * Pure arithmetic — shift + OR, zero heap allocation.
     */
    @Benchmark
    @Threads(1)
    public void longKeyCreation(CacheState shared, ThreadState ts, Blackhole bh) {
        int f = ts.nextFile();
        int b = ts.nextBlock();
        long vfdId = f + 1;
        bh.consume(encode(vfdId, b));
    }

    /**
     * Multi-threaded key creation: FileBlockCacheKey under GC pressure
     * from concurrent allocation.
     */
    @Benchmark
    @Threads(Threads.MAX)
    public void objectKeyCreation_multiThread(CacheState shared, ThreadState ts, Blackhole bh) {
        int f = ts.nextFile();
        int b = ts.nextBlock();
        long offset = (long) b * CacheState.BLOCK_SIZE;
        bh.consume(new FileBlockCacheKey(shared.filePaths[f], offset));
    }

    /**
     * Multi-threaded key creation: VirtualPage.encode() — no GC pressure,
     * no contention on allocator.
     */
    @Benchmark
    @Threads(Threads.MAX)
    public void longKeyCreation_multiThread(CacheState shared, ThreadState ts, Blackhole bh) {
        int f = ts.nextFile();
        int b = ts.nextBlock();
        long vfdId = f + 1;
        bh.consume(encode(vfdId, b));
    }
}
