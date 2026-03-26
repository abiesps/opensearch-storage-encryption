/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import sun.misc.Unsafe;

/**
 * Microbenchmark comparing raw read throughput of three buffer access strategies
 * on a single CACHE_BLOCK_SIZE (8KB) direct buffer:
 *
 * <ol>
 *   <li><b>ByteBuffer.get(int)</b> — standard API, bounds-checked per access</li>
 *   <li><b>MemorySegment.get(ValueLayout, offset)</b> — with original segment (bounds-checked)
 *       and with {@code reinterpret()} (bounds-check eliminated on full blocks)</li>
 *   <li><b>Unsafe.get*(address + offset)</b> — zero bounds checks, single instruction after JIT</li>
 * </ol>
 *
 * <p>Each benchmark reads 1024 values at random offsets within the 8KB block to simulate
 * the hot-path read pattern in {@code ByteBufferPoolIndexInput}. The random offsets are
 * pre-computed per iteration to avoid RNG noise in the measurement.
 *
 * <p>Run:
 * <pre>
 *   ./gradlew jmhJar
 *   java --enable-preview --enable-native-access=ALL-UNNAMED \
 *        -jar build/distributions/storage-encryption-jmh.jar BufferAccess
 *
 *   # With perfasm profiler for assembly inspection:
 *   java --enable-preview --enable-native-access=ALL-UNNAMED \
 *        -jar build/distributions/storage-encryption-jmh.jar BufferAccess -prof perfasm
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview",
        "--add-opens=java.base/java.nio=ALL-UNNAMED" })
@State(Scope.Thread)
public class BufferAccessBenchmark {

    /** Block size matching production cache blocks (8KB). */
    private static final int BLOCK_SIZE = 8192;

    /** Number of reads per benchmark invocation — enough to amortize call overhead. */
    private static final int READS_PER_OP = 1024;

    // ---- ValueLayout constants (static final for C2 constant folding) ----
    private static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    private static final ValueLayout.OfInt LAYOUT_LE_INT =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LAYOUT_LE_LONG =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    // ---- Unsafe instance ----
    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ---- Buffers (allocated once per trial) ----
    private ByteBuffer byteBuffer;
    private MemorySegment segment;          // original segment (byteSize = BLOCK_SIZE)
    private MemorySegment segmentReinterp;  // reinterpreted to BLOCK_SIZE (bounds-check elim)
    private long unsafeAddress;

    // ---- Pre-computed random offsets (regenerated each iteration) ----
    private int[] byteOffsets;   // [0, BLOCK_SIZE)
    private int[] intOffsets;    // [0, BLOCK_SIZE - 4)
    private int[] longOffsets;   // [0, BLOCK_SIZE - 8)

    @Setup(Level.Trial)
    public void setupTrial() {
        // Allocate a single direct ByteBuffer — the backing store for all approaches
        byteBuffer = ByteBuffer.allocateDirect(BLOCK_SIZE).order(ByteOrder.LITTLE_ENDIAN);

        // Fill with deterministic data so reads are not all zeros
        for (int i = 0; i < BLOCK_SIZE; i++) {
            byteBuffer.put(i, (byte) ((i * 31 + (i >>> 8) * 7) & 0xFF));
        }

        // MemorySegment wrapping the same direct buffer
        segment = MemorySegment.ofBuffer(byteBuffer);

        // Reinterpreted segment — same address, same size, but a fresh segment
        // whose bounds are exactly BLOCK_SIZE. This is what the POC does with
        // MemorySegment.reinterpret(CACHE_BLOCK_SIZE) to help C2 eliminate
        // bounds checks when it can prove offset < BLOCK_SIZE at compile time.
        segmentReinterp = segment.reinterpret(BLOCK_SIZE);

        // Raw address for Unsafe
        unsafeAddress = segment.address();
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        // Fresh random offsets each iteration to prevent branch predictor gaming
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        byteOffsets = new int[READS_PER_OP];
        intOffsets = new int[READS_PER_OP];
        longOffsets = new int[READS_PER_OP];
        for (int i = 0; i < READS_PER_OP; i++) {
            byteOffsets[i] = rng.nextInt(BLOCK_SIZE);
            intOffsets[i] = rng.nextInt(BLOCK_SIZE - Integer.BYTES + 1);
            longOffsets[i] = rng.nextInt(BLOCK_SIZE - Long.BYTES + 1);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        byteBuffer = null;
        segment = null;
        segmentReinterp = null;
    }

    // ========================================================================
    // readByte benchmarks
    // ========================================================================

    @Benchmark
    public void readByte_ByteBuffer(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < READS_PER_OP; i++) {
            sum += byteBuffer.get(byteOffsets[i]);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void readByte_MemorySegment(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < READS_PER_OP; i++) {
            sum += segment.get(LAYOUT_BYTE, byteOffsets[i]);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void readByte_MemorySegmentReinterpret(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < READS_PER_OP; i++) {
            sum += segmentReinterp.get(LAYOUT_BYTE, byteOffsets[i]);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void readByte_Unsafe(Blackhole bh) {
        int sum = 0;
        long base = unsafeAddress;
        for (int i = 0; i < READS_PER_OP; i++) {
            sum += UNSAFE.getByte(base + byteOffsets[i]);
        }
        bh.consume(sum);
    }

    // ========================================================================
    // readInt benchmarks
    // ========================================================================

    @Benchmark
    public void readInt_ByteBuffer(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < READS_PER_OP; i++) {
            sum += byteBuffer.getInt(intOffsets[i]);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void readInt_MemorySegment(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < READS_PER_OP; i++) {
            sum += segment.get(LAYOUT_LE_INT, intOffsets[i]);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void readInt_MemorySegmentReinterpret(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < READS_PER_OP; i++) {
            sum += segmentReinterp.get(LAYOUT_LE_INT, intOffsets[i]);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void readInt_Unsafe(Blackhole bh) {
        int sum = 0;
        long base = unsafeAddress;
        for (int i = 0; i < READS_PER_OP; i++) {
            sum += UNSAFE.getInt(base + intOffsets[i]);
        }
        bh.consume(sum);
    }

    // ========================================================================
    // readLong benchmarks
    // ========================================================================

    @Benchmark
    public void readLong_ByteBuffer(Blackhole bh) {
        long sum = 0;
        for (int i = 0; i < READS_PER_OP; i++) {
            sum += byteBuffer.getLong(longOffsets[i]);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void readLong_MemorySegment(Blackhole bh) {
        long sum = 0;
        for (int i = 0; i < READS_PER_OP; i++) {
            sum += segment.get(LAYOUT_LE_LONG, longOffsets[i]);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void readLong_MemorySegmentReinterpret(Blackhole bh) {
        long sum = 0;
        for (int i = 0; i < READS_PER_OP; i++) {
            sum += segmentReinterp.get(LAYOUT_LE_LONG, longOffsets[i]);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void readLong_Unsafe(Blackhole bh) {
        long sum = 0;
        long base = unsafeAddress;
        for (int i = 0; i < READS_PER_OP; i++) {
            sum += UNSAFE.getLong(base + longOffsets[i]);
        }
        bh.consume(sum);
    }

    // ========================================================================
    // Bulk copy benchmarks (the MOST critical optimization from POC)
    // Simulates readInts/readLongs/readFloats block-aware bulk copy
    // ========================================================================

    @Benchmark
    public void bulkCopyInt_ByteBuffer(Blackhole bh) {
        // ByteBuffer bulk: must use a loop or IntBuffer view
        int[] dst = new int[BLOCK_SIZE / Integer.BYTES];
        byteBuffer.position(0);
        byteBuffer.asIntBuffer().get(dst);
        bh.consume(dst[0]);
    }

    @Benchmark
    public void bulkCopyInt_MemorySegmentCopy(Blackhole bh) {
        // MemorySegment.copy → Unsafe.copyMemory → native memcpy
        int[] dst = new int[BLOCK_SIZE / Integer.BYTES];
        MemorySegment.copy(segmentReinterp, LAYOUT_LE_INT, 0,
                dst, 0, dst.length);
        bh.consume(dst[0]);
    }

    @Benchmark
    public void bulkCopyInt_Unsafe(Blackhole bh) {
        // Unsafe.copyMemory — raw memcpy
        int[] dst = new int[BLOCK_SIZE / Integer.BYTES];
        UNSAFE.copyMemory(null, unsafeAddress,
                dst, Unsafe.ARRAY_INT_BASE_OFFSET,
                (long) dst.length * Integer.BYTES);
        bh.consume(dst[0]);
    }

    @Benchmark
    public void bulkCopyLong_ByteBuffer(Blackhole bh) {
        long[] dst = new long[BLOCK_SIZE / Long.BYTES];
        byteBuffer.position(0);
        byteBuffer.asLongBuffer().get(dst);
        bh.consume(dst[0]);
    }

    @Benchmark
    public void bulkCopyLong_MemorySegmentCopy(Blackhole bh) {
        long[] dst = new long[BLOCK_SIZE / Long.BYTES];
        MemorySegment.copy(segmentReinterp, LAYOUT_LE_LONG, 0,
                dst, 0, dst.length);
        bh.consume(dst[0]);
    }

    @Benchmark
    public void bulkCopyLong_Unsafe(Blackhole bh) {
        long[] dst = new long[BLOCK_SIZE / Long.BYTES];
        UNSAFE.copyMemory(null, unsafeAddress,
                dst, Unsafe.ARRAY_LONG_BASE_OFFSET,
                (long) dst.length * Long.BYTES);
        bh.consume(dst[0]);
    }
}
