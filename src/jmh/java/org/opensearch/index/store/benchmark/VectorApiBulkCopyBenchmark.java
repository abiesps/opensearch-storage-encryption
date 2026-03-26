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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import sun.misc.Unsafe;

/**
 * Benchmarks bulk copy from a direct ByteBuffer (8KB cache block) into byte[],
 * comparing: ByteBuffer bulk get, MemorySegment.copy, Unsafe.copyMemory,
 * and Vector API (SIMD) explicit vectorized copy.
 *
 * Copy length is parameterized: 64, 512, 4096, 8192 bytes.
 *
 * Run:
 * <pre>
 *   ./gradlew jmhJar
 *   java --enable-preview --enable-native-access=ALL-UNNAMED \
 *        --add-modules jdk.incubator.vector \
 *        -jar build/distributions/storage-encryption-jmh.jar VectorApiBulkCopy
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 1)
@Fork(value = 1, jvmArgsAppend = {
        "--enable-native-access=ALL-UNNAMED",
        "--enable-preview",
        "--add-modules", "jdk.incubator.vector",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
})
@State(Scope.Thread)
public class VectorApiBulkCopyBenchmark {

    private static final int BLOCK_SIZE = 8192;

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

    @Param({"64", "512", "4096", "8192"})
    int copyLen;

    private ByteBuffer srcBuffer;
    private MemorySegment srcSegment;
    private long srcAddress;
    private byte[] dst;
    private MemorySegment dstSegment;

    @Setup(Level.Trial)
    public void setup() {
        srcBuffer = ByteBuffer.allocateDirect(BLOCK_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            srcBuffer.put(i, (byte) ((i * 31 + 7) & 0xFF));
        }
        srcSegment = MemorySegment.ofBuffer(srcBuffer);
        srcAddress = srcSegment.address();
        dst = new byte[copyLen];
        dstSegment = MemorySegment.ofArray(dst);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        srcBuffer = null;
        srcSegment = null;
        dst = null;
        dstSegment = null;
    }

    @Benchmark
    public void bulkCopy_ByteBuffer(Blackhole bh) {
        srcBuffer.get(0, dst, 0, copyLen);
        bh.consume(dst[0]);
    }

    @Benchmark
    public void bulkCopy_MemorySegmentCopy(Blackhole bh) {
        MemorySegment.copy(srcSegment, 0, dstSegment, 0, copyLen);
        bh.consume(dst[0]);
    }

    @Benchmark
    public void bulkCopy_Unsafe(Blackhole bh) {
        UNSAFE.copyMemory(null, srcAddress, dst, Unsafe.ARRAY_BYTE_BASE_OFFSET, copyLen);
        bh.consume(dst[0]);
    }

    @Benchmark
    public void bulkCopy_VectorApi(Blackhole bh) {
        VectorBulkCopy.copyToArray(srcSegment, dst, copyLen);
        bh.consume(dst[0]);
    }

    @Benchmark
    public void bulkCopy_VectorApiSegToSeg(Blackhole bh) {
        VectorBulkCopy.copySegToSeg(srcSegment, dstSegment, copyLen);
        bh.consume(dst[0]);
    }
}
