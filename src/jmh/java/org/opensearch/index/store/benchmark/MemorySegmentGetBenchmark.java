/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmark isolating MemorySegment.get(JAVA_BYTE, offset) performance
 * across different backing types:
 *
 * 1. mmap_segment       — MemorySegment from FileChannel.map (what Lucene uses)
 * 2. dbb_segment        — MemorySegment.ofBuffer(directByteBuffer.asReadOnlyBuffer())
 * 3. dbb_reinterpret    — same as #2 but with .reinterpret(BLOCK_SIZE)
 * 4. bytebuffer_get     — ByteBuffer.get(offset) directly (no MemorySegment)
 * 5. native_segment     — MemorySegment.ofAddress(nativeAddr).reinterpret(BLOCK_SIZE)
 *
 * All read the same 8KB block of data sequentially, one byte at a time.
 * This proves which backing type is the bottleneck.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
@State(Scope.Benchmark)
@Threads(1)
public class MemorySegmentGetBenchmark {

    static final int BLOCK_SIZE = 8192;
    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;

    // Number of bytes to read per invocation (simulates reading within one block)
    @Param({ "8000" })
    int bytesToRead;

    private Path tempFile;
    private Arena mmapArena;

    // Variant 1: mmap-backed MemorySegment
    private MemorySegment mmapSegment;

    // Variant 2: DirectByteBuffer-backed MemorySegment (asReadOnlyBuffer)
    private MemorySegment dbbSegment;
    private ByteBuffer dbbReadOnly;

    // Variant 3: DirectByteBuffer-backed MemorySegment after reinterpret
    private MemorySegment dbbReinterpretSegment;

    // Variant 4: raw ByteBuffer for direct get()
    private ByteBuffer rawByteBuffer;

    // Variant 5: native address MemorySegment (ofAddress + reinterpret)
    private MemorySegment nativeSegment;

    @Setup(Level.Trial)
    @SuppressWarnings("preview")
    public void setup() throws Exception {
        // Create a temp file with random data
        tempFile = Files.createTempFile("memseg-bench", ".dat");
        byte[] data = new byte[BLOCK_SIZE];
        new Random(42).nextBytes(data);
        Files.write(tempFile, data);

        // 1. mmap-backed segment (what Lucene does)
        mmapArena = Arena.ofShared();
        try (FileChannel fc = FileChannel.open(tempFile, StandardOpenOption.READ)) {
            mmapSegment = fc.map(FileChannel.MapMode.READ_ONLY, 0, BLOCK_SIZE, mmapArena);
        }

        // 2. DirectByteBuffer → asReadOnlyBuffer → MemorySegment.ofBuffer
        ByteBuffer directBuf = ByteBuffer.allocateDirect(BLOCK_SIZE);
        directBuf.put(data);
        directBuf.flip();
        dbbReadOnly = directBuf.asReadOnlyBuffer();
        dbbSegment = MemorySegment.ofBuffer(dbbReadOnly);

        // 3. Same as #2 but with reinterpret (what our code does)
        ByteBuffer directBuf2 = ByteBuffer.allocateDirect(BLOCK_SIZE);
        directBuf2.put(data);
        directBuf2.flip();
        ByteBuffer ro2 = directBuf2.asReadOnlyBuffer();
        dbbReinterpretSegment = MemorySegment.ofBuffer(ro2).reinterpret(BLOCK_SIZE);

        // 4. Raw ByteBuffer for direct get()
        rawByteBuffer = ByteBuffer.allocateDirect(BLOCK_SIZE);
        rawByteBuffer.put(data);
        rawByteBuffer.flip();
        rawByteBuffer = rawByteBuffer.asReadOnlyBuffer();

        // 5. Native address segment — extract address from DirectByteBuffer, create native segment
        ByteBuffer directBuf3 = ByteBuffer.allocateDirect(BLOCK_SIZE);
        directBuf3.put(data);
        directBuf3.flip();
        long nativeAddr = MemorySegment.ofBuffer(directBuf3).address();
        nativeSegment = MemorySegment.ofAddress(nativeAddr).reinterpret(BLOCK_SIZE);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (mmapArena != null) mmapArena.close();
        Files.deleteIfExists(tempFile);
    }

    @Benchmark
    public void mmap_segment(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < bytesToRead; i++) {
            sum += mmapSegment.get(LAYOUT_BYTE, i);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void dbb_segment(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < bytesToRead; i++) {
            sum += dbbSegment.get(LAYOUT_BYTE, i);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void dbb_reinterpret(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < bytesToRead; i++) {
            sum += dbbReinterpretSegment.get(LAYOUT_BYTE, i);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void bytebuffer_get(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < bytesToRead; i++) {
            sum += rawByteBuffer.get(i);
        }
        bh.consume(sum);
    }

    @Benchmark
    public void native_segment(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < bytesToRead; i++) {
            sum += nativeSegment.get(LAYOUT_BYTE, i);
        }
        bh.consume(sum);
    }
}
