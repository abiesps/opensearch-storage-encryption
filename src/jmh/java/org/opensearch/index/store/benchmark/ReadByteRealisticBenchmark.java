/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Realistic readByte() benchmark that simulates actual object field access patterns.
 * Uses inner classes to mimic Lucene's MultiSegmentImpl vs our DirectByteBufferIndexInput.
 *
 * Tests:
 * 1. final_inner_class  — Lucene pattern: final static inner class, single counter, IOOBE
 * 2. top_level_class    — Our pattern: non-final class, two counters, explicit check
 * 3. top_level_final    — Our pattern but class is final
 * 4. top_level_single   — Our class structure but single counter like Lucene
 *
 * All called through an abstract base type to simulate IndexInput polymorphism.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
@State(Scope.Benchmark)
@Threads(1)
public class ReadByteRealisticBenchmark {

    static final int BLOCK_SIZE = 8192;
    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;

    @Param({ "8000" })
    int bytesToRead;

    private Path tempFile;
    private Arena arena;
    private MemorySegment segment;

    // Readers stored as the abstract base type (like IndexInput)
    private AbstractReader luceneReader;
    private AbstractReader ourReader;
    private AbstractReader ourFinalReader;
    private AbstractReader ourSingleCounterReader;

    // ---- Abstract base (simulates IndexInput) ----
    static abstract class AbstractReader {
        abstract byte readByte();
        abstract void reset();
    }

    // ---- Lucene pattern: final static inner class, single counter, IOOBE ----
    static final class LuceneStyleReader extends AbstractReader {
        private final MemorySegment seg;
        private long curPosition;
        private final long segOffset = 0;

        LuceneStyleReader(MemorySegment seg) { this.seg = seg; }

        @Override byte readByte() {
            try {
                byte v = seg.get(LAYOUT_BYTE, curPosition - segOffset);
                curPosition++;
                return v;
            } catch (IndexOutOfBoundsException e) {
                throw new AssertionError();
            }
        }

        @Override void reset() { curPosition = 0; }
    }

    // ---- Our pattern: non-final, two counters, explicit check ----
    static class OurStyleReader extends AbstractReader {
        final MemorySegment seg;
        long curPosition;
        int offsetInBlock;
        MemorySegment currentSegment;

        OurStyleReader(MemorySegment seg) {
            this.seg = seg;
            this.currentSegment = seg;
        }

        @Override byte readByte() {
            if (offsetInBlock >= BLOCK_SIZE || currentSegment == null) {
                throw new AssertionError();
            }
            byte v = currentSegment.get(LAYOUT_BYTE, offsetInBlock);
            offsetInBlock++;
            curPosition++;
            return v;
        }

        @Override void reset() { curPosition = 0; offsetInBlock = 0; currentSegment = seg; }
    }

    // ---- Our pattern but final class ----
    static final class OurStyleFinalReader extends AbstractReader {
        final MemorySegment seg;
        long curPosition;
        int offsetInBlock;
        MemorySegment currentSegment;

        OurStyleFinalReader(MemorySegment seg) {
            this.seg = seg;
            this.currentSegment = seg;
        }

        @Override byte readByte() {
            if (offsetInBlock >= BLOCK_SIZE || currentSegment == null) {
                throw new AssertionError();
            }
            byte v = currentSegment.get(LAYOUT_BYTE, offsetInBlock);
            offsetInBlock++;
            curPosition++;
            return v;
        }

        @Override void reset() { curPosition = 0; offsetInBlock = 0; currentSegment = seg; }
    }

    // ---- Our class structure but single counter ----
    static class OurSingleCounterReader extends AbstractReader {
        final MemorySegment seg;
        long curPosition;
        final long segOffset = 0;
        MemorySegment currentSegment;

        OurSingleCounterReader(MemorySegment seg) {
            this.seg = seg;
            this.currentSegment = seg;
        }

        @Override byte readByte() {
            try {
                byte v = currentSegment.get(LAYOUT_BYTE, curPosition - segOffset);
                curPosition++;
                return v;
            } catch (IndexOutOfBoundsException e) {
                throw new AssertionError();
            }
        }

        @Override void reset() { curPosition = 0; currentSegment = seg; }
    }

    @Setup(Level.Trial)
    @SuppressWarnings("preview")
    public void setup() throws Exception {
        tempFile = Files.createTempFile("readbyte-real-bench", ".dat");
        byte[] data = new byte[BLOCK_SIZE];
        new Random(42).nextBytes(data);
        Files.write(tempFile, data);

        arena = Arena.ofShared();
        try (FileChannel fc = FileChannel.open(tempFile, StandardOpenOption.READ)) {
            segment = fc.map(FileChannel.MapMode.READ_ONLY, 0, BLOCK_SIZE, arena);
        }

        luceneReader = new LuceneStyleReader(segment);
        ourReader = new OurStyleReader(segment);
        ourFinalReader = new OurStyleFinalReader(segment);
        ourSingleCounterReader = new OurSingleCounterReader(segment);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (arena != null) arena.close();
        Files.deleteIfExists(tempFile);
    }

    @Benchmark
    public void final_inner_class(Blackhole bh) {
        luceneReader.reset();
        int sum = 0;
        for (int i = 0; i < bytesToRead; i++) {
            sum += luceneReader.readByte();
        }
        bh.consume(sum);
    }

    @Benchmark
    public void top_level_class(Blackhole bh) {
        ourReader.reset();
        int sum = 0;
        for (int i = 0; i < bytesToRead; i++) {
            sum += ourReader.readByte();
        }
        bh.consume(sum);
    }

    @Benchmark
    public void top_level_final(Blackhole bh) {
        ourFinalReader.reset();
        int sum = 0;
        for (int i = 0; i < bytesToRead; i++) {
            sum += ourFinalReader.readByte();
        }
        bh.consume(sum);
    }

    @Benchmark
    public void top_level_single_counter(Blackhole bh) {
        ourSingleCounterReader.reset();
        int sum = 0;
        for (int i = 0; i < bytesToRead; i++) {
            sum += ourSingleCounterReader.readByte();
        }
        bh.consume(sum);
    }
}
