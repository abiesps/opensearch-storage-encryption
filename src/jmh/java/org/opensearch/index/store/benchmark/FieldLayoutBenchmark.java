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
 * Tests whether object field layout (cache line pollution) affects readByte() throughput.
 *
 * Compares:
 * 1. slim_reader    — only hot fields (segment, offset, position) — 3 fields
 * 2. fat_reader     — hot fields + many cold fields mimicking DirectByteBufferIndexInput
 * 3. lucene_reader  — Lucene-style single counter with minimal fields
 *
 * All called through abstract base type. All use same mmap segment.
 * This tests whether having 10+ cold fields on the object degrades
 * readByte() performance due to cache line effects.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
@State(Scope.Benchmark)
@Threads(1)
public class FieldLayoutBenchmark {

    static final int BLOCK_SIZE = 8192;
    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;

    @Param({ "8000" })
    int bytesToRead;

    private Path tempFile;
    private Arena arena;
    private MemorySegment segment;

    private AbstractReader slimReader;
    private AbstractReader fatReader;
    private AbstractReader luceneReader;

    static abstract class AbstractReader {
        abstract byte readByte();
        abstract void reset();
    }

    // Minimal fields — just what readByte() needs
    static final class SlimReader extends AbstractReader {
        MemorySegment currentSegment;
        int currentOffsetInBlock;
        long curPosition;
        private final MemorySegment seg;

        SlimReader(MemorySegment seg) { this.seg = seg; this.currentSegment = seg; }

        @Override byte readByte() {
            if (currentOffsetInBlock >= BLOCK_SIZE || currentSegment == null) {
                throw new AssertionError();
            }
            byte v = currentSegment.get(LAYOUT_BYTE, currentOffsetInBlock);
            currentOffsetInBlock++;
            curPosition++;
            return v;
        }

        @Override void reset() { curPosition = 0; currentOffsetInBlock = 0; currentSegment = seg; }
    }

    // Many cold fields mimicking DirectByteBufferIndexInput's full field set
    static final class FatReader extends AbstractReader {
        // Cold fields (same types/count as DirectByteBufferIndexInput)
        final long length = 1048579L;
        final int vfd = 42;
        final Object blockCache = new Object(); // simulates CaffeineBlockCacheV2 ref
        final long absoluteBaseOffset = 0L;
        final boolean isClone = true;
        final Object blockTable = new Object(); // simulates SparseLongBlockTable ref
        boolean isOpen = true;
        private long currentBlockId = 0;
        private int lastOffsetInBlock = 0;

        // Hot fields
        MemorySegment currentSegment;
        int currentOffsetInBlock;
        long curPosition;
        private final MemorySegment seg;

        FatReader(MemorySegment seg) { this.seg = seg; this.currentSegment = seg; }

        @Override byte readByte() {
            if (currentOffsetInBlock >= BLOCK_SIZE || currentSegment == null) {
                throw new AssertionError();
            }
            byte v = currentSegment.get(LAYOUT_BYTE, currentOffsetInBlock);
            currentOffsetInBlock++;
            curPosition++;
            return v;
        }

        @Override void reset() { curPosition = 0; currentOffsetInBlock = 0; currentSegment = seg; }
    }

    // Lucene-style: single counter, minimal fields
    static final class LuceneReader extends AbstractReader {
        long curPosition;
        final long segOffset = 0;
        final MemorySegment seg;

        LuceneReader(MemorySegment seg) { this.seg = seg; }

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

    @Setup(Level.Trial)
    @SuppressWarnings("preview")
    public void setup() throws Exception {
        tempFile = Files.createTempFile("field-layout-bench", ".dat");
        byte[] data = new byte[BLOCK_SIZE];
        new Random(42).nextBytes(data);
        Files.write(tempFile, data);

        arena = Arena.ofShared();
        try (FileChannel fc = FileChannel.open(tempFile, StandardOpenOption.READ)) {
            segment = fc.map(FileChannel.MapMode.READ_ONLY, 0, BLOCK_SIZE, arena);
        }

        slimReader = new SlimReader(segment);
        fatReader = new FatReader(segment);
        luceneReader = new LuceneReader(segment);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (arena != null) arena.close();
        Files.deleteIfExists(tempFile);
    }

    @Benchmark
    public void slim_two_counter(Blackhole bh) {
        slimReader.reset();
        int sum = 0;
        for (int i = 0; i < bytesToRead; i++) {
            sum += slimReader.readByte();
        }
        bh.consume(sum);
    }

    @Benchmark
    public void fat_two_counter(Blackhole bh) {
        fatReader.reset();
        int sum = 0;
        for (int i = 0; i < bytesToRead; i++) {
            sum += fatReader.readByte();
        }
        bh.consume(sum);
    }

    @Benchmark
    public void lucene_single_counter(Blackhole bh) {
        luceneReader.reset();
        int sum = 0;
        for (int i = 0; i < bytesToRead; i++) {
            sum += luceneReader.readByte();
        }
        bh.consume(sum);
    }
}
