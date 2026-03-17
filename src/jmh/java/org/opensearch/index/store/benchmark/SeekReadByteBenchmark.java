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
 * Benchmarks seek + readByte patterns to measure seek overhead.
 *
 * Simulates the sequentialReadBytesFromClone access pattern:
 *   for each block: seek(offset+1), then read N bytes
 *
 * Compares:
 * 1. lucene_seek_read  — Lucene pattern: seek just sets curPosition, readByte resolves block
 * 2. our_seek_read     — Our pattern: seek computes blockId and resolves offset eagerly
 * 3. our_lazy_seek     — Our structure but with lazy seek (just set curPosition like Lucene)
 *
 * All use same mmap segment. Single block, so seek always stays in-block.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
@State(Scope.Benchmark)
@Threads(1)
public class SeekReadByteBenchmark {

    static final int BLOCK_SIZE = 8192;
    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;

    // Reads per seek — simulates sequentialReadNumBytes (random 0 to blockSize-3)
    @Param({ "4000" })
    int readsPerSeek;

    // Number of seek+read cycles per invocation (simulates blockStartOffsets.length)
    @Param({ "128" })
    int numSeeks;

    private Path tempFile;
    private Arena arena;
    private MemorySegment segment;

    static abstract class AbstractReader {
        abstract void seek(long pos);
        abstract byte readByte();
    }

    // ---- Lucene pattern: seek is trivial, readByte resolves block ----
    static final class LuceneStyleReader extends AbstractReader {
        private final MemorySegment[] segments;
        private final int segmentSize;
        long curPosition;
        int curSegmentIndex;
        MemorySegment curSegment;
        long curSegmentOffset;

        LuceneStyleReader(MemorySegment seg) {
            this.segments = new MemorySegment[] { seg };
            this.segmentSize = BLOCK_SIZE;
            this.curSegment = seg;
            this.curSegmentOffset = 0;
            this.curSegmentIndex = 0;
        }

        @Override void seek(long pos) {
            // Lucene's seek: just set position. Block resolution is lazy.
            curPosition = pos;
        }

        @Override byte readByte() {
            try {
                byte v = curSegment.get(LAYOUT_BYTE, curPosition - curSegmentOffset);
                curPosition++;
                return v;
            } catch (IndexOutOfBoundsException e) {
                // Would switch segment — never happens in single-block test
                curSegmentIndex++;
                curSegment = segments[curSegmentIndex];
                curSegmentOffset = (long) curSegmentIndex * segmentSize;
                byte v = curSegment.get(LAYOUT_BYTE, curPosition - curSegmentOffset);
                curPosition++;
                return v;
            }
        }
    }

    // ---- Our pattern: seek eagerly resolves block, readByte uses two counters ----
    static final class OurStyleReader extends AbstractReader {
        final MemorySegment seg;
        long curPosition;
        int currentOffsetInBlock;
        MemorySegment currentSegment;
        long currentBlockId = 0;
        // Cold fields to match real object layout
        final long length = 1048579L;
        final long absoluteBaseOffset = 0L;
        boolean isOpen = true;

        OurStyleReader(MemorySegment seg) {
            this.seg = seg;
            this.currentSegment = seg;
        }

        @Override void seek(long pos) {
            // Our seek: eagerly compute block and offset
            curPosition = pos;
            long fileOffset = absoluteBaseOffset + pos;
            long blockId = fileOffset >>> 13; // CACHE_BLOCK_SIZE_POWER for 8KB
            if (blockId == currentBlockId && currentSegment != null) {
                currentOffsetInBlock = (int)(fileOffset & (BLOCK_SIZE - 1));
            } else {
                currentOffsetInBlock = BLOCK_SIZE; // force slow path
            }
        }

        @Override byte readByte() {
            if (currentOffsetInBlock >= BLOCK_SIZE || currentSegment == null) {
                throw new AssertionError("Should not happen in single-block test");
            }
            byte v = currentSegment.get(LAYOUT_BYTE, currentOffsetInBlock);
            currentOffsetInBlock++;
            curPosition++;
            return v;
        }
    }

    // ---- Our structure but with lazy seek like Lucene + single counter ----
    static final class OurLazySeekReader extends AbstractReader {
        final MemorySegment seg;
        long curPosition;
        MemorySegment currentSegment;
        long currentSegmentOffset = 0;
        // Cold fields
        final long length = 1048579L;
        final long absoluteBaseOffset = 0L;
        boolean isOpen = true;

        OurLazySeekReader(MemorySegment seg) {
            this.seg = seg;
            this.currentSegment = seg;
        }

        @Override void seek(long pos) {
            // Lazy seek: just set position like Lucene
            curPosition = pos;
        }

        @Override byte readByte() {
            try {
                byte v = currentSegment.get(LAYOUT_BYTE, curPosition - currentSegmentOffset);
                curPosition++;
                return v;
            } catch (IndexOutOfBoundsException e) {
                throw new AssertionError("Should not happen in single-block test");
            }
        }
    }

    private AbstractReader luceneReader;
    private AbstractReader ourReader;
    private AbstractReader ourLazyReader;

    @Setup(Level.Trial)
    @SuppressWarnings("preview")
    public void setup() throws Exception {
        tempFile = Files.createTempFile("seek-bench", ".dat");
        byte[] data = new byte[BLOCK_SIZE];
        new Random(42).nextBytes(data);
        Files.write(tempFile, data);

        arena = Arena.ofShared();
        try (FileChannel fc = FileChannel.open(tempFile, StandardOpenOption.READ)) {
            segment = fc.map(FileChannel.MapMode.READ_ONLY, 0, BLOCK_SIZE, arena);
        }

        luceneReader = new LuceneStyleReader(segment);
        ourReader = new OurStyleReader(segment);
        ourLazyReader = new OurLazySeekReader(segment);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (arena != null) arena.close();
        Files.deleteIfExists(tempFile);
    }

    /** Lucene: trivial seek + single-counter readByte with IOOBE */
    @Benchmark
    public void lucene_seek_read(Blackhole bh) {
        int sum = 0;
        for (int s = 0; s < numSeeks; s++) {
            luceneReader.seek(1);
            for (int i = 0; i < readsPerSeek; i++) {
                sum += luceneReader.readByte();
            }
        }
        bh.consume(sum);
    }

    /** Ours: eager seek (blockId resolution) + two-counter readByte */
    @Benchmark
    public void our_seek_read(Blackhole bh) {
        int sum = 0;
        for (int s = 0; s < numSeeks; s++) {
            ourReader.seek(1);
            for (int i = 0; i < readsPerSeek; i++) {
                sum += ourReader.readByte();
            }
        }
        bh.consume(sum);
    }

    /** Our structure but lazy seek + single counter (best of both) */
    @Benchmark
    public void our_lazy_seek_read(Blackhole bh) {
        int sum = 0;
        for (int s = 0; s < numSeeks; s++) {
            ourLazyReader.seek(1);
            for (int i = 0; i < readsPerSeek; i++) {
                sum += ourLazyReader.readByte();
            }
        }
        bh.consume(sum);
    }
}
