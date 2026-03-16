/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Hot path benchmark: all data is already resident in block cache (BufferPool)
 * or page cache (MMap). Measures pure read throughput without I/O latency.
 *
 * <p>Each benchmark method spawns {@code threadCount} concurrent reader threads,
 * each with its own IndexInput clone. This gives true concurrent read contention
 * as a JMH {@code @Param} without relying on {@code @Threads} annotation.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 2)
//@Fork(value = 2, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
@State(Scope.Benchmark)
@Threads(1)
public class HotPathReadBenchmarks extends ReadBenchmarkBase {

    // Expand to "1", "4", "8", "16", "32" for full sweep
    @Param({ "1", "32" })
    public int threadCount;

    private ExecutorService executor;

    /**
     * Pre-warm caches by reading all files once.
     *
     * For MMap: we read through the already-opened indexInputs[] and seek back to 0.
     * Opening separate temporary IndexInputs and closing them can invalidate shared
     * arenas in Lucene 10.x, which would corrupt the benchmark IndexInputs.
     *
     * For BufferPool/V0/DirectBufferPool: we open separate inputs to populate the
     * block cache without affecting the benchmark inputs' cursor positions.
     */
    @Setup(Level.Trial)
    public void setupHotPathTrial() throws Exception {
        super.setupTrial();

        final int blockSize = StaticConfigs.CACHE_BLOCK_SIZE;
        byte[] buf = new byte[blockSize];
        // BufferPool: read all files to populate block cache
        if ("bufferpool".equals(directoryType)) {
            for (String fileName : fileNames) {
                try (IndexInput in = bufferPoolDirectory.openInput(fileName, org.apache.lucene.store.IOContext.DEFAULT)) {
                    long remaining = in.length();
                    while (remaining > 0) {
                        int toRead = (int) Math.min(buf.length, remaining);
                        in.readBytes(buf, 0, toRead);
                        remaining -= toRead;
                    }
                }
            }
        }
        // MMap: read through the already-opened indexInputs to populate page cache,
        // then seek back to 0. Do NOT open separate inputs — closing them can
        // invalidate shared arenas and corrupt the benchmark IndexInputs.
        if ("mmap".equals(directoryType) || "mmap_single".equals(directoryType)) {
            for (IndexInput in : indexInputs) {
                in.seek(0);
                long remaining = in.length();
                while (remaining > 0) {
                    int toRead = (int) Math.min(buf.length, remaining);
                    in.readBytes(buf, 0, toRead);
                    remaining -= toRead;
                }
                in.seek(0);
            }
        }
        // BufferPoolV0: read all files to populate block cache (original V0 implementation)
        if ("bufferpoolv0".equals(directoryType)) {
            for (String fileName : fileNames) {
                try (IndexInput in = bufferPoolDirectoryV0.openInput(fileName, org.apache.lucene.store.IOContext.DEFAULT)) {
                    long remaining = in.length();
                    while (remaining > 0) {
                        int toRead = (int) Math.min(buf.length, remaining);
                        in.readBytes(buf, 0, toRead);
                        remaining -= toRead;
                    }
                }
            }
        }
        // DirectBufferPool: read all files to populate Caffeine block cache
        if ("directbufferpool".equals(directoryType)) {
            for (String fileName : fileNames) {
                try (IndexInput in = directBufferPoolDirectory.openInput(fileName, org.apache.lucene.store.IOContext.DEFAULT)) {
                    long remaining = in.length();
                    while (remaining > 0) {
                        int toRead = (int) Math.min(buf.length, remaining);
                        in.readBytes(buf, 0, toRead);
                        remaining -= toRead;
                    }
                }
            }
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        AtomicInteger counter = new AtomicInteger();
        executor = Executors.newFixedThreadPool(threadCount, r -> {
            Thread t = new Thread(r, "jmh-reader-" + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Runs a task on {@code threadCount} threads concurrently.
     * Waits for all to complete and consumes results via Blackhole.
     */
    private void runConcurrent(ReaderTask task, Blackhole bh) throws Exception {
        List<Future<?>> futures = new ArrayList<>(threadCount);
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    byte[] buf = new byte[StaticConfigs.CACHE_BLOCK_SIZE * 2];
                    task.run(buf, bh);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get();
        }
    }

    @FunctionalInterface
    interface ReaderTask {
        void run(byte[] buf, Blackhole bh) throws IOException;
    }

    // ---- Random reads via clone (crossing block boundaries) ----
    @Benchmark
    public void randomReadByteFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            // Randomly read bytes from distinct blocks across all files
            long[] randomReadOffsets = randomReadByteOffsets();
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    RandomAccessInput in = (RandomAccessInput) fileInput;
                    for (long offset : randomReadOffsets) {
                        byte b = in.readByte(offset);
                        hole.consume(b);
                    }
                } finally {
                    // MMap clones with MultiSegmentImpl share the parent's MemorySegment[] array.
                    // Closing a clone nulls out segments, corrupting the parent and other clones.
                    // Only close non-mmap clones; mmap clones are lightweight and GC'd naturally.
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // Sequentially read X bytes from each block (no block boundary crossing)

    @Benchmark
    public void sequentialReadBytesFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    for (long offset : blockStartOffsets) {
                        int dummyConsumer = 0;
                        fileInput.seek(offset+1);
                        long remaining = Math.min(sequentialReadNumBytes, fileSize - offset-1);
                        for (long i = 0; i < remaining; i++) {
                            byte b = fileInput.readByte();
                            dummyConsumer += b;
                        }
                        hole.consume(dummyConsumer);
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    /**
     * Reads bytes within a single block only — no block transitions.
     * Seek to block 0 offset 1, read sequentialReadNumBytes, repeat.
     * This isolates the pure in-block readByte() cost.
     */
    @Benchmark
    public void singleBlockReadBytesFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    // Repeat the same number of iterations as sequentialReadBytesFromClone
                    // but always within block 0 — no block transitions
                    for (int iter = 0; iter < blockStartOffsets.length; iter++) {
                        int dummyConsumer = 0;
                        fileInput.seek(1); // always block 0, offset 1
                        long remaining = Math.min(sequentialReadNumBytes, fileSize - 1);
                        for (long i = 0; i < remaining; i++) {
                            byte b = fileInput.readByte();
                            dummyConsumer += b;
                        }
                        hole.consume(dummyConsumer);
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // ======================== RandomAccessInput API benchmarks ========================

    // RandomAccessInput.readShort(long pos) — random positions across blocks
    //@Benchmark
    public void randomReadShortFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            long[] offsets = randomReadByteOffsets();
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    RandomAccessInput in = (RandomAccessInput) fileInput;
                    for (long offset : offsets) {
                        if (offset + Short.BYTES <= fileSize) {
                            hole.consume(in.readShort(offset));
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // RandomAccessInput.readInt(long pos) — random positions across blocks
    //@Benchmark
    public void randomReadIntFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            long[] offsets = randomReadByteOffsets();
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    RandomAccessInput in = (RandomAccessInput) fileInput;
                    for (long offset : offsets) {
                        if (offset + Integer.BYTES <= fileSize) {
                            hole.consume(in.readInt(offset));
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // RandomAccessInput.readLong(long pos) — random positions across blocks
    //@Benchmark
    public void randomReadLongFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            long[] offsets = randomReadByteOffsets();
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    RandomAccessInput in = (RandomAccessInput) fileInput;
                    for (long offset : offsets) {
                        if (offset + Long.BYTES <= fileSize) {
                            hole.consume(in.readLong(offset));
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // RandomAccessInput.readBytes(long pos, byte[], int offset, int length) — bulk random read
    @Benchmark
    public void randomReadBulkBytesFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            long[] offsets = randomReadByteOffsets();
            byte[] readBuf = new byte[StaticConfigs.CACHE_BLOCK_SIZE];
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    RandomAccessInput in = (RandomAccessInput) fileInput;
                    for (long offset : offsets) {
                        int readable = (int) Math.min(readBuf.length, fileSize - offset);
                        if (readable > 0) {
                            in.readBytes(offset, readBuf, 0, readable);
                            hole.consume(readBuf[0]);
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // ======================== IndexInput sequential API benchmarks ========================

    // IndexInput.readBytes(byte[], int, int) — bulk sequential read from block starts
    @Benchmark
    public void sequentialReadBulkBytesFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            byte[] readBuf = new byte[StaticConfigs.CACHE_BLOCK_SIZE];
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    for (long offset : blockStartOffsets) {
                        fileInput.seek(offset);
                        int readable = (int) Math.min(readBuf.length, fileSize - offset);
                        if (readable > 0) {
                            fileInput.readBytes(readBuf, 0, readable);
                            hole.consume(readBuf[0]);
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // IndexInput.readShort() — sequential short reads from block starts
    //@Benchmark
    public void sequentialReadShortFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    for (long offset : blockStartOffsets) {
                        if (offset + Short.BYTES <= fileSize) {
                            fileInput.seek(offset);
                            hole.consume(fileInput.readShort());
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // IndexInput.readInt() — sequential int reads from block starts
    //@Benchmark
    public void sequentialReadIntFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    for (long offset : blockStartOffsets) {
                        if (offset + Integer.BYTES <= fileSize) {
                            fileInput.seek(offset);
                            hole.consume(fileInput.readInt());
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // IndexInput.readLong() — sequential long reads from block starts
    //@Benchmark
    public void sequentialReadLongFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    for (long offset : blockStartOffsets) {
                        if (offset + Long.BYTES <= fileSize) {
                            fileInput.seek(offset);
                            hole.consume(fileInput.readLong());
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // IndexInput.readInts(int[], int, int) — bulk int array read from block starts
   // @Benchmark
    public void sequentialReadIntsFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            int numInts = StaticConfigs.CACHE_BLOCK_SIZE / Integer.BYTES;
            int[] intBuf = new int[numInts];
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    for (long offset : blockStartOffsets) {
                        long remaining = fileSize - offset;
                        int readable = (int) Math.min(numInts, remaining / Integer.BYTES);
                        if (readable > 0) {
                            fileInput.seek(offset);
                            fileInput.readInts(intBuf, 0, readable);
                            hole.consume(intBuf[0]);
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // IndexInput.readLongs(long[], int, int) — bulk long array read from block starts
    //@Benchmark
    public void sequentialReadLongsFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            int numLongs = StaticConfigs.CACHE_BLOCK_SIZE / Long.BYTES;
            long[] longBuf = new long[numLongs];
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    for (long offset : blockStartOffsets) {
                        long remaining = fileSize - offset;
                        int readable = (int) Math.min(numLongs, remaining / Long.BYTES);
                        if (readable > 0) {
                            fileInput.seek(offset);
                            fileInput.readLongs(longBuf, 0, readable);
                            hole.consume(longBuf[0]);
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // IndexInput.readFloats(float[], int, int) — bulk float array read from block starts
    //@Benchmark
    public void sequentialReadFloatsFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            int numFloats = StaticConfigs.CACHE_BLOCK_SIZE / Float.BYTES;
            float[] floatBuf = new float[numFloats];
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    for (long offset : blockStartOffsets) {
                        long remaining = fileSize - offset;
                        int readable = (int) Math.min(numFloats, remaining / Float.BYTES);
                        if (readable > 0) {
                            fileInput.seek(offset);
                            fileInput.readFloats(floatBuf, 0, readable);
                            hole.consume(floatBuf[0]);
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // IndexInput.slice() + read — create a slice and read through it
    //@Benchmark
    public void sliceReadBytesFromClone(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            byte[] readBuf = new byte[StaticConfigs.CACHE_BLOCK_SIZE];
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    for (long offset : blockStartOffsets) {
                        long sliceLen = Math.min(StaticConfigs.CACHE_BLOCK_SIZE, fileSize - offset);
                        if (sliceLen > 0) {
                            try (IndexInput slice = fileInput.slice("bench-slice", offset, sliceLen)) {
                                slice.readBytes(readBuf, 0, (int) sliceLen);
                                hole.consume(readBuf[0]);
                            }
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // IndexInput.skipBytes(long) — seek + skip pattern
    //@Benchmark
    public void skipBytesFromClone(Blackhole bh) throws Exception {
        runConcurrent((buf, hole) -> {
            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    fileInput.seek(0);
                    long pos = 0;
                    while (pos + StaticConfigs.CACHE_BLOCK_SIZE < fileSize) {
                        fileInput.skipBytes(StaticConfigs.CACHE_BLOCK_SIZE);
                        pos += StaticConfigs.CACHE_BLOCK_SIZE;
                        if (pos + 1 <= fileSize) {
                            hole.consume(fileInput.readByte());
                            pos++;
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    // Mixed workload: randomly exercises all read APIs in a single benchmark
    // to simulate realistic Lucene read patterns where different APIs are interleaved.
    static Random random = new Random();
   // @Benchmark
    public void mixedReadWorkload(Blackhole bh) throws Exception {
        runConcurrent(( buf, hole) -> {
            long[] offsets = randomReadByteOffsets();
            byte[] readBuf = new byte[StaticConfigs.CACHE_BLOCK_SIZE];
            int numInts = StaticConfigs.CACHE_BLOCK_SIZE / Integer.BYTES;
            int[] intBuf = new int[numInts];
            int numLongs = StaticConfigs.CACHE_BLOCK_SIZE / Long.BYTES;
            long[] longBuf = new long[numLongs];
            int numFloats = StaticConfigs.CACHE_BLOCK_SIZE / Float.BYTES;
            float[] floatBuf = new float[numFloats];

            for (int fileIdx = 0; fileIdx < numFilesToRead; fileIdx++) {
                IndexInput fileInput = indexInputs[fileIdx].clone();
                try {
                    RandomAccessInput rai = (RandomAccessInput) fileInput;
                    for (int i = 0; i < offsets.length; i++) {
                        long offset = offsets[i];
                        int op = random.nextInt(12);
                        switch (op) {
                            case 0 -> hole.consume(rai.readByte(offset));
                            case 1 -> {
                                if (offset + Short.BYTES <= fileSize)
                                    hole.consume(rai.readShort(offset));
                            }
                            case 2 -> {
                                if (offset + Integer.BYTES <= fileSize)
                                    hole.consume(rai.readInt(offset));
                            }
                            case 3 -> {
                                if (offset + Long.BYTES <= fileSize)
                                    hole.consume(rai.readLong(offset));
                            }
                            case 4 -> {
                                int readable = (int) Math.min(readBuf.length, fileSize - offset);
                                if (readable > 0) {
                                    rai.readBytes(offset, readBuf, 0, readable);
                                    hole.consume(readBuf[0]);
                                }
                            }
                            case 5 -> {
                                fileInput.seek(offset);
                                hole.consume(fileInput.readByte());
                            }
                            case 6 -> {
                                fileInput.seek(offset);
                                int readable = (int) Math.min(readBuf.length, fileSize - offset);
                                if (readable > 0) {
                                    fileInput.readBytes(readBuf, 0, readable);
                                    hole.consume(readBuf[0]);
                                }
                            }
                            case 7 -> {
                                fileInput.seek(offset);
                                long remaining = fileSize - offset;
                                int readable = (int) Math.min(numInts, remaining / Integer.BYTES);
                                if (readable > 0) {
                                    fileInput.readInts(intBuf, 0, readable);
                                    hole.consume(intBuf[0]);
                                }
                            }
                            case 8 -> {
                                fileInput.seek(offset);
                                long remaining = fileSize - offset;
                                int readable = (int) Math.min(numLongs, remaining / Long.BYTES);
                                if (readable > 0) {
                                    fileInput.readLongs(longBuf, 0, readable);
                                    hole.consume(longBuf[0]);
                                }
                            }
                            case 9 -> {
                                fileInput.seek(offset);
                                long remaining = fileSize - offset;
                                int readable = (int) Math.min(numFloats, remaining / Float.BYTES);
                                if (readable > 0) {
                                    fileInput.readFloats(floatBuf, 0, readable);
                                    hole.consume(floatBuf[0]);
                                }
                            }
                            case 10 -> {
                                long sliceLen = Math.min(StaticConfigs.CACHE_BLOCK_SIZE, fileSize - offset);
                                if (sliceLen > 0) {
                                    try (IndexInput slice = fileInput.slice("bench-slice", offset, sliceLen)) {
                                        slice.readBytes(readBuf, 0, (int) sliceLen);
                                        hole.consume(readBuf[0]);
                                    }
                                }
                            }
                            case 11 -> {
                                fileInput.seek(offset);
                                long skip = Math.min(StaticConfigs.CACHE_BLOCK_SIZE, fileSize - offset);
                                if (skip > 0) {
                                    fileInput.skipBytes(skip);
                                    hole.consume(fileInput.getFilePointer());
                                }
                            }
                        }
                    }
                } finally {
                    if (!directoryType.startsWith("mmap")) {
                        fileInput.close();
                    }
                }
            }
        }, bh);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() throws IOException {
        if (executor != null) {
            // Use shutdown() instead of shutdownNow() to avoid sending interrupts.
            // Lucene's MMapDirectory docs warn: "Accessing this class from a thread
            // while it's interrupted can close the underlying channel immediately",
            // which would invalidate all MemorySegments and break subsequent iterations.
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    executor.awaitTermination(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
    }

    @TearDown(Level.Trial)
    public void tearDownHotPathTrial() throws Exception {
        super.closeInputs();
        super.tearDownTrial();
    }
}
