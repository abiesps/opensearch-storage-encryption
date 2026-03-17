/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.index.store.block_cache_v2.CaffeineBlockCacheV2;
import org.opensearch.index.store.block_cache_v2.DirectBufferPoolDirectory;
import org.opensearch.index.store.block_cache_v2.VirtualFileDescriptorRegistry;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Isolates the per-readByte overhead by comparing:
 *
 * 1. tight_loop: single segment, tight loop of 638K reads (ceiling)
 * 2. block_loop: 129 segments in an array, outer loop switches segment,
 *    inner loop reads 4953 bytes — mimics the full benchmark structure
 *    but with zero cache/table/IndexInput overhead
 * 3. block_loop_dbb: same as #2 but segments from MemorySegment.ofBuffer(DirectByteBuffer)
 *
 * If block_loop matches tight_loop, the overhead is in IndexInput/cache.
 * If block_loop is slower, the overhead is in the loop structure itself.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
@State(Scope.Benchmark)
@Threads(1)
public class ReadByteOverheadBenchmark {

    static final int BLOCK_SIZE = 8192;
    static final int NUM_BLOCKS = 129;
    static final int BYTES_PER_BLOCK = 4953;
    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;

    // Single large segment for tight_loop
    private MemorySegment singleSegment;
    private Arena singleArena;

    // Array of mmap segments for block_loop
    private MemorySegment[] mmapSegments;
    private Arena mmapArena;

    // Array of dbb-backed segments for block_loop_dbb
    private MemorySegment[] dbbSegments;

    // Array of native segments (ofAddress) for block_loop_native
    private MemorySegment[] nativeSegments;
    private ByteBuffer[] keepAliveBuffers; // prevent GC

    private Path tempDir;

    // IndexInput variants for full-stack comparison
    private IndexInput mmapIndexInput;
    private IndexInput dbbIndexInput;
    private MMapDirectory mmapDir;
    private DirectBufferPoolDirectory dbbDir;
    private CaffeineBlockCacheV2 caffeineCache;

    @Setup(Level.Trial)
    @SuppressWarnings("preview")
    public void setup() throws Exception {

        byte[] data = new byte[BLOCK_SIZE];
        new Random(42).nextBytes(data);

        tempDir = Files.createTempDirectory("readbyte-overhead-bench");

        // Single large mmap segment
        Path singleFile = tempDir.resolve("single.dat");
        byte[] bigData = new byte[BLOCK_SIZE * NUM_BLOCKS];
        for (int i = 0; i < NUM_BLOCKS; i++) {
            System.arraycopy(data, 0, bigData, i * BLOCK_SIZE, BLOCK_SIZE);
        }
        Files.write(singleFile, bigData);
        singleArena = Arena.ofShared();
        try (FileChannel fc = FileChannel.open(singleFile, StandardOpenOption.READ)) {
            singleSegment = fc.map(FileChannel.MapMode.READ_ONLY, 0, bigData.length, singleArena);
        }

        // Array of mmap segments (one per block, like MMapDirectory with 8KB chunks)
        mmapArena = Arena.ofShared();
        mmapSegments = new MemorySegment[NUM_BLOCKS];
        Path mmapFile = tempDir.resolve("mmap_blocks.dat");
        Files.write(mmapFile, bigData);
        try (FileChannel fc = FileChannel.open(mmapFile, StandardOpenOption.READ)) {
            for (int i = 0; i < NUM_BLOCKS; i++) {
                mmapSegments[i] = fc.map(FileChannel.MapMode.READ_ONLY,
                        (long) i * BLOCK_SIZE, BLOCK_SIZE, mmapArena);
            }
        }

        // Array of dbb-backed segments (what directbufferpool does)
        dbbSegments = new MemorySegment[NUM_BLOCKS];
        for (int i = 0; i < NUM_BLOCKS; i++) {
            ByteBuffer dbb = ByteBuffer.allocateDirect(BLOCK_SIZE);
            dbb.put(data);
            dbb.flip();
            ByteBuffer ro = dbb.asReadOnlyBuffer();
            dbbSegments[i] = MemorySegment.ofBuffer(ro).reinterpret(BLOCK_SIZE);
        }

        // Array of native segments (ofAddress from DirectByteBuffer)
        nativeSegments = new MemorySegment[NUM_BLOCKS];
        keepAliveBuffers = new ByteBuffer[NUM_BLOCKS];
        for (int i = 0; i < NUM_BLOCKS; i++) {
            ByteBuffer dbb = ByteBuffer.allocateDirect(BLOCK_SIZE);
            dbb.put(data);
            dbb.flip();
            keepAliveBuffers[i] = dbb;
            long addr = MemorySegment.ofBuffer(dbb).address();
            nativeSegments[i] = MemorySegment.ofAddress(addr).reinterpret(BLOCK_SIZE);
        }

        // MMapDirectory IndexInput (full Lucene stack)
        Path mmapDirPath = tempDir.resolve("mmap_dir");
        Files.createDirectories(mmapDirPath);
        mmapDir = new MMapDirectory(mmapDirPath, BLOCK_SIZE);
        mmapDir.setGroupingFunction(MMapDirectory.NO_GROUPING);
        try (IndexOutput out = mmapDir.createOutput("bench.dat", IOContext.DEFAULT)) {
            out.writeBytes(bigData, bigData.length);
        }
        mmapIndexInput = mmapDir.openInput("bench.dat", IOContext.DEFAULT);
        // Warm page cache
        mmapIndexInput.seek(0);
        byte[] warmBuf = new byte[BLOCK_SIZE];
        long rem = mmapIndexInput.length();
        while (rem > 0) { int r = (int) Math.min(warmBuf.length, rem); mmapIndexInput.readBytes(warmBuf, 0, r); rem -= r; }
        mmapIndexInput.seek(0);

        // DirectBufferPool IndexInput (full V2 stack)
        Path dbbDirPath = tempDir.resolve("dbb_dir");
        Files.createDirectories(dbbDirPath);
        long totalBlocks = (bigData.length + BLOCK_SIZE - 1) / BLOCK_SIZE;
        caffeineCache = new CaffeineBlockCacheV2(totalBlocks * 2);
        dbbDir = new DirectBufferPoolDirectory(dbbDirPath, caffeineCache);
        try (IndexOutput out = dbbDir.createOutput("bench.dat", IOContext.DEFAULT)) {
            out.writeBytes(bigData, bigData.length);
        }
        dbbIndexInput = dbbDir.openInput("bench.dat", IOContext.DEFAULT);
        // Warm block cache
        dbbIndexInput.seek(0);
        rem = dbbIndexInput.length();
        while (rem > 0) { int r = (int) Math.min(warmBuf.length, rem); dbbIndexInput.readBytes(warmBuf, 0, r); rem -= r; }
        dbbIndexInput.seek(0);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (singleArena != null) singleArena.close();
        if (mmapArena != null) mmapArena.close();
        if (mmapIndexInput != null) mmapIndexInput.close();
        if (dbbIndexInput != null) dbbIndexInput.close();
        if (mmapDir != null) mmapDir.close();
        if (dbbDir != null) dbbDir.close();
        VirtualFileDescriptorRegistry.getInstance().clear();
        BenchmarkConfig.deleteRecursively(tempDir);
    }

    /** Ceiling: tight loop over single large segment, total 129*4953 = 638937 bytes */
    @Benchmark
    public void tight_loop_mmap(Blackhole bh) {
        int sum = 0;
        int totalBytes = NUM_BLOCKS * BYTES_PER_BLOCK;
        for (int i = 0; i < totalBytes; i++) {
            sum += singleSegment.get(LAYOUT_BYTE, i);
        }
        bh.consume(sum);
    }

    /** Block loop with mmap segments — mimics MMapDirectory MultiSegmentImpl pattern */
    @Benchmark
    public void block_loop_mmap(Blackhole bh) {
        int sum = 0;
        for (int block = 0; block < NUM_BLOCKS; block++) {
            MemorySegment seg = mmapSegments[block];
            for (int i = 1; i < BYTES_PER_BLOCK + 1; i++) { // offset 1 like the benchmark
                sum += seg.get(LAYOUT_BYTE, i);
            }
        }
        bh.consume(sum);
    }

    /** Block loop with dbb-backed segments — mimics directbufferpool pattern */
    @Benchmark
    public void block_loop_dbb(Blackhole bh) {
        int sum = 0;
        for (int block = 0; block < NUM_BLOCKS; block++) {
            MemorySegment seg = dbbSegments[block];
            for (int i = 1; i < BYTES_PER_BLOCK + 1; i++) {
                sum += seg.get(LAYOUT_BYTE, i);
            }
        }
        bh.consume(sum);
    }

    /** Block loop with native segments (ofAddress) */
    @Benchmark
    public void block_loop_native(Blackhole bh) {
        int sum = 0;
        for (int block = 0; block < NUM_BLOCKS; block++) {
            MemorySegment seg = nativeSegments[block];
            for (int i = 1; i < BYTES_PER_BLOCK + 1; i++) {
                sum += seg.get(LAYOUT_BYTE, i);
            }
        }
        bh.consume(sum);
    }

    /** Full Lucene MMapDirectory IndexInput — clone + seek + readByte loop, same structure as HotPathReadBenchmarks */
    @Benchmark
    public void indexinput_mmap(Blackhole bh) throws Exception {
        IndexInput clone = mmapIndexInput.clone();
        int sum = 0;
        for (int iter = 0; iter < NUM_BLOCKS; iter++) {
            clone.seek(1);
            for (int i = 0; i < BYTES_PER_BLOCK; i++) {
                sum += clone.readByte();
            }
        }
        bh.consume(sum);
        // Don't close mmap clones — same as HotPathReadBenchmarks
    }

    /** Full DirectByteBufferIndexInput — clone + seek + readByte loop, same structure as HotPathReadBenchmarks */
    @Benchmark
    public void indexinput_dbb(Blackhole bh) throws Exception {
        IndexInput clone = dbbIndexInput.clone();
        try {
            int sum = 0;
            for (int iter = 0; iter < NUM_BLOCKS; iter++) {
                clone.seek(1);
                for (int i = 0; i < BYTES_PER_BLOCK; i++) {
                    sum += clone.readByte();
                }
            }
            bh.consume(sum);
        } finally {
            clone.close();
        }
    }
}
