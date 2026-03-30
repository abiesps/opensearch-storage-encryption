/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures prefetch effectiveness: how quickly does data become
 * available after calling {@code IndexInput.prefetch()}?
 *
 * <h2>Method</h2>
 * <ol>
 *   <li>Write a large file via O_DIRECT (bypasses page cache)</li>
 *   <li>Open with MMap directory</li>
 *   <li>Each iteration picks a unique block offset (guaranteed cold)</li>
 *   <li>Call prefetch(), wait {@code delayUs} microseconds, then readByte()</li>
 *   <li>Measure readByte latency — if prefetch worked, it's a cache hit</li>
 * </ol>
 *
 * <h2>Key metric</h2>
 * {@code readLatencyNs} at each delay. When it drops from cold-IO latency
 * (~ms on EFS, ~100μs on SSD) to near-zero, the prefetch has completed.
 * The delay at which this transition happens is the effective prefetch
 * completion time.
 *
 * <h2>Run</h2>
 * <pre>
 *   # Local SSD (fine-grained delays)
 *   java --enable-preview --enable-native-access=ALL-UNNAMED \
 *     -jar storage-encryption-jmh.jar PrefetchLatencyBenchmark \
 *     -f 1 -i 50 -bm ss -tu us \
 *     -p delayUs=0,10,50,100,200,500,1000,5000
 *
 *   # EFS (coarser delays)
 *   java --enable-preview --enable-native-access=ALL-UNNAMED \
 *     -jar storage-encryption-jmh.jar PrefetchLatencyBenchmark \
 *     -f 1 -i 50 -bm ss -tu us \
 *     -p delayUs=0,500,1000,2000,5000,10000,20000,50000
 * </pre>
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {
    "--enable-native-access=ALL-UNNAMED",
    "--enable-preview"
})
public class PrefetchLatencyBenchmark {

    // ======================== O_DIRECT file writer via Panama FFI ========================

    private static final Linker LINKER = Linker.nativeLinker();
    private static final MethodHandle OPEN_MH;
    private static final MethodHandle PWRITE_MH;
    private static final MethodHandle CLOSE_MH;

    static {
        SymbolLookup stdlib = LINKER.defaultLookup();
        OPEN_MH = LINKER.downcallHandle(
            stdlib.find("open").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
        PWRITE_MH = LINKER.downcallHandle(
            stdlib.find("pwrite").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
        CLOSE_MH = LINKER.downcallHandle(
            stdlib.find("close").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
    }

    // Linux O_DIRECT | O_WRONLY | O_CREAT | O_TRUNC
    private static final int O_WRONLY = 1;
    private static final int O_CREAT = 64;
    private static final int O_TRUNC = 512;
    private static final int O_DIRECT = 0x4000; // Linux only

    private static final int BLOCK_SIZE = 8192;
    private static final int FILE_SIZE_MB = 128; // 128MB → 16384 blocks
    private static final int TOTAL_BLOCKS = (FILE_SIZE_MB * 1024 * 1024) / BLOCK_SIZE;

    /** Delay in microseconds between prefetch() and readByte(). */
    @Param({ "0", "100", "1000", "5000" })
    int delayUs;

    private Path tempDir;
    private Path testFile;
    private Directory mmapDirectory;
    private IndexInput indexInput;

    /** Monotonically increasing offset cursor — each iteration gets a unique cold block. */
    private final AtomicInteger blockCursor = new AtomicInteger(0);

    @Setup(Level.Trial)
    public void setup() throws Exception {
        tempDir = Files.createTempDirectory("prefetch-bench");
        testFile = tempDir.resolve("cold_data.bin");

        // Write file via O_DIRECT to bypass page cache
        writeFileWithODirect(testFile, FILE_SIZE_MB);

        // Open with MMap
        mmapDirectory = new MMapDirectory(tempDir);
        indexInput = mmapDirectory.openInput("cold_data.bin", IOContext.DEFAULT);

        System.out.println("File: " + testFile + " (" + FILE_SIZE_MB + " MB, "
            + TOTAL_BLOCKS + " blocks of " + BLOCK_SIZE + " bytes)");
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (indexInput != null) indexInput.close();
        if (mmapDirectory != null) mmapDirectory.close();
        if (tempDir != null) {
            try (var walk = Files.walk(tempDir)) {
                walk.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> { try { Files.delete(p); } catch (IOException e) { /* ignore */ } });
            }
        }
    }

    /**
     * Prefetch a cold block, wait delayUs, then read first byte.
     * The readByte latency reveals whether the prefetch completed in time.
     */
    @Benchmark
    public void prefetchThenRead(Blackhole bh) throws Exception {
        int blockIdx = blockCursor.getAndIncrement();
        if (blockIdx >= TOTAL_BLOCKS) {
            // Wrap around — pages from early iterations may have been evicted
            // by OS memory pressure, but this is best-effort
            blockCursor.set(0);
            blockIdx = 0;
        }

        long offset = (long) blockIdx * BLOCK_SIZE;

        // Call prefetch
        indexInput.prefetch(offset, BLOCK_SIZE);

        // Precise delay
        if (delayUs > 0) {
            long deadlineNs = System.nanoTime() + (long) delayUs * 1000L;
            while (System.nanoTime() < deadlineNs) {
                Thread.onSpinWait();
            }
        }

        // Measure read latency (this is what JMH reports as the SingleShotTime)
        indexInput.seek(offset);
        byte b = indexInput.readByte();
        bh.consume(b);
    }

    /**
     * Control: read a cold block WITHOUT prefetch.
     * This gives the baseline cold-IO latency for comparison.
     */
    @Benchmark
    public void coldReadNoPrefetch(Blackhole bh) throws Exception {
        int blockIdx = blockCursor.getAndIncrement();
        if (blockIdx >= TOTAL_BLOCKS) {
            blockCursor.set(0);
            blockIdx = 0;
        }

        long offset = (long) blockIdx * BLOCK_SIZE;

        // No prefetch — direct cold read
        // Delay still applied for fair comparison of measurement overhead
        if (delayUs > 0) {
            long deadlineNs = System.nanoTime() + (long) delayUs * 1000L;
            while (System.nanoTime() < deadlineNs) {
                Thread.onSpinWait();
            }
        }

        indexInput.seek(offset);
        byte b = indexInput.readByte();
        bh.consume(b);
    }

    // ======================== O_DIRECT file writer ========================

    /**
     * Writes a file using O_DIRECT (Linux) or F_NOCACHE (macOS) to ensure
     * the data goes to disk without populating the page cache.
     * Each block is filled with its block index for verification.
     */
    private static void writeFileWithODirect(Path path, int sizeMB) throws Exception {
        int totalBlocks = (sizeMB * 1024 * 1024) / BLOCK_SIZE;
        String osName = System.getProperty("os.name", "").toLowerCase();

        if (osName.contains("linux")) {
            writeWithODirectLinux(path, totalBlocks);
        } else {
            // macOS / other: write normally, then use fcntl F_NOCACHE
            // or just accept that the file may be in page cache
            // (on macOS, use a file larger than available RAM, or purge manually)
            writeWithJavaIO(path, totalBlocks);
            System.out.println("WARNING: O_DIRECT not available on " + osName
                + ". File may be in page cache. Use 'sudo purge' on macOS before running.");
        }
    }

    private static void writeWithODirectLinux(Path path, int totalBlocks) throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cPath = arena.allocateUtf8String(path.toString());
            try {
                int fd = (int) OPEN_MH.invokeExact(cPath,
                    O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0644);
                if (fd == -1) {
                    throw new IOException("open(O_DIRECT) failed for: " + path);
                }
                try {
                    // O_DIRECT requires aligned buffer
                    MemorySegment buf = arena.allocate(BLOCK_SIZE, 4096); // 4K aligned
                    for (int i = 0; i < totalBlocks; i++) {
                        // Fill with block index pattern
                        for (int j = 0; j < BLOCK_SIZE; j += 4) {
                            buf.set(ValueLayout.JAVA_INT_UNALIGNED, j, i);
                        }
                        long offset = (long) i * BLOCK_SIZE;
                        long written = (long) PWRITE_MH.invokeExact(fd, buf, (long) BLOCK_SIZE, offset);
                        if (written != BLOCK_SIZE) {
                            throw new IOException("pwrite short write: " + written + " at offset " + offset);
                        }
                    }
                } finally {
                    CLOSE_MH.invokeExact(fd);
                }
            } catch (Throwable t) {
                if (t instanceof IOException ioe) throw ioe;
                throw new IOException("O_DIRECT write failed", t);
            }
        }
        System.out.println("Wrote " + totalBlocks + " blocks via O_DIRECT to " + path);
    }

    private static void writeWithJavaIO(Path path, int totalBlocks) throws IOException {
        byte[] block = new byte[BLOCK_SIZE];
        try (var out = Files.newOutputStream(path)) {
            for (int i = 0; i < totalBlocks; i++) {
                // Fill with block index pattern
                for (int j = 0; j < BLOCK_SIZE && j + 3 < BLOCK_SIZE; j += 4) {
                    block[j]     = (byte) (i >> 24);
                    block[j + 1] = (byte) (i >> 16);
                    block[j + 2] = (byte) (i >> 8);
                    block[j + 3] = (byte) i;
                }
                out.write(block);
            }
        }
        System.out.println("Wrote " + totalBlocks + " blocks via Java IO to " + path
            + " (page cache may be warm — run 'sudo purge' on macOS)");
    }
}
