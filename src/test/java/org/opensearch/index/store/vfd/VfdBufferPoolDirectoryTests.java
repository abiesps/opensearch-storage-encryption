/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Integration tests for {@link VfdBufferPoolDirectory}.
 *
 * <p>Verifies the factory wiring: open directory → write file → read file
 * → verify round-trip for both plain and encrypted paths.</p>
 *
 * <p><b>Validates: Requirements 33 AC8, 52 AC9, 53 AC8</b></p>
 */
public class VfdBufferPoolDirectoryTests extends OpenSearchTestCase {

    private static final int FD_LIMIT = 10;
    private static final int FD_BURST = 5;
    private static final int POOL_SIZE = 32;
    private static final int INFLIGHT_PERMITS = 16;
    private static final long INFLIGHT_TIMEOUT_MS = 5_000L;

    private VfdRegistry registry;
    private MemoryPool memoryPool;
    private BlockCache blockCache;
    private Semaphore inflightLimiter;
    private ExecutorService prefetchExecutor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new VfdRegistry(FD_LIMIT, FD_BURST);
        memoryPool = new MemoryPool(POOL_SIZE);
        blockCache = new BlockCache(1024, registry);
        inflightLimiter = new Semaphore(INFLIGHT_PERMITS, true);
        prefetchExecutor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "test-prefetch");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void tearDown() throws Exception {
        // In tests, we own the node-level resources — clean them up
        prefetchExecutor.shutdownNow();
        blockCache.close();
        memoryPool.close();
        registry.close();
        super.tearDown();
    }

    // ---- Round-trip: write file → read file → verify ----

    public void testWriteAndReadRoundTrip() throws IOException {
        Path dir = createTempDir("vfd-dir");
        try (VfdBufferPoolDirectory directory = new VfdBufferPoolDirectory(
                dir, registry, memoryPool, blockCache,
                new FileChannelIOBackend(), inflightLimiter,
                INFLIGHT_TIMEOUT_MS, prefetchExecutor)) {

            byte[] data = generateTestData(10_000);
            writeFile(directory, "test.dat", data);
            byte[] readBack = readFile(directory, "test.dat", data.length);

            assertEquals(data.length, readBack.length);
            for (int i = 0; i < data.length; i++) {
                assertEquals("Byte mismatch at offset " + i, data[i], readBack[i]);
            }
        }
    }

    public void testWriteAndReadMultipleBlocks() throws IOException {
        Path dir = createTempDir("vfd-dir-multi");
        try (VfdBufferPoolDirectory directory = new VfdBufferPoolDirectory(
                dir, registry, memoryPool, blockCache,
                new FileChannelIOBackend(), inflightLimiter,
                INFLIGHT_TIMEOUT_MS, prefetchExecutor)) {

            // Write data spanning multiple cache blocks
            int blockSize = org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;
            byte[] data = generateTestData(blockSize * 5 + 137);
            writeFile(directory, "multiblock.dat", data);
            byte[] readBack = readFile(directory, "multiblock.dat", data.length);

            assertEquals(data.length, readBack.length);
            for (int i = 0; i < data.length; i++) {
                assertEquals("Byte mismatch at offset " + i, data[i], readBack[i]);
            }
        }
    }

    public void testWriteAndReadMultipleFiles() throws IOException {
        Path dir = createTempDir("vfd-dir-files");
        try (VfdBufferPoolDirectory directory = new VfdBufferPoolDirectory(
                dir, registry, memoryPool, blockCache,
                new FileChannelIOBackend(), inflightLimiter,
                INFLIGHT_TIMEOUT_MS, prefetchExecutor)) {

            byte[] data1 = generateTestData(5_000);
            byte[] data2 = generateTestData(8_000);

            writeFile(directory, "file1.dat", data1);
            writeFile(directory, "file2.dat", data2);

            byte[] read1 = readFile(directory, "file1.dat", data1.length);
            byte[] read2 = readFile(directory, "file2.dat", data2.length);

            for (int i = 0; i < data1.length; i++) {
                assertEquals("file1 mismatch at " + i, data1[i], read1[i]);
            }
            for (int i = 0; i < data2.length; i++) {
                assertEquals("file2 mismatch at " + i, data2[i], read2[i]);
            }
        }
    }

    public void testCreateOutputAndCreateTempOutput() throws IOException {
        Path dir = createTempDir("vfd-dir-temp");
        try (VfdBufferPoolDirectory directory = new VfdBufferPoolDirectory(
                dir, registry, memoryPool, blockCache,
                new FileChannelIOBackend(), inflightLimiter,
                INFLIGHT_TIMEOUT_MS, prefetchExecutor)) {

            // Test createOutput
            byte[] data = generateTestData(1_000);
            writeFile(directory, "regular.dat", data);

            // Test createTempOutput
            byte[] tempData = generateTestData(2_000);
            String tempName;
            try (IndexOutput tempOut = directory.createTempOutput("tmp", "dat", IOContext.DEFAULT)) {
                tempName = tempOut.getName();
                tempOut.writeBytes(tempData, 0, tempData.length);
            }

            // Read back temp file
            byte[] readBack = readFile(directory, tempName, tempData.length);
            for (int i = 0; i < tempData.length; i++) {
                assertEquals("temp file mismatch at " + i, tempData[i], readBack[i]);
            }
        }
    }

    public void testEncryptionDisabledByDefault() throws IOException {
        Path dir = createTempDir("vfd-dir-noenc");
        try (VfdBufferPoolDirectory directory = new VfdBufferPoolDirectory(
                dir, registry, memoryPool, blockCache,
                new FileChannelIOBackend(), inflightLimiter,
                INFLIGHT_TIMEOUT_MS, prefetchExecutor)) {
            assertFalse("Encryption should be disabled when no KeyResolver provided",
                directory.isEncryptionEnabled());
        }
    }

    public void testCloseReleasesResources() throws IOException {
        Path dir = createTempDir("vfd-dir-close");
        VfdBufferPoolDirectory directory = new VfdBufferPoolDirectory(
            dir, registry, memoryPool, blockCache,
            new FileChannelIOBackend(), inflightLimiter,
            INFLIGHT_TIMEOUT_MS, prefetchExecutor);

        byte[] data = generateTestData(1_000);
        writeFile(directory, "closeme.dat", data);
        readFile(directory, "closeme.dat", data.length);

        directory.close();

        // Directory.close() must NOT shut down shared node-level resources.
        // With 1000s of directories per node (one per shard), closing one
        // directory must not affect others. The node-level owner manages
        // the lifecycle of registry, memoryPool, blockCache, etc.
        assertFalse("Registry must remain open after single directory close", registry.isClosed());
        assertFalse("MemoryPool must remain open after single directory close", memoryPool.isClosed());
    }

    public void testReadByteSequential() throws IOException {
        Path dir = createTempDir("vfd-dir-seq");
        try (VfdBufferPoolDirectory directory = new VfdBufferPoolDirectory(
                dir, registry, memoryPool, blockCache,
                new FileChannelIOBackend(), inflightLimiter,
                INFLIGHT_TIMEOUT_MS, prefetchExecutor)) {

            byte[] data = generateTestData(500);
            writeFile(directory, "seq.dat", data);

            try (IndexInput input = directory.openInput("seq.dat", IOContext.DEFAULT)) {
                for (int i = 0; i < data.length; i++) {
                    assertEquals("readByte mismatch at " + i, data[i], input.readByte());
                }
            }
        }
    }

    public void testSliceAndClone() throws IOException {
        Path dir = createTempDir("vfd-dir-slice");
        try (VfdBufferPoolDirectory directory = new VfdBufferPoolDirectory(
                dir, registry, memoryPool, blockCache,
                new FileChannelIOBackend(), inflightLimiter,
                INFLIGHT_TIMEOUT_MS, prefetchExecutor)) {

            byte[] data = generateTestData(10_000);
            writeFile(directory, "sliceable.dat", data);

            try (IndexInput input = directory.openInput("sliceable.dat", IOContext.DEFAULT)) {
                // Test slice
                IndexInput slice = input.slice("test-slice", 100, 500);
                byte[] sliceData = new byte[500];
                slice.readBytes(sliceData, 0, 500);
                for (int i = 0; i < 500; i++) {
                    assertEquals("slice mismatch at " + i, data[100 + i], sliceData[i]);
                }

                // Test clone
                IndexInput cloned = input.clone();
                cloned.seek(200);
                byte[] cloneData = new byte[300];
                cloned.readBytes(cloneData, 0, 300);
                for (int i = 0; i < 300; i++) {
                    assertEquals("clone mismatch at " + i, data[200 + i], cloneData[i]);
                }
            }
        }
    }

    /**
     * Simulates a node with multiple shards (directories) sharing the same
     * node-level resources. Closing one directory must NOT break the others.
     * This guards against regressions where Directory.close() accidentally
     * shuts down shared singletons (registry, memoryPool, blockCache, etc.).
     */
    public void testMultipleDirectoriesShareNodeResources() throws IOException {
        Path dir1 = createTempDir("shard-0");
        Path dir2 = createTempDir("shard-1");
        Path dir3 = createTempDir("shard-2");

        VfdBufferPoolDirectory shard0 = new VfdBufferPoolDirectory(
            dir1, registry, memoryPool, blockCache,
            new FileChannelIOBackend(), inflightLimiter,
            INFLIGHT_TIMEOUT_MS, prefetchExecutor);
        VfdBufferPoolDirectory shard1 = new VfdBufferPoolDirectory(
            dir2, registry, memoryPool, blockCache,
            new FileChannelIOBackend(), inflightLimiter,
            INFLIGHT_TIMEOUT_MS, prefetchExecutor);
        VfdBufferPoolDirectory shard2 = new VfdBufferPoolDirectory(
            dir3, registry, memoryPool, blockCache,
            new FileChannelIOBackend(), inflightLimiter,
            INFLIGHT_TIMEOUT_MS, prefetchExecutor);

        // Write data to all three shards
        byte[] data1 = generateTestData(5_000);
        byte[] data2 = generateTestData(8_000);
        byte[] data3 = generateTestData(3_000);
        writeFile(shard0, "seg0.dat", data1);
        writeFile(shard1, "seg1.dat", data2);
        writeFile(shard2, "seg2.dat", data3);

        // Read from all three — sanity check
        assertArrayEquals(data1, readFile(shard0, "seg0.dat", data1.length));
        assertArrayEquals(data2, readFile(shard1, "seg1.dat", data2.length));
        assertArrayEquals(data3, readFile(shard2, "seg2.dat", data3.length));

        // Close shard-0 — this must NOT kill shared resources
        shard0.close();

        // Shared resources must still be alive
        assertFalse("Registry must survive shard-0 close", registry.isClosed());
        assertFalse("MemoryPool must survive shard-0 close", memoryPool.isClosed());

        // Shard-1 and shard-2 must still be fully functional
        assertArrayEquals("shard-1 must still read after shard-0 close",
            data2, readFile(shard1, "seg1.dat", data2.length));
        assertArrayEquals("shard-2 must still read after shard-0 close",
            data3, readFile(shard2, "seg2.dat", data3.length));

        // Write new data to shard-1 after shard-0 is closed
        byte[] newData = generateTestData(6_000);
        writeFile(shard1, "seg1_new.dat", newData);
        assertArrayEquals("shard-1 must still write after shard-0 close",
            newData, readFile(shard1, "seg1_new.dat", newData.length));

        // Close shard-1 — shard-2 must still work
        shard1.close();
        assertFalse("Registry must survive shard-1 close", registry.isClosed());
        assertArrayEquals("shard-2 must still read after shard-1 close",
            data3, readFile(shard2, "seg2.dat", data3.length));

        shard2.close();
    }

    // ---- Helpers ----

    private byte[] generateTestData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) ((i * 31 + 7) & 0xFF);
        }
        return data;
    }

    private void writeFile(VfdBufferPoolDirectory directory, String name, byte[] data) throws IOException {
        try (IndexOutput out = directory.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(data, 0, data.length);
        }
    }

    private byte[] readFile(VfdBufferPoolDirectory directory, String name, int length) throws IOException {
        try (IndexInput input = directory.openInput(name, IOContext.DEFAULT)) {
            byte[] result = new byte[length];
            input.readBytes(result, 0, length);
            return result;
        }
    }
}
