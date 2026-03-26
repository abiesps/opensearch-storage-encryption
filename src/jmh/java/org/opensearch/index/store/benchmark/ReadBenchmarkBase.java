/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import static org.opensearch.index.store.CryptoDirectoryFactory.DEFAULT_CRYPTO_PROVIDER;

import org.opensearch.index.store.bufferpoolfs.StaticConfigs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.block_loader.DirectByteBufferBlockLoader;
import org.opensearch.index.store.block_cache_v2.CaffeineBlockCacheV2;
import org.opensearch.index.store.block_cache_v2.DirectBufferPoolDirectory;
import org.opensearch.index.store.block_cache_v2.DirectBufferPoolV05Directory;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectoryV0;
import org.opensearch.index.store.bufferpoolfs.NoOpRadixBlockTable;
import org.opensearch.index.store.bufferpoolfs.NoOpSparseLongBlockTable;
import org.opensearch.index.store.bufferpoolfs.TestKeyResolver;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.EncryptionMetadataCacheRegistry;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.pool.PoolBuilder;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;

/**
 * Base state for read benchmarks. Sets up both BufferPool (encrypted) and MMap directories
 * with a shared test file, and provides range-generation utilities.
 *
 * <p>JMH parameters:
 * <ul>
 *   <li>{@code directoryType}: "bufferpool" or "mmap"</li>
 *   <li>{@code fileSizeMB}: size of the test file in MB</li>
 * </ul>
 */
@State(Scope.Benchmark)
public class ReadBenchmarkBase {

    @Param({ "directbufferpool", "mmap" })
    public String directoryType;

    @Param({ "1" })
    public int fileSizeMB;

    @Param({ "1" })
    public int numFilesToRead;

    //@Param({ "8", "32","128", "1024" })
    @Param({ "8" })
    public int cacheBlockSizeKB;

    /**
     * When "true", disables the L1 block table (SparseLongBlockTable) for
     * bufferpool and directbufferpool directory types. This forces every
     * block read through the L2 Caffeine cache, allowing measurement of
     * L1 cache overhead. Has no effect on mmap/mmap_single directory types.
     */
    @Param({ "false" })
    public String disableL1Cache;

    public int sequentialReadNumBytes;

    // ---- internal state ----
    protected Path tempDir;
    protected Path bufferPoolPath;
    protected Path mmapPath;

    protected BufferPoolDirectory bufferPoolDirectory;
    protected BufferPoolDirectoryV0 bufferPoolDirectoryV0;
    protected MMapDirectory mmapDirectory;
    protected MMapDirectory mmapSingleDirectory;
    protected DirectBufferPoolDirectory directBufferPoolDirectory;
    protected DirectBufferPoolV05Directory directBufferPoolV05Directory;
    protected CaffeineBlockCacheV2 caffeineBlockCacheV2;
    protected CaffeineBlockCacheV2 caffeineBlockCacheV2_05;
    protected PoolBuilder.PoolResources poolResources;
    protected PoolBuilder.PoolResources poolResourcesV0;

    protected static final String FILE_NAME_PREFIX = "bench_data";
    protected int fileSize;
    protected byte[] fileData;
    protected IndexInput[] indexInputs;

    protected String[] fileNames;
    private Random random;
    protected long[] blockStartOffsets;
    protected long[] randomReadByteOffsets;
    private int numBlocks;

    /**
     * Initializes directories, writes test files, and generates ranges.
     * Subclasses must call this from their own {@code @Setup(Level.Trial)} method.
     */
    public void setupTrial() throws Exception {
        // Override cache block size before any directory/pool creation
        int power = switch (cacheBlockSizeKB) {
            case 1 -> 10;    // 1KB
            case 2 -> 11;    // 2KB
            case 4 -> 12;    // 4KB
            case 8 -> 13;    // 8KB
            case 16 -> 14;   // 16KB
            case 32 -> 15;   // 32KB
            case 64 -> 16;   // 64KB
            case 128 -> 17;  // 128KB
            case 256 -> 18;  // 256KB
            case 512 -> 19;  // 512KB
            case 1024 -> 20; // 1MB
            default -> throw new IllegalArgumentException("cacheBlockSizeKB must be a power of 2 between 1 and 1024, got: " + cacheBlockSizeKB);
        };
        StaticConfigs.overrideCacheBlockSize(power);

        // Initialize metrics service with no-op registry (normally done during node startup)
        CryptoMetricsService.initialize(NoopMetricsRegistry.INSTANCE);

        final int blockSize = StaticConfigs.CACHE_BLOCK_SIZE;
        fileSize = fileSizeMB * 1024 * 1024 + 3; // Always add a partial block
        numBlocks = (fileSize + blockSize - 1) / blockSize;
        fileData = BenchmarkConfig.buildDeterministicPattern(fileSize);
        random = new Random(BenchmarkConfig.RANGE_SEED);
        sequentialReadNumBytes = random.nextInt(0, blockSize - 3);

        // Allocate arrays now that numFilesToRead and numBlocks are known
        fileNames = new String[numFilesToRead];
        indexInputs = new IndexInput[numFilesToRead];
        blockStartOffsets = new long[numBlocks];

        tempDir = Files.createTempDirectory(Path.of(System.getProperty("java.io.tmpdir")), "jmh-read-bench");
        bufferPoolPath = tempDir.resolve("bufferpool");
        mmapPath = tempDir.resolve("mmap");
        Path mmapSinglePath = tempDir.resolve("mmap_single");
        Path directBufferPoolPath = tempDir.resolve("directbufferpool");
        Path directBufferPoolV05Path = tempDir.resolve("directbufferpoolv05");
        Path bufferPoolV0Path = tempDir.resolve("bufferpoolv0");
        Files.createDirectories(bufferPoolPath);
        Files.createDirectories(mmapPath);
        Files.createDirectories(mmapSinglePath);
        Files.createDirectories(directBufferPoolPath);
        Files.createDirectories(directBufferPoolV05Path);
        Files.createDirectories(bufferPoolV0Path);

        // Disable write-through cache so cold path tests are meaningful
        // Only needed for encrypted directory types
        if ("bufferpool".equals(directoryType) || "bufferpoolv0".equals(directoryType)) {
            CryptoDirectoryFactory.setNodeSettings(Settings.builder().put("node.store.crypto.write_cache_enabled", false).build());
        }
        setBlockStartOffset();
        setRandomReadByteOffsets();
        generateFileNamesToRead();
        // Only set up directories that are needed for the current directoryType
        // to avoid native crypto failures when running from jmhJar
        if ("bufferpool".equals(directoryType)) {
            setupBufferPoolDirectory();
        }
        if ("bufferpoolv0".equals(directoryType)) {
            setupBufferPoolV0Directory(bufferPoolV0Path);
        }
        if ("mmap".equals(directoryType)) {
            setupMMapDirectory();
        }
        if ("mmap_single".equals(directoryType)) {
            setupMMapSingleDirectory(mmapSinglePath);
        }
        if ("directbufferpool".equals(directoryType)) {
            setupDirectBufferPoolDirectory(directBufferPoolPath);
        }
        if ("directbufferpoolv05".equals(directoryType)) {
            setupDirectBufferPoolV05Directory(directBufferPoolV05Path);
        }

        // Release file data after both directories are set up — no longer needed
        fileData = null;
    }

    private void setupBufferPoolDirectory() throws Exception {
        Provider provider = Security.getProvider(DEFAULT_CRYPTO_PROVIDER);
        MasterKeyProvider keyProvider = BenchmarkKeyProvider.create();

        Settings nodeSettings = Settings
            .builder()
            .put("plugins.crypto.enabled", true)
            .put("node.store.crypto.pool_size_percentage", 0.10)
            .put("node.store.crypto.warmup_percentage", 0.0)
            .put("node.store.crypto.cache_to_pool_ratio", 0.8)
            .build();

        this.poolResources = PoolBuilder.build(nodeSettings);
        Pool<RefCountedMemorySegment> segmentPool = poolResources.getSegmentPool();

        String indexUuid = "bench-idx-" + System.nanoTime();
        String indexName = "bench-index";
        FSDirectory fsDir = FSDirectory.open(bufferPoolPath);
        int shardId = 0;

        KeyResolver keyResolver = new TestKeyResolver(indexUuid, indexName, fsDir, provider, keyProvider, shardId);

        EncryptionMetadataCache encMetaCache = EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, shardId, indexName);

        BlockLoader<RefCountedMemorySegment> loader = new DirectByteBufferBlockLoader(keyResolver, encMetaCache);
        Worker worker = poolResources.getSharedReadaheadWorker();

        @SuppressWarnings("unchecked")
        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> sharedCache =
            (CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment>) poolResources.getBlockCache();

        BlockCache<RefCountedMemorySegment> directoryCache = new CaffeineBlockCache<>(
            sharedCache.getCache(),
            loader,
            poolResources.getMaxCacheBlocks()
        );

        this.bufferPoolDirectory = new BufferPoolDirectory(
            bufferPoolPath,
            FSLockFactory.getDefault(),
            provider,
            keyResolver,
            segmentPool,
            directoryCache,
            loader,
            worker,
            encMetaCache
        );

        // Write test files through bufferpool write path
        // ToDo Allow disabling encryption and pure use of bufferpool directory.
        for (String fileName : fileNames) {
            try (IndexOutput out = bufferPoolDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                out.writeBytes(fileData, fileData.length);
            }
        }

        // Only open inputs if this is the active directory type
        if ("bufferpool".equals(directoryType)) {
            if ("true".equalsIgnoreCase(disableL1Cache)) {
                bufferPoolDirectory.setBlockTableOverride(NoOpSparseLongBlockTable.INSTANCE);
            }
            for (int i = 0; i < fileNames.length; i++) {
                indexInputs[i] = bufferPoolDirectory.openInput(fileNames[i], IOContext.DEFAULT);
            }
        }
    }

    private void setupBufferPoolV0Directory(Path v0Path) throws Exception {
        Provider provider = Security.getProvider(DEFAULT_CRYPTO_PROVIDER);
        MasterKeyProvider keyProvider = BenchmarkKeyProvider.create();

        Settings nodeSettings = Settings
            .builder()
            .put("plugins.crypto.enabled", true)
            .put("node.store.crypto.pool_size_percentage", 0.10)
            .put("node.store.crypto.warmup_percentage", 0.0)
            .put("node.store.crypto.cache_to_pool_ratio", 0.8)
            .build();

        this.poolResourcesV0 = PoolBuilder.build(nodeSettings);
        Pool<RefCountedMemorySegment> segmentPoolV0 = poolResourcesV0.getSegmentPool();

        String indexUuid = "bench-idx-v0-" + System.nanoTime();
        String indexName = "bench-index-v0";
        FSDirectory fsDir = FSDirectory.open(v0Path);
        int shardId = 0;

        KeyResolver keyResolver = new TestKeyResolver(indexUuid, indexName, fsDir, provider, keyProvider, shardId);
        EncryptionMetadataCache encMetaCache = EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, shardId, indexName);

        BlockLoader<RefCountedMemorySegment> loader = new CryptoDirectIOBlockLoader(segmentPoolV0, keyResolver, encMetaCache);
        Worker worker = poolResourcesV0.getSharedReadaheadWorker();

        @SuppressWarnings("unchecked")
        CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> sharedCache =
            (CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment>) poolResourcesV0.getBlockCache();

        BlockCache<RefCountedMemorySegment> directoryCache = new CaffeineBlockCache<>(
            sharedCache.getCache(),
            loader,
            poolResourcesV0.getMaxCacheBlocks()
        );

        this.bufferPoolDirectoryV0 = new BufferPoolDirectoryV0(
            v0Path,
            FSLockFactory.getDefault(),
            provider,
            keyResolver,
            segmentPoolV0,
            directoryCache,
            loader,
            worker,
            encMetaCache
        );

        // Write test files through V0 write path (same encryption as bufferpool)
        for (String fileName : fileNames) {
            try (IndexOutput out = bufferPoolDirectoryV0.createOutput(fileName, IOContext.DEFAULT)) {
                out.writeBytes(fileData, fileData.length);
            }
        }

        // Only open inputs if this is the active directory type
        if ("bufferpoolv0".equals(directoryType)) {
            if ("true".equalsIgnoreCase(disableL1Cache)) {
                bufferPoolDirectoryV0.setDisableL1Cache(true);
            }
            for (int i = 0; i < fileNames.length; i++) {
                indexInputs[i] = bufferPoolDirectoryV0.openInput(fileNames[i], IOContext.DEFAULT);
            }
        }
    }

    private void setupMMapDirectory() throws Exception {
        // Use the same chunk size as the block cache for apples-to-apples comparison.
        // MMapDirectory constructor takes maxChunkSize in bytes; cacheBlockSizeKB is in KB.
        this.mmapDirectory = new MMapDirectory(mmapPath, cacheBlockSizeKB * 1024);
        // Disable shared arena grouping so each openInput gets its own arena.
        // This prevents closing one IndexInput from invalidating another's MemorySegments.
        mmapDirectory.setGroupingFunction(MMapDirectory.NO_GROUPING);
        // Write identical plaintext data for MMap (no encryption)
        for (String fileName : fileNames) {
            try (IndexOutput out = mmapDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                out.writeBytes(fileData, fileData.length);
            }
        }

        // Only open inputs if this is the active directory type
        if ("mmap".equals(directoryType)) {
            for (int i = 0; i < fileNames.length; i++) {
                indexInputs[i] = mmapDirectory.openInput(fileNames[i], IOContext.DEFAULT);
            }
        }
    }

    private void setupMMapSingleDirectory(Path dirPath) throws Exception {
        // Default MMapDirectory — uses Lucene's default chunk size (typically Integer.MAX_VALUE),
        // producing a single MemorySegment per file (SingleSegmentImpl) instead of MultiSegmentImpl.
        this.mmapSingleDirectory = new MMapDirectory(dirPath);
        mmapSingleDirectory.setGroupingFunction(MMapDirectory.NO_GROUPING);
        for (String fileName : fileNames) {
            try (IndexOutput out = mmapSingleDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                out.writeBytes(fileData, fileData.length);
            }
        }

        if ("mmap_single".equals(directoryType)) {
            for (int i = 0; i < fileNames.length; i++) {
                indexInputs[i] = mmapSingleDirectory.openInput(fileNames[i], IOContext.DEFAULT);
            }
        }
    }

    private void setupDirectBufferPoolDirectory(Path dirPath) throws Exception {
        // Size cache to hold all blocks for the benchmark files with some headroom
        long totalBlocks = ((long) fileSize * numFilesToRead + StaticConfigs.CACHE_BLOCK_SIZE - 1) / StaticConfigs.CACHE_BLOCK_SIZE;
        long maxCacheBlocks = totalBlocks * 2; // 2x headroom
        this.caffeineBlockCacheV2 = new CaffeineBlockCacheV2(maxCacheBlocks);
        this.directBufferPoolDirectory = new DirectBufferPoolDirectory(dirPath, caffeineBlockCacheV2);

        // Write plaintext data (no encryption — apples-to-apples with mmap)
        for (String fileName : fileNames) {
            try (IndexOutput out = directBufferPoolDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                out.writeBytes(fileData, fileData.length);
            }
        }

        // Only open inputs if this is the active directory type
        if ("directbufferpool".equals(directoryType)) {
            if ("true".equalsIgnoreCase(disableL1Cache)) {
                directBufferPoolDirectory.setBlockTableOverride(NoOpRadixBlockTable.INSTANCE);
            }
            for (int i = 0; i < fileNames.length; i++) {
                indexInputs[i] = directBufferPoolDirectory.openInput(fileNames[i], IOContext.DEFAULT);
            }
        }
    }

    private void setupDirectBufferPoolV05Directory(Path dirPath) throws Exception {
        long totalBlocks = ((long) fileSize * numFilesToRead + StaticConfigs.CACHE_BLOCK_SIZE - 1) / StaticConfigs.CACHE_BLOCK_SIZE;
        long maxCacheBlocks = totalBlocks * 2;
        this.caffeineBlockCacheV2_05 = new CaffeineBlockCacheV2(maxCacheBlocks);
        this.directBufferPoolV05Directory = new DirectBufferPoolV05Directory(dirPath, caffeineBlockCacheV2_05);

        for (String fileName : fileNames) {
            try (IndexOutput out = directBufferPoolV05Directory.createOutput(fileName, IOContext.DEFAULT)) {
                out.writeBytes(fileData, fileData.length);
            }
        }

        if ("directbufferpoolv05".equals(directoryType)) {
            if ("true".equalsIgnoreCase(disableL1Cache)) {
                directBufferPoolV05Directory.setBlockTableOverride(NoOpRadixBlockTable.INSTANCE);
            }
            for (int i = 0; i < fileNames.length; i++) {
                indexInputs[i] = directBufferPoolV05Directory.openInput(fileNames[i], IOContext.DEFAULT);
            }
        }
    }

    private void generateFileNamesToRead() {
        for (int i = 0; i < numFilesToRead; i++) {
            String fileName = FILE_NAME_PREFIX + i;
            fileNames[i] = fileName;
        }
    }

    public void tearDownTrial() throws Exception {
        if (bufferPoolDirectory != null)
            bufferPoolDirectory.close();
        if (bufferPoolDirectoryV0 != null)
            bufferPoolDirectoryV0.close();
        if (mmapDirectory != null)
            mmapDirectory.close();
        if (mmapSingleDirectory != null)
            mmapSingleDirectory.close();
        if (directBufferPoolDirectory != null)
            directBufferPoolDirectory.close();
        if (directBufferPoolV05Directory != null)
            directBufferPoolV05Directory.close();
        if (poolResources != null)
            poolResources.close();
        if (poolResourcesV0 != null)
            poolResourcesV0.close();
        BenchmarkConfig.deleteRecursively(tempDir);
    }

    /** Returns a random active IndexInput for the current directory type (thread-safe). */
    protected IndexInput input() {
        return indexInputs[ThreadLocalRandom.current().nextInt(0, numFilesToRead)];
    }

    protected void closeInputs() throws IOException {
        for (IndexInput indexInput : indexInputs) {
            indexInput.close();
        }
    }

    protected void setBlockStartOffset() {
        final int blockSize = StaticConfigs.CACHE_BLOCK_SIZE;
        for (int block = 0; block < numBlocks; block++) {
            blockStartOffsets[block] = (long) block * blockSize;
        }
    }

    protected void setRandomReadByteOffsets() {
        final int blockSize = StaticConfigs.CACHE_BLOCK_SIZE;
        this.randomReadByteOffsets = new long[numBlocks];
        for (int i = 0; i < numBlocks; i++) {
            int blockBytes = (i == numBlocks - 1) ? Math.max(1, fileSize - (int) blockStartOffsets[i]) : blockSize;
            // Reserve 128 bytes headroom so multi-byte reads (readLong etc.) stay in-block.
            // For tiny last blocks, just use offset 0 within the block.
            int usable = Math.max(1, blockBytes - 128);
            randomReadByteOffsets[i] = blockStartOffsets[i] + random.nextInt(usable);
        }
    }

    protected long[] randomReadByteOffsets() {
        return randomReadByteOffsets;
    }

}
