/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.MemorySegmentDecryptor;
import org.opensearch.index.store.footer.EncryptionFooter;
import org.opensearch.index.store.footer.EncryptionMetadataTrailer;
import org.opensearch.index.store.key.KeyResolver;

/**
 * Factory {@link FSDirectory} that wires together all VFD layer components:
 * {@link VfdRegistry}, {@link MemoryPool}, {@link BlockCache},
 * {@link VirtualFileChannel}, and {@link MemorySegmentPoolIndexInput}.
 *
 * <p>When encryption is enabled (a {@link KeyResolver} is provided),
 * the directory builds a {@link BlockDecryptor} lambda that decrypts
 * blocks in-place after physical I/O but before L1/L2 caching. This
 * ensures cached blocks are always plaintext. Write path uses
 * {@link EncryptedIndexOutput}. The plain classes have zero dependency
 * on encryption classes — the factory wires decryption at construction.</p>
 *
 * <p><b>Satisfies: Requirements 33 AC8, 52 AC9, 53 AC8</b></p>
 *
 * @opensearch.internal
 */
public class VfdBufferPoolDirectory extends FSDirectory {

    private final AtomicLong nextTempFileCounter = new AtomicLong();

    private final VfdRegistry registry;
    private final MemoryPool memoryPool;
    private final BlockCache blockCache;
    private final IOBackend ioBackend;
    private final Semaphore inflightLimiter;
    private final ExecutorService prefetchExecutor;
    private final long inflightTimeoutMs;
    private final Path dirPath;

    // Encryption support (null when encryption is disabled)
    private final KeyResolver keyResolver;
    private final EncryptionMetadataCache encryptionMetadataCache;

    private final boolean encryptionEnabled;

    /**
     * Creates a new VfdBufferPoolDirectory without encryption.
     *
     * @param path             the directory path
     * @param registry         the VFD registry
     * @param memoryPool       the buffer pool
     * @param blockCache       the L2 block cache
     * @param ioBackend        the I/O backend for physical reads
     * @param inflightLimiter  semaphore limiting concurrent physical I/O
     * @param inflightTimeoutMs timeout in ms for inflight waits
     * @param prefetchExecutor executor for async prefetch (may be null)
     * @throws IOException if the directory cannot be created or accessed
     */
    public VfdBufferPoolDirectory(
            Path path,
            VfdRegistry registry,
            MemoryPool memoryPool,
            BlockCache blockCache,
            IOBackend ioBackend,
            Semaphore inflightLimiter,
            long inflightTimeoutMs,
            ExecutorService prefetchExecutor) throws IOException {
        this(path, FSLockFactory.getDefault(), registry, memoryPool, blockCache,
             ioBackend, inflightLimiter, inflightTimeoutMs, prefetchExecutor,
             null, null);
    }

    /**
     * Creates a new VfdBufferPoolDirectory with optional encryption.
     *
     * @param path                    the directory path
     * @param lockFactory             the lock factory
     * @param registry                the VFD registry
     * @param memoryPool              the buffer pool
     * @param blockCache              the L2 block cache
     * @param ioBackend               the I/O backend for physical reads
     * @param inflightLimiter         semaphore limiting concurrent physical I/O
     * @param inflightTimeoutMs       timeout in ms for inflight waits
     * @param prefetchExecutor        executor for async prefetch (may be null)
     * @param keyResolver             key resolver for encryption (null to disable)
     * @param encryptionMetadataCache encryption metadata cache (null to disable)
     * @throws IOException if the directory cannot be created or accessed
     */
    public VfdBufferPoolDirectory(
            Path path,
            LockFactory lockFactory,
            VfdRegistry registry,
            MemoryPool memoryPool,
            BlockCache blockCache,
            IOBackend ioBackend,
            Semaphore inflightLimiter,
            long inflightTimeoutMs,
            ExecutorService prefetchExecutor,
            KeyResolver keyResolver,
            EncryptionMetadataCache encryptionMetadataCache) throws IOException {
        super(path, lockFactory);
        this.registry = registry;
        this.memoryPool = memoryPool;
        this.blockCache = blockCache;
        this.ioBackend = ioBackend;
        this.inflightLimiter = inflightLimiter;
        this.inflightTimeoutMs = inflightTimeoutMs;
        this.prefetchExecutor = prefetchExecutor;
        this.dirPath = getDirectory();
        this.keyResolver = keyResolver;
        this.encryptionMetadataCache = encryptionMetadataCache;
        this.encryptionEnabled = (keyResolver != null && encryptionMetadataCache != null);

        // Wire BlockCache into VfdRegistry for eager L2 eviction on VFD close
        registry.setBlockCache(blockCache);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);

        Path file = dirPath.resolve(name);
        long fileSize = Files.size(file);
        if (fileSize == 0) {
            throw new IOException("Cannot open empty file: " + file);
        }

        // Acquire VFD from registry (creates or reuses)
        VirtualFileDescriptor vfd = registry.acquire(file);

        // Build BlockDecryptor: decrypt-before-cache for encrypted files, no-op otherwise
        BlockDecryptor decryptor = BlockDecryptor.NOOP;
        if (encryptionEnabled) {
            decryptor = buildDecryptor(vfd);
        }

        // Create VirtualFileChannel with decryptor wired in
        VirtualFileChannel channel = new VirtualFileChannel(
            vfd, ioBackend, memoryPool, blockCache, registry,
            inflightLimiter, inflightTimeoutMs, prefetchExecutor,
            VirtualFileChannel.DEFAULT_MAX_COALESCED_IO_SIZE,
            NoOpFaultInjector.INSTANCE, decryptor);

        return new MemorySegmentPoolIndexInput(vfd, channel, blockCache, fileSize);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        Path path = dirPath.resolve(name);

        if (encryptionEnabled) {
            return new EncryptedIndexOutput(
                "EncryptedIndexOutput(path=\"" + path + "\")",
                name, path, keyResolver, encryptionMetadataCache);
        }

        return new MemorySegmentPoolIndexOutput(
            "MemorySegmentPoolIndexOutput(path=\"" + path + "\")",
            name, path);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        ensureOpen();
        String name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());
        Path path = dirPath.resolve(name);

        if (encryptionEnabled) {
            return new EncryptedIndexOutput(
                "EncryptedIndexOutput(path=\"" + path + "\")",
                name, path, keyResolver, encryptionMetadataCache);
        }

        return new MemorySegmentPoolIndexOutput(
            "MemorySegmentPoolIndexOutput(path=\"" + path + "\")",
            name, path);
    }

    /**
     * Closes this directory instance. Does NOT close shared node-level
     * resources (registry, memoryPool, blockCache, prefetchExecutor)
     * because those are shared across all Directory instances on the node
     * (one per shard — potentially 1000s). The node-level owner that
     * created these singletons is responsible for their lifecycle.
     */
    @Override
    public synchronized void close() throws IOException {
        super.close();
    }

    /**
     * Build a BlockDecryptor lambda that decrypts in-place on a writable
     * MemorySegment, matching the CryptoDirectIOBlockLoader pattern:
     * physical I/O -> decrypt -> cache plaintext.
     */
    private BlockDecryptor buildDecryptor(VirtualFileDescriptor vfd) {
        return (MemorySegment segment, long fileOffset) -> {
            try {
                String normalizedPath = vfd.normalizedPath();
                byte[] masterKey = keyResolver.getDataKey().getEncoded();

                EncryptionFooter footer = loadFooterForDecrypt(vfd, masterKey);
                var metadata = encryptionMetadataCache.getOrLoadMetadata(
                    normalizedPath, footer, masterKey);

                byte[] fileKey = metadata.getFileKey();
                byte[] messageId = metadata.getFooter().getMessageId();
                long frameSize = metadata.getFooter().getFrameSize();

                MemorySegmentDecryptor.decryptInPlaceFrameBased(
                    segment.address(),
                    segment.byteSize(),
                    fileKey,
                    masterKey,
                    messageId,
                    frameSize,
                    fileOffset,
                    normalizedPath,
                    encryptionMetadataCache
                );
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException("Decryption failed at offset " + fileOffset, e);
            }
        };
    }

    /**
     * Load the encryption footer for decryption. Cache-first, disk fallback.
     */
    private EncryptionFooter loadFooterForDecrypt(VirtualFileDescriptor vfd,
                                                   byte[] masterKey) throws IOException {
        String normalizedPath = vfd.normalizedPath();
        EncryptionFooter cached = encryptionMetadataCache.getFooter(normalizedPath);
        if (cached != null) {
            return cached;
        }
        try (FileChannel fc = FileChannel.open(vfd.filePath(), StandardOpenOption.READ)) {
            return EncryptionFooter.readViaFileChannel(normalizedPath, fc, masterKey,
                encryptionMetadataCache);
        }
    }

    /** Returns the VFD registry (for testing/observability). */
    public VfdRegistry registry() {
        return registry;
    }

    /** Returns the memory pool (for testing/observability). */
    public MemoryPool memoryPool() {
        return memoryPool;
    }

    /** Returns the block cache (for testing/observability). */
    public BlockCache blockCache() {
        return blockCache;
    }

    /** Returns whether encryption is enabled. */
    public boolean isEncryptionEnabled() {
        return encryptionEnabled;
    }
}
