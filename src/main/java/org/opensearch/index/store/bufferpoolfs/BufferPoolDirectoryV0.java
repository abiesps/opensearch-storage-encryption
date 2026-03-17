/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.footer.EncryptionFooter;
import org.opensearch.index.store.footer.EncryptionMetadataTrailer;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.metrics.ErrorType;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.ReadaheadManagerImpl;

import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * BufferPoolDirectory variant that uses the original V0 CachedMemorySegmentIndexInput
 * (with BlockSlotTinyCache / pin-unpin lifecycle) for benchmarking comparison.
 */
public class BufferPoolDirectoryV0 extends BufferPoolDirectory {

    private final BlockCache<RefCountedMemorySegment> blockCacheRef;
    private final Worker readAheadWorkerRef;
    private final Path dirPathRef;
    private final byte[] masterKeyBytesRef;
    private final EncryptionMetadataCache encryptionMetadataCacheRef;
    private boolean disableL1Cache;

    public BufferPoolDirectoryV0(
        Path path,
        LockFactory lockFactory,
        Provider provider,
        KeyResolver keyResolver,
        Pool<RefCountedMemorySegment> memorySegmentPool,
        BlockCache<RefCountedMemorySegment> blockCache,
        BlockLoader<RefCountedMemorySegment> blockLoader,
        Worker worker,
        EncryptionMetadataCache encryptionMetadataCache
    ) throws IOException {
        super(path, lockFactory, provider, keyResolver, memorySegmentPool, blockCache, blockLoader, worker, encryptionMetadataCache);
        this.blockCacheRef = blockCache;
        this.readAheadWorkerRef = worker;
        this.dirPathRef = getDirectory();
        this.masterKeyBytesRef = keyResolver.getDataKey().getEncoded();
        this.encryptionMetadataCacheRef = encryptionMetadataCache;
    }

    /**
     * When set to true, subsequent openInput calls will use a
     * {@link NoOpBlockSlotTinyCache} instead of the real L1 cache.
     */
    public void setDisableL1Cache(boolean disableL1Cache) {
        this.disableL1Cache = disableL1Cache;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        try {
            ensureOpen();
            ensureCanRead(name);

            Path file = dirPathRef.resolve(name);
            long rawFileSize = Files.size(file);
            if (rawFileSize == 0) {
                throw new IOException("Cannot open empty file with DirectIO: " + file);
            }

            long contentLength = calculateContentLength(file, rawFileSize);

            ReadaheadManager readAheadManager = new ReadaheadManagerImpl(readAheadWorkerRef, blockCacheRef);
            ReadaheadContext readAheadContext = readAheadManager.register(file, contentLength);
            BlockSlotTinyCache pinRegistry = disableL1Cache
                ? new NoOpBlockSlotTinyCache(blockCacheRef, file, contentLength)
                : new BlockSlotTinyCache(blockCacheRef, file, contentLength);

            return CachedMemorySegmentIndexInputV0.newInstance(
                "CachedMemorySegmentIndexInputV0(path=\"" + file + "\")",
                file,
                contentLength,
                blockCacheRef,
                readAheadManager,
                readAheadContext,
                pinRegistry
            );
        } catch (Exception e) {
            CryptoMetricsService.getInstance().recordError(ErrorType.INDEX_INPUT_ERROR);
            throw e;
        }
    }

    private long calculateContentLength(Path file, long rawFileSize) throws IOException {
        if (rawFileSize < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
            return rawFileSize;
        }

        String normalizedPath = EncryptionMetadataCache.normalizePath(file);
        EncryptionFooter cachedFooter = encryptionMetadataCacheRef.getFooter(normalizedPath);
        if (cachedFooter != null) {
            return rawFileSize - cachedFooter.getFooterLength();
        }

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            EncryptionFooter footer = EncryptionFooter.readViaFileChannel(normalizedPath, channel, masterKeyBytesRef, encryptionMetadataCacheRef);
            return rawFileSize - footer.getFooterLength();
        } catch (EncryptionFooter.NotOSEFFileException e) {
            return rawFileSize;
        }
    }
}
