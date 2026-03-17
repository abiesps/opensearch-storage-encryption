/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import static org.opensearch.index.store.block_loader.DirectIOReaderUtil.directIOReadAligned;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.cipher.MemorySegmentDecryptor;
import org.opensearch.index.store.footer.EncryptionFooter;
import org.opensearch.index.store.footer.EncryptionMetadataTrailer;
import org.opensearch.index.store.key.KeyResolver;

/**
 * BlockLoader that uses ByteBuffer.allocateDirect() instead of MemorySegmentPool.
 *
 * Same read + decrypt logic as CryptoDirectIOBlockLoader, but each block gets its own
 * direct ByteBuffer wrapped as a MemorySegment. GC handles cleanup — no pool, no explicit free.
 *
 * The RefCountedMemorySegment releaser is a no-op since there's no pool to return to.
 */
@SuppressWarnings("preview")
public class DirectByteBufferBlockLoader implements BlockLoader<RefCountedMemorySegment> {
    private static final Logger LOGGER = LogManager.getLogger(DirectByteBufferBlockLoader.class);

    private final KeyResolver keyResolver;
    private final EncryptionMetadataCache encryptionMetadataCache;

    public DirectByteBufferBlockLoader(KeyResolver keyResolver, EncryptionMetadataCache encryptionMetadataCache) {
        this.keyResolver = keyResolver;
        this.encryptionMetadataCache = encryptionMetadataCache;
    }

    @Override
    public RefCountedMemorySegment[] load(Path filePath, long startOffset, long blockCount, long poolTimeoutMs) throws Exception {
        if (!Files.exists(filePath)) {
            throw new NoSuchFileException(filePath.toString());
        }
        if ((startOffset & CACHE_BLOCK_MASK) != 0) {
            throw new IllegalArgumentException("startOffset must be block-aligned: " + startOffset);
        }
        if (blockCount <= 0) {
            throw new IllegalArgumentException("blockCount must be positive: " + blockCount);
        }

        RefCountedMemorySegment[] result = new RefCountedMemorySegment[(int) blockCount];
        long readLength = blockCount << CACHE_BLOCK_SIZE_POWER;

        try (
            Arena arena = Arena.ofConfined();
            FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ, DirectIOReaderUtil.getDirectOpenOption())
        ) {
            MemorySegment readBytes = directIOReadAligned(channel, filePath, startOffset, readLength, arena);
            long bytesRead = readBytes.byteSize();

            String normalizedPath = filePath.toAbsolutePath().normalize().toString();
            byte[] masterKey = keyResolver.getDataKey().getEncoded();

            EncryptionFooter footer = readFooterFromDisk(filePath, masterKey);
            var metadata = encryptionMetadataCache.getOrLoadMetadata(normalizedPath, footer, masterKey);
            byte[] messageId = metadata.getFooter().getMessageId();
            byte[] fileKey = metadata.getFileKey();

            MemorySegmentDecryptor.decryptInPlaceFrameBased(
                readBytes.address(),
                readBytes.byteSize(),
                fileKey,
                masterKey,
                messageId,
                EncryptionMetadataTrailer.DEFAULT_FRAME_SIZE,
                startOffset,
                normalizedPath,
                encryptionMetadataCache
            );

            if (bytesRead == 0) {
                throw new java.io.EOFException("Unexpected EOF at offset " + startOffset + " for " + filePath);
            }

            int blockIndex = 0;
            long bytesCopied = 0;

            while (blockIndex < blockCount && bytesCopied < bytesRead) {
                int remaining = (int) (bytesRead - bytesCopied);
                int toCopy = Math.min(CACHE_BLOCK_SIZE, remaining);

                // Allocate a direct ByteBuffer and wrap as MemorySegment
                ByteBuffer directBuf = ByteBuffer.allocateDirect(toCopy);
                MemorySegment bufSeg = MemorySegment.ofBuffer(directBuf);

                // Copy decrypted data into the direct buffer
                MemorySegment.copy(readBytes, bytesCopied, bufSeg, 0, toCopy);

                // Wrap in RefCountedMemorySegment with no-op releaser (GC handles ByteBuffer cleanup)
                RefCountedMemorySegment handle = new RefCountedMemorySegment(bufSeg, toCopy, rcms -> {});
                result[blockIndex++] = handle;
                bytesCopied += toCopy;
            }

            return result;

        } catch (NoSuchFileException e) {
            throw e;
        } catch (Exception e) {
            // Release any already-allocated handles on failure
            for (int i = 0; i < result.length; i++) {
                if (result[i] != null) {
                    result[i].close();
                    result[i] = null;
                }
            }
            LOGGER.error("DirectByteBuffer bulk read failed: path={} offset={} length={} err={}",
                filePath, startOffset, readLength, e.toString());
            throw e;
        }
    }

    private EncryptionFooter readFooterFromDisk(Path filePath, byte[] masterKey) throws IOException {
        String normalizedPath = filePath.toAbsolutePath().normalize().toString();
        EncryptionFooter cachedFooter = encryptionMetadataCache.getFooter(normalizedPath);
        if (cachedFooter != null) {
            return cachedFooter;
        }
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize < EncryptionMetadataTrailer.MIN_FOOTER_SIZE) {
                throw new IOException("File too small to contain footer: " + filePath);
            }
            ByteBuffer minBuffer = ByteBuffer.allocate(EncryptionMetadataTrailer.MIN_FOOTER_SIZE);
            channel.read(minBuffer, fileSize - EncryptionMetadataTrailer.MIN_FOOTER_SIZE);
            byte[] minFooterBytes = minBuffer.array();
            if (!isValidOSEFFile(minFooterBytes)) {
                throw new IOException("Not an OSEF file - " + filePath);
            }
            return EncryptionFooter.readViaFileChannel(normalizedPath, channel, masterKey, encryptionMetadataCache);
        }
    }

    private boolean isValidOSEFFile(byte[] minFooterBytes) {
        int magicOffset = minFooterBytes.length - EncryptionMetadataTrailer.MAGIC.length;
        for (int i = 0; i < EncryptionMetadataTrailer.MAGIC.length; i++) {
            if (minFooterBytes[magicOffset + i] != EncryptionMetadataTrailer.MAGIC[i]) {
                return false;
            }
        }
        return true;
    }
}
