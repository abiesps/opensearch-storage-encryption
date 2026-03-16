package org.opensearch.index.store.bufferpoolfs;

import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;

import java.nio.file.Path;

public class CachedMemorySegmentIndexInputByteBuffer {
    public static IndexInput newInstance(String resourceDescription,
                                         Path file,
                                         long contentLength,
                                         BlockCache<RefCountedMemorySegment> blockCacheRef,
                                         ReadaheadManager readAheadManager,
                                         ReadaheadContext readAheadContext,
                                         BlockSlotTinyCache pinRegistry) {
        return null;
    }
}
