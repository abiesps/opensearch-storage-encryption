# Requirements Document

## Introduction

A major redesign of the bufferpool block cache architecture for CachedMemorySegmentIndexInputV2 and its memory management layer. The redesign eliminates performance bottlenecks (String-based cache keys, BlockSlotTinyCache, malloc-based MemorySegmentPool with reference counting) and simplifies memory lifecycle by leveraging ByteBuffer.allocateDirect with GC-based reclamation and Caffeine-driven eviction with eviction-listener-based SparseLongBlockTable invalidation. The target scale is 100K shards × 80 segments × 100 files per segment.

## Glossary

- **IndexInput**: A Lucene abstract class representing a readable stream of bytes from an index file. Supports sequential reads, random access, cloning, and slicing.
- **CachedMemorySegmentIndexInputV2**: The new IndexInput implementation that reads file data from cached 4KB blocks backed by direct ByteBuffers, using a SparseLongBlockTable for per-file block lookup.
- **SparseLongBlockTable**: A per-file, read-side cache that maps a block ID (long) to a MemorySegment view of the ByteBuffer held by CaffeineCache. It is NOT the source of truth for block ownership — CaffeineCache is. When Caffeine evicts a block, the eviction listener nulls the corresponding table entry. A null entry simply means "ask Caffeine again." Uses a two-level sparse array with a directory of chunk arrays, where each chunk holds 256 entries. Only chunks that are actually accessed get allocated.
- **VirtualFileDescriptorRegistry**: A node-level singleton that assigns a globally unique integer vfd to each openInput() call using an AtomicInteger counter. Each parent IndexInput gets its own vfd, even if multiple parents are opened for the same file path. This ensures a 1:1 mapping between vfd and SparseLongBlockTable, simplifying eviction listener cleanup. Node-level scope is required because CaffeineBlockCache is shared across all shards on the node.
- **VirtualFileDescriptorId (vfd)**: An integer identifier assigned by VirtualFileDescriptorRegistry to uniquely identify a parent IndexInput across the entire node. Two parents opened for the same file get different vfds.
- **BlockCacheKey**: A cache key composed of (int vfd, long blockOffset) that uniquely identifies a cached block. Replaces the previous (Path, long) key to avoid String operations on the hot path.
- **CaffeineCache**: A Caffeine-based concurrent cache that stores (BlockCacheKey → ByteBuffer) mappings, handles automatic loading on miss, and fires eviction listeners for SparseLongBlockTable cleanup.
- **DirectMemoryAdmissionController**: A component that checks direct memory utilization before every ByteBuffer.allocateDirect() call, using a self-tracked AtomicLong counter with periodic BufferPoolMXBean cross-checks, and applies soft/hard thresholds to trigger or block on GC.
- **Parent_IndexInput**: The original IndexInput instance opened by BufferPoolDirectory for a file. Owns the SparseLongBlockTable and VirtualFileDescriptorRegistry entry. Always explicitly closed by Lucene's IndexReader lifecycle.
- **Clone**: A copy of an IndexInput sharing the same underlying block data, created via IndexInput.clone(). Lifecycle is bounded by the parent's close().
- **Slice**: A sub-range view of an IndexInput, created via IndexInput.slice(). Lifecycle is bounded by the parent's close().
- **Block**: A 4KB unit of cached file data, stored as a direct ByteBuffer in CaffeineCache.
- **Chunk**: A 256-entry array within SparseLongBlockTable. Each chunk occupies 2KB of heap memory and maps 256 consecutive block IDs to their cached ByteBuffer references.
- **BufferPoolDirectory**: A Lucene Directory implementation that manages one shard's files and creates CachedMemorySegmentIndexInputV2 instances. References the node-level VirtualFileDescriptorRegistry singleton and the node-level CaffeineBlockCache singleton.

## Requirements

### Requirement 1: VirtualFileDescriptorRegistry

**User Story:** As a system designer, I want file paths replaced with integer identifiers in cache keys, so that hot-path cache lookups avoid String hashing and equals operations.

#### Acceptance Criteria

1. THE VirtualFileDescriptorRegistry SHALL be a node-level singleton, shared across all BufferPoolDirectory instances on the host.
2. THE VirtualFileDescriptorRegistry SHALL assign a globally unique integer vfd to each openInput() call using an AtomicInteger counter, even if the same file path is opened multiple times.
3. THE VirtualFileDescriptorRegistry SHALL NOT deduplicate vfd assignments by file path. Each parent IndexInput gets its own vfd to ensure a 1:1 mapping between vfd and SparseLongBlockTable.
4. WHEN a Parent_IndexInput is closed, THE VirtualFileDescriptorRegistry SHALL remove the mapping for that file's vfd.
5. THE VirtualFileDescriptorRegistry SHALL support concurrent registration and deregistration from multiple threads across all shards without data corruption.
6. THE vfd values SHALL be globally unique across the entire node because the CaffeineBlockCache is a node-level singleton and cache keys (vfd, blockOffset) must not collide across different parent IndexInputs.
7. THE VirtualFileDescriptorRegistry SHALL detect AtomicInteger overflow (wrap-around past Integer.MAX_VALUE). WHEN overflow is detected, THE registry SHALL either recycle vfd IDs from closed entries or upgrade the counter to AtomicLong. The system SHALL NOT silently wrap around and produce duplicate vfd values for concurrent parent IndexInputs.

### Requirement 2: BlockCacheKey Redesign

**User Story:** As a system designer, I want cache keys to use integer vfd and long blockOffset instead of Path objects, so that cache operations on the hot path are faster and allocation-free.

#### Acceptance Criteria

1. THE BlockCacheKey SHALL be composed of an int vfd field and a long blockOffset field.
2. THE BlockCacheKey SHALL implement equals() using only the vfd and blockOffset fields.
3. THE BlockCacheKey SHALL implement hashCode() using only the vfd and blockOffset fields, producing a well-distributed hash.
4. THE BlockCacheKey SHALL not reference any Path or String object.

### Requirement 3: SparseLongBlockTable Chunk Size Reduction

**User Story:** As a system designer, I want SparseLongBlockTable to use 256-entry chunks instead of 4096-entry chunks, so that per-chunk memory overhead is proportional to actual cached data.

#### Acceptance Criteria

1. THE SparseLongBlockTable SHALL use a CHUNK_SHIFT of 8, resulting in 256-entry chunks.
2. WHEN a block ID is accessed for the first time in a chunk that has not been allocated, THE SparseLongBlockTable SHALL allocate a new 256-entry chunk on demand.
3. THE SparseLongBlockTable SHALL support files up to 10GB with a 4KB block size (up to 2,621,440 blocks per file).
4. THE SparseLongBlockTable SHALL track a chunkPopCount per chunk using an AtomicInteger, representing the number of non-null entries in that chunk. AtomicInteger is required because put() and remove() can race on the same chunk from different threads, and a plain int would produce lost increments/decrements.
5. WHEN chunkPopCount for a chunk reaches zero, THE SparseLongBlockTable SHALL re-scan the chunk to verify it is truly empty before nulling out the chunk array. This prevents premature chunk deallocation from put/remove races where: put(block) and remove(block) race → popcount decrements to zero → but another entry was concurrently inserted. Only if the re-scan confirms all slots are null SHALL the chunk be deallocated to reclaim the 2KB of heap memory.

#### Concurrency Model

6. THE SparseLongBlockTable SHALL use a relaxed concurrency model with no locks on the read hot path. Java reference atomicity (JLS §17.7) guarantees that readers always see either null or a fully-constructed MemorySegment reference.
7. THE SparseLongBlockTable SHALL use VarHandle with proper memory ordering for all chunk array slot access. Slot writes (put and remove) SHALL use VarHandle.setRelease() to ensure a store-store fence, so that the MemorySegment's internal fields are fully initialized before the reference becomes visible. Slot reads (get) SHALL use VarHandle.getAcquire() to ensure a load-load fence, so that after reading a non-null reference, subsequent reads of the MemorySegment's fields see the fully initialized values. Plain array access (chunk[inner]) is insufficient because it could expose a partially initialized MemorySegment reference to a reader thread.
8. THE SparseLongBlockTable.directory field SHALL be declared volatile. This ensures that when a thread grows the directory (allocates a larger array and copies chunk references), all other threads immediately see the new directory. Without volatile, a reader could see the old directory but access a chunk that was allocated in the new directory, leading to invisible entries.
9. THE `get(blockId)` operation SHALL be lock-free: read the volatile directory reference, then read the chunk array slot via VarHandle.getAcquire(). If a reader obtains a MemorySegment reference before the eviction listener nulls the slot, the reader safely completes its read because the MemorySegment keeps the underlying ByteBuffer reachable (preventing GC collection), and the data is correct (ByteBuffers are never reused in V2).
10. THE `put(blockId, segment)` operation SHALL synchronize only for chunk allocation (directory growth). The slot write itself SHALL use VarHandle.setRelease().
11. THE `remove(blockId)` operation (called by Caffeine eviction listener) SHALL write null to the chunk array slot via VarHandle.setRelease(). No lock required.
12. WHEN `put()` and `remove()` race on the same slot, THE system SHALL tolerate the outcome because SparseLongBlockTable is a read-side cache, not the source of truth. If put wins, the entry points to a ByteBuffer that Caffeine no longer holds — the reader gets correct data and the ByteBuffer is collected after the reader finishes. If remove wins, the next reader finds null and falls back to Caffeine. The chunkPopCount (AtomicInteger) ensures that transient races do not cause premature chunk deallocation — the re-scan-before-delete guard in criterion 5 catches any inconsistency.
13. THE `clear()` operation (called on parent close) SHALL null out all chunk references. Concurrent readers that already obtained a MemorySegment reference before clear() SHALL safely complete their in-progress read because the MemorySegment keeps the ByteBuffer reachable.

#### VarHandle Testing

14. THE concurrency tests SHALL verify VarHandle ordering by having writer threads publish MemorySegments with known sentinel data, and reader threads SHALL verify that any non-null MemorySegment read via getAcquire() contains the expected sentinel data (no partially initialized references).

#### Concurrency Testing

15. THE SparseLongBlockTable SHALL have aggressive concurrency unit tests covering the following scenarios:
    - a. N writer threads calling `put()` on overlapping block ID ranges while M reader threads call `get()` continuously. Readers SHALL never observe a corrupted reference (only null or a valid MemorySegment).
    - b. N writer threads calling `put()` while M eviction threads call `remove()` on the same block IDs. After all threads complete, every slot SHALL be either null or a valid MemorySegment.
    - c. One thread calling `clear()` while N reader threads call `get()` and M writer threads call `put()`. No thread SHALL throw an exception. Readers SHALL see either null or a valid MemorySegment.
    - d. N threads concurrently triggering chunk allocation for the same outer index. Only one chunk SHALL be allocated (no lost writes from double allocation).
    - e. N threads concurrently triggering directory growth beyond the initial capacity. The directory SHALL grow correctly with no lost chunks.
    - f. Sustained concurrent put/get/remove over 10 seconds with 32+ threads. No exceptions, no corrupted references, no memory leaks (all MemorySegments created during the test SHALL be valid or null at completion).

### Requirement 4: ByteBuffer.allocateDirect Replaces MemorySegmentPool

**User Story:** As a system designer, I want to replace the malloc-based MemorySegmentPool and reference counting with ByteBuffer.allocateDirect and GC-based reclamation, so that memory management is simpler and eliminates the free list, generation tracking, and reference counting overhead.

#### Acceptance Criteria

1. WHEN CaffeineCache loads a new block on a cache miss, THE CaffeineCache SHALL allocate a direct ByteBuffer of 4KB via ByteBuffer.allocateDirect().
2. THE CaffeineCache SHALL store the parent ByteBuffer as the cache value.
3. WHEN a reader needs access to a cached block, THE CaffeineCache SHALL return a slice of the parent ByteBuffer.
4. WHEN a cached block is evicted by CaffeineCache, THE system SHALL rely on GC to reclaim the direct ByteBuffer memory once no references (parent or slices) remain.
5. THE system SHALL not use any free list, generation counter, or reference counting for block lifecycle management.

### Requirement 5: Caffeine Eviction Listener Drives SparseLongBlockTable Cleanup

**User Story:** As a system designer, I want Caffeine eviction events to automatically clean up SparseLongBlockTable entries, so that the table stays consistent with what Caffeine actually holds, even for long-lived parent IndexInputs that are never closed.

#### Acceptance Criteria

1. WHEN CaffeineCache evicts or invalidates a (vfd, blockOffset) entry, THE eviction listener SHALL look up the SparseLongBlockTable for that vfd via a ConcurrentHashMap of WeakReference entries (vfdToTable registry).
2. WHEN the SparseLongBlockTable for the evicted vfd is found, THE eviction listener SHALL null out the corresponding slot first (via VarHandle.setRelease()), then decrement the chunkPopCount. This ordering is critical: the slot must be nulled before the popcount is decremented, and both operations must use release semantics. If popcount were decremented first, another thread could observe popcount == 0 while the slot is still non-null, triggering a premature chunk deallocation that races with the slot null-write.
3. WHEN chunkPopCount for a chunk reaches zero after an eviction, THE eviction listener SHALL re-scan the chunk (per Requirement 3 criterion 5) and null out the entire chunk array only if all slots are confirmed null.
4. IF the WeakReference for a vfd has been cleared (SparseLongBlockTable was garbage collected because the parent was closed), THEN THE eviction listener SHALL skip cleanup for that entry without error.
5. WHEN the eviction listener encounters a cleared WeakReference during lookup, THE eviction listener SHALL opportunistically remove the stale WeakReference entry from the vfdToTable registry. This prevents unbounded growth of the registry from accumulated cleared WeakReferences over the lifetime of the node.
6. WHEN a reader thread holds a MemorySegment reference to a block that Caffeine has evicted and the table entry has been nulled, THE reader SHALL safely complete its in-progress read because the underlying ByteBuffer remains reachable via the MemorySegment until the reader releases it.
7. AFTER the eviction listener nulls a table entry, THE next read to that block SHALL find null in the table, fall back to CaffeineCache getOrLoad, and re-populate the table entry with the newly loaded block.

### Requirement 6: CachedMemorySegmentIndexInputV2 Segment Access Pattern

**User Story:** As a system designer, I want the IndexInput to follow a Lucene-style MultiSegmentIndexInput access pattern using SparseLongBlockTable, so that sequential reads are fast and random access is efficient.

#### Acceptance Criteria

1. WHEN performing a sequential read, THE CachedMemorySegmentIndexInputV2 SHALL attempt to read from the currentSegment using the MemorySegment layout accessor.
2. WHEN a sequential read throws IndexOutOfBoundsException (block boundary crossed), THE CachedMemorySegmentIndexInputV2 SHALL advance currentSegmentIndex and load the next segment from SparseLongBlockTable.
3. WHEN a sequential read throws NullPointerException (segment not yet loaded), THE CachedMemorySegmentIndexInputV2 SHALL look up the block in SparseLongBlockTable first, then fall back to CaffeineCache getOrLoad if the table entry is null.
4. WHEN the new segment index exceeds the maximum possible for the file size, THE CachedMemorySegmentIndexInputV2 SHALL throw EOFException.
5. WHEN performing a random access read, THE CachedMemorySegmentIndexInputV2 SHALL compute the block ID from the position and go directly to SparseLongBlockTable.get(blockId).
6. WHEN SparseLongBlockTable.get(blockId) returns null for a random access read, THE CachedMemorySegmentIndexInputV2 SHALL load the block via CaffeineCache getOrLoad and populate the SparseLongBlockTable entry.

### Requirement 7: IndexInput Close Semantics

**User Story:** As a system designer, I want simple close() semantics that rely on Lucene's existing lifecycle guarantees, where the parent IndexInput owns the lifecycle and clones/slices are always scoped within the parent's lifetime.

#### Acceptance Criteria

1. WHEN close() is called on a Parent_IndexInput, THE Parent_IndexInput SHALL null out the entire SparseLongBlockTable and deregister from the vfdToTable WeakReference registry.
2. WHEN close() is called on a Parent_IndexInput, THE Parent_IndexInput SHALL deregister the vfd from VirtualFileDescriptorRegistry.
3. WHEN close() is called on a clone or slice, THE clone or slice SHALL set currentSegment to null.
4. WHEN close() is called on any IndexInput that is already closed, THE CachedMemorySegmentIndexInputV2 SHALL return without error (idempotent close).
5. THE system SHALL NOT use java.lang.ref.Cleaner for any IndexInput lifecycle management. Lucene guarantees that clones and slices do not outlive the parent IndexInput.

### Requirement 8: Direct Memory Admission Control

**User Story:** As a system designer, I want direct memory allocation to be gated by admission control with soft and hard thresholds, so that the system avoids OutOfMemoryError under heavy allocation churn.

#### Acceptance Criteria

1. WHEN ByteBuffer.allocateDirect() is about to be called, THE DirectMemoryAdmissionController SHALL check the current direct memory utilization.
2. THE DirectMemoryAdmissionController SHALL track direct memory utilization using a self-tracked AtomicLong counter (no syscall on the hot path).
3. THE DirectMemoryAdmissionController SHALL track outstanding direct allocations — incrementing the counter on every ByteBuffer.allocateDirect() call and decrementing it when the corresponding ByteBuffer is evicted from CaffeineCache (in the eviction listener). This provides an accurate view of live direct memory held by the cache, independent of GC timing.
4. THE DirectMemoryAdmissionController SHALL periodically cross-check the self-tracked counter against BufferPoolMXBean to correct drift.
5. WHEN direct memory utilization exceeds the soft threshold (configurable, default 85%), THE DirectMemoryAdmissionController SHALL request an asynchronous System.gc() and allow the allocation to proceed without blocking.
6. WHEN direct memory utilization exceeds the hard threshold (configurable, default 95%), THE DirectMemoryAdmissionController SHALL block the calling thread and wait for GC to reclaim memory before allowing the allocation.
7. IF the hard threshold is exceeded and GC does not reclaim sufficient memory within a configurable timeout, THEN THE DirectMemoryAdmissionController SHALL throw an appropriate exception indicating direct memory exhaustion.
8. THE DirectMemoryAdmissionController SHALL require -XX:+ExplicitGCInvokesConcurrent JVM flag for production use to ensure System.gc() triggers concurrent GC rather than stop-the-world.

### Requirement 9: BlockSlotTinyCache Removal

**User Story:** As a system designer, I want BlockSlotTinyCache removed entirely, so that each file uses its own SparseLongBlockTable for block lookup and the L1 cache indirection is eliminated.

#### Acceptance Criteria

1. THE CachedMemorySegmentIndexInputV2 SHALL not use BlockSlotTinyCache for any block lookup operation.
2. THE CachedMemorySegmentIndexInputV2 SHALL use SparseLongBlockTable as the primary per-file block lookup structure.
3. WHEN a block is loaded from CaffeineCache, THE CachedMemorySegmentIndexInputV2 SHALL populate the corresponding SparseLongBlockTable entry.

### Requirement 10: Scale and Configuration Parameters

**User Story:** As a system designer, I want the system to support the target scale parameters, so that it can handle production workloads of 100K shards with 80 segments and 100 files per segment.

#### Acceptance Criteria

1. THE system SHALL use a block size of 4KB in production configuration.
2. THE system SHALL support segment files up to 10GB in size.
3. THE SparseLongBlockTable SHALL support up to 2,621,440 blocks per file (10GB / 4KB).
4. THE SparseLongBlockTable SHALL use 256-entry chunks (CHUNK_SHIFT=8), each consuming 2KB of heap memory.
5. THE system SHALL support concurrent operation across 100,000 shards, each with up to 80 segments and 100 files per segment.


### Requirement 11: Memory Leak Validation

**User Story:** As a system designer, I want comprehensive memory leak validation tests, so that I can verify all lifecycle paths correctly release resources.

#### Acceptance Criteria

1. WHEN a Parent_IndexInput is closed, THE SparseLongBlockTable chunks SHALL be nulled and the vfdToTable registry entry SHALL be removed.
2. WHEN CaffeineCache evicts entries for a file, THE corresponding SparseLongBlockTable entries SHALL be nulled.
3. WHEN all entries in a SparseLongBlockTable chunk are evicted, THE chunk array SHALL be nulled (reclaiming 2KB).
4. WHEN files are opened and closed repeatedly, THE VirtualFileDescriptorRegistry size SHALL remain bounded (no leaked vfd mappings).
5. WHEN direct ByteBuffers are churned through the cache under a constrained MaxDirectMemorySize, THE system SHALL not throw OutOfMemoryError.
6. WHEN 10,000 files with concurrent clones are operated on simultaneously, THE total memory usage SHALL remain bounded.

### Requirement 12: Direct Memory Admission Control Validation

**User Story:** As a system designer, I want admission control tests that verify the system handles direct memory pressure correctly under constrained MaxDirectMemorySize.

#### Acceptance Criteria

1. WHEN a single thread churns 256MB of data through a 4MB cache with a 32MB direct memory limit, THE DirectMemoryAdmissionController SHALL prevent OutOfMemoryError.
2. WHEN 8 threads concurrently churn 512MB total of data through a 4MB cache with a 32MB direct memory limit, THE DirectMemoryAdmissionController SHALL prevent OutOfMemoryError.
3. WHEN admission control is disabled and the same churn workload is applied, THE test SHALL document the failure mode (OutOfMemoryError or elevated peak memory usage).
4. THE admission control validation tests SHALL run with -XX:MaxDirectMemorySize=32m JVM flag.
