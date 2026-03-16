# Implementation Plan: BufferPool V2 Memory Management

## Overview

Incremental implementation of the V2 block cache memory management redesign. Each task builds on the previous, starting with foundational components (VFD registry, cache key), then admission controller, then upgrading SparseLongBlockTable, integrating cache with admission control, then concurrency stress tests (which now exercise the full stack including admission control), then IndexInput changes with an early integration test, and finally admission control stress tests. All changes target existing classes in `block_cache_v2` and `bufferpoolfs` packages.

## Tasks

- [x] 1. Set up test infrastructure for jqwik property-based testing
  - Add `net.jqwik:jqwik:1.9.+` dependency to `build.gradle` under `testImplementation`
  - Switch test task from `useJUnit()` to `useJUnitPlatform()` to enable JUnit 5 + jqwik
  - Add `--enable-preview` to test JVM args if not already present
  - Create test directories: `src/test/java/org/opensearch/index/store/block_cache_v2/` and `src/test/java/org/opensearch/index/store/integration/`
  - Verify jqwik runs with a trivial `@Property` smoke test
  - _Requirements: Testing Strategy (design document)_

- [x] 2. Update VirtualFileDescriptorRegistry with overflow detection
  - [x] 2.1 Add overflow detection to `register()` — check `vfd < 0` after `getAndIncrement()` and throw `IllegalStateException`
    - Modify `VirtualFileDescriptorRegistry.register()` per design Component 1
    - _Requirements: 1.2, 1.3, 1.5, 1.7_

  - [x]* 2.2 Write property test: VFD uniqueness under concurrent registration (Property 1)
    - **Property 1: VFD uniqueness under concurrent registration**
    - N concurrent `register()` calls → all returned vfds are distinct positive integers
    - Use jqwik `@Property` with `@ForAll int threadCount` (2–64 range)
    - **Validates: Requirements 1.2, 1.3, 1.5, 1.6**

  - [x]* 2.3 Write property test: VFD register/deregister lifecycle (Property 2)
    - **Property 2: VFD register/deregister lifecycle**
    - After `deregister(vfd)`, `getPath(vfd)` returns null; registry size stays bounded across cycles
    - **Validates: Requirements 1.4, 7.1, 7.2, 11.1, 11.4**

  - [x]* 2.4 Write unit tests for VirtualFileDescriptorRegistry edge cases
    - Test singleton behavior (Req 1.1)
    - Test overflow detection: set counter near `Integer.MAX_VALUE`, verify `IllegalStateException` on wrap (Req 1.7)
    - _Requirements: 1.1, 1.7_

- [x] 3. Update BlockCacheKeyV2 to store blockId with new hashCode
  - [x] 3.1 Refactor `BlockCacheKeyV2` to store `long blockId` instead of `long blockOffset`
    - Rename field from `blockOffset` to `blockId`
    - Add `blockOffset()` method that reconstructs via `blockId << StaticConfigs.CACHE_BLOCK_SIZE_POWER`
    - Update `hashCode()` to `vfd * 31 ^ Long.hashCode(blockId)` per design Component 2
    - Update `equals()` to compare `vfd` and `blockId`
    - Update `offset()` to return `blockOffset()`
    - Update `toString()` to show `blockId`
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [x]* 3.2 Write property test: BlockCacheKeyV2 equals/hashCode contract (Property 3)
    - **Property 3: BlockCacheKeyV2 equals/hashCode contract**
    - For any two keys, `a.equals(b)` iff `a.vfd()==b.vfd() && a.blockId()==b.blockId()`; when equal, `hashCode` matches; round-trip accessor check
    - **Validates: Requirements 2.1, 2.2, 2.3**

  - [x]* 3.3 Write property test: blockId ↔ blockOffset roundtrip consistency (Property 3b)
    - **Property 3b: blockId consistency**
    - For any valid blockOffset (aligned to CACHE_BLOCK_SIZE), `blockOffset >>> CACHE_BLOCK_SIZE_POWER` → blockId → `blockId << CACHE_BLOCK_SIZE_POWER` → original blockOffset. And for any blockId in [0, maxBlockId], `blockId << POWER >>> POWER` == blockId.
    - Catches subtle shift bugs where incorrect power or signed shift silently corrupts keys.
    - **Validates: Requirements 2.1, 2.3**

- [x] 4. Checkpoint — Verify foundational components
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Upgrade SparseLongBlockTable with VarHandle, chunkPopCount, and chunk reclamation
  - [x] 5.1 Add VarHandle for `MemorySegment[]` slot access and `AtomicInteger[] chunkPopCounts`
    - Add `private static final VarHandle SEGMENT_ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(MemorySegment[].class)`
    - Add `private AtomicInteger[] chunkPopCounts` parallel to `directory`, grown in lockstep in `allocateChunk()`
    - Update `get()` to use `SEGMENT_ARRAY_HANDLE.getAcquire(chunk, inner)`
    - Update `put()` to use `SEGMENT_ARRAY_HANDLE.setRelease(chunk, inner, segment)` and increment `chunkPopCounts[outer]` when prev was null
    - Update `remove()` to use `SEGMENT_ARRAY_HANDLE.setRelease(chunk, inner, null)`, decrement `chunkPopCounts[outer]`, and call `reclaimChunkIfEmpty(outer)` when count hits 0
    - Add `reclaimChunkIfEmpty()` re-scan guard per design Component 3
    - Update `clear()` to null all chunks and reset popcount array
    - Update `allocateChunk()` to grow `chunkPopCounts` in lockstep with `directory`
    - _Requirements: 3.1, 3.2, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10, 3.11, 3.12, 3.13_

  - [x]* 5.2 Write property test: put/get round trip (Property 4)
    - **Property 4: SparseLongBlockTable put/get round trip**
    - For any blockId and MemorySegment, after `put(blockId, seg)`, `get(blockId)` returns the stored segment, including lazy chunk allocation
    - **Validates: Requirements 3.2, 9.3**

  - [x]* 5.3 Write property test: chunkPopCount accuracy (Property 5)
    - **Property 5: SparseLongBlockTable chunkPopCount accuracy**
    - For any sequence of put/remove on a single chunk, chunkPopCount equals the number of non-null entries
    - **Validates: Requirements 3.4**

  - [x]* 5.4 Write property test: chunk deallocation after full eviction (Property 6)
    - **Property 6: Chunk deallocation after full eviction**
    - After all entries in a chunk are removed and re-scan confirms all null, chunk array is nulled
    - **Validates: Requirements 3.5, 5.3, 11.3**

  - [x]* 5.5 Write property test: VarHandle ordering — no partial references (Property 7)
    - **Property 7: VarHandle ordering — no partially initialized references**
    - Writer threads publish MemorySegments with sentinel data via `setRelease`; reader threads via `getAcquire` verify any non-null segment has expected sentinel data
    - **Validates: Requirements 3.7, 3.14**

  - [x]* 5.6 Write unit test for max block ID edge case
    - Test 10GB file → blockId = 2,621,439 (max for 4KB blocks) — put/get/remove succeed
    - _Requirements: 3.3, 10.2, 10.3_

- [x] 6. Checkpoint — Verify SparseLongBlockTable core logic
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Implement DirectMemoryAdmissionController (moved earlier — before cache integration)
  - [x] 7.1 Create `DirectMemoryAdmissionController` class in `block_cache_v2` package
    - Implement `AtomicLong outstandingBytes` counter
    - Implement `acquire(int bytes)` with soft/hard threshold logic per design Component 5
    - Implement `release(int bytes)` decrement
    - Implement periodic `BufferPoolMXBean` cross-check (every 1000 allocations)
    - Configurable `softThreshold` (default 0.85), `hardThreshold` (default 0.95), `hardTimeoutMs` (default 5000)
    - Create `DirectMemoryExhaustedException` exception class
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7_

  - [x]* 7.2 Write property test: counter accuracy (Property 22)
    - **Property 22: Admission controller counter accuracy**
    - For any sequence of `acquire(n)` and `release(n)`, outstanding bytes = sum(acquired) - sum(released)
    - **Validates: Requirements 8.2, 8.3**

  - [x]* 7.3 Write property test: soft threshold allows allocation (Property 23)
    - **Property 23: Soft threshold allows allocation**
    - When utilization is between soft and hard thresholds, `acquire()` succeeds without blocking
    - **Validates: Requirements 8.5**

  - [x]* 7.4 Write property test: hard threshold blocks allocation (Property 24)
    - **Property 24: Hard threshold blocks allocation**
    - When utilization exceeds hard threshold, `acquire()` blocks until utilization drops or timeout
    - **Validates: Requirements 8.6**

  - [x]* 7.5 Write unit test for hard threshold timeout exception
    - Verify `DirectMemoryExhaustedException` thrown when GC doesn't reclaim within timeout
    - _Requirements: 8.7_

- [x] 8. Update CaffeineBlockCacheV2 with admission control and blockId-based eviction
  - [x] 8.1 Integrate `DirectMemoryAdmissionController` into `CaffeineBlockCacheV2`
    - Add `admissionController` field, call `acquire(CACHE_BLOCK_SIZE)` before `allocateDirect()` in `loadBlock()`
    - Call `release(CACHE_BLOCK_SIZE)` in eviction listener and on load failure (catch block)
    - Update `loadBlock()` to use `key.blockOffset()` (reconstructed from blockId) for disk read offset
    - Update eviction listener `onRemoval()` to call `table.remove(key.blockId())` directly — no shift needed
    - Add opportunistic stale WeakReference cleanup in eviction listener
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 5.1, 5.2, 5.3, 5.4, 5.5_

  - [x]* 8.2 Write property test: cache returns read-only direct ByteBuffer (Property 14)
    - **Property 14: Cache returns read-only ByteBuffer slices**
    - For any loaded block, returned ByteBuffer is direct, read-only, limit = block size (or remaining for last block)
    - **Validates: Requirements 4.1, 4.3**

  - [x]* 8.3 Write property test: eviction nulls table entry (Property 15)
    - **Property 15: Eviction nulls table entry and triggers fallback**
    - After eviction, `SparseLongBlockTable.get(blockId)` returns null; next access reloads from Caffeine
    - **Validates: Requirements 5.1, 5.2, 5.7, 11.2**

  - [x]* 8.4 Write property test: stale WeakReference cleanup (Property 16)
    - **Property 16: Stale WeakReference cleanup**
    - When WeakRef is cleared, eviction listener doesn't throw and opportunistically removes stale entry
    - **Validates: Requirements 5.4, 5.5**

  - [x]* 8.5 Write property test: reader safety after eviction (Property 17)
    - **Property 17: Reader safety after eviction**
    - Reader holding MemorySegment reference before eviction safely completes read with correct data
    - **Validates: Requirements 5.6**

- [x] 9. Checkpoint — Verify admission control and cache integration
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. SparseLongBlockTable aggressive concurrency tests (now exercises full stack including admission control)
  - [x] 10.1 Write concurrency test: concurrent put/get safety (Property 8)
    - **Property 8: Concurrent put/get safety**
    - N writer threads + M reader threads on overlapping blockId ranges; readers never see corrupted references
    - Use `CountDownLatch` + `ExecutorService` with 32+ threads
    - **Validates: Requirements 3.15a**

  - [x] 10.2 Write concurrency test: concurrent put/remove safety (Property 9)
    - **Property 9: Concurrent put/remove safety**
    - N writers + M removers on same blockIds; after completion, every slot is null or valid
    - **Validates: Requirements 3.12, 3.15b**

  - [x] 10.3 Write concurrency test: concurrent clear safety (Property 10)
    - **Property 10: Concurrent clear safety**
    - One thread calls `clear()` while N readers + M writers operate; no exceptions thrown
    - **Validates: Requirements 3.13, 3.15c**

  - [x] 10.4 Write concurrency test: concurrent chunk allocation (Property 11)
    - **Property 11: Concurrent chunk allocation — no lost writes**
    - N threads trigger chunk allocation for same outer index; exactly one chunk allocated, all subsequent ops succeed
    - **Validates: Requirements 3.15d**

  - [x] 10.5 Write concurrency test: concurrent directory growth (Property 12)
    - **Property 12: Concurrent directory growth — no lost chunks**
    - N threads trigger directory growth beyond initial capacity; directory grows correctly
    - **Validates: Requirements 3.15e**

  - [x] 10.6 Write concurrency test: sustained stress (Property 13)
    - **Property 13: Sustained concurrency stress — no corruption**
    - 32+ threads doing put/get/remove for 10 seconds; no exceptions, no corrupted references
    - **Validates: Requirements 3.15f**

- [x] 11. Checkpoint — Verify SparseLongBlockTable concurrency
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Update StaticConfigs and DirectByteBufferIndexInput, then early integration test
  - [x] 12.1 Update StaticConfigs default block size to 4KB
    - Change `CACHE_BLOCK_SIZE_POWER` default from 15 (32KB) to 12 (4KB) in `StaticConfigs.java`
    - Verify `CACHE_BLOCK_SIZE` = 4096, `CACHE_BLOCK_MASK` = 4095
    - _Requirements: 10.1, 10.4_

  - [x] 12.2 Update `DirectByteBufferIndexInput` to construct `BlockCacheKeyV2(vfd, blockId)` instead of `BlockCacheKeyV2(vfd, blockOffset)`
    - In `loadFromCaffeine()`, change `new BlockCacheKeyV2(vfd, blockOffset)` to `new BlockCacheKeyV2(vfd, blockId)`
    - Verify close semantics: parent close calls `blockTable.clear()`, `blockCache.deregisterTable(vfd)`, `vfdRegistry.deregister(vfd)`
    - Verify clone/slice close only sets `currentSegment = null`
    - Verify idempotent close (already implemented — `if (!isOpen) return`)
    - _Requirements: 6.1, 6.2, 6.3, 6.5, 6.6, 7.1, 7.2, 7.3, 7.4, 7.5, 9.1, 9.2, 9.3_

  - [x] 12.3 Write early integration test: end-to-end read path smoke test
    - Create a small temp file on disk (e.g., 16KB — spans 4 blocks at 4KB)
    - Open via DirectBufferPoolDirectory → DirectByteBufferIndexInput
    - Read all bytes sequentially, verify against direct disk read
    - Read random positions, verify correctness
    - Clone the input, read from clone, close clone, verify parent still works
    - Close parent, verify VFD deregistered and table cleared
    - This catches subtle wiring bugs (blockId vs blockOffset, key construction, eviction listener hookup) immediately after the IndexInput change, rather than waiting until the end.
    - _Requirements: 6.1, 6.2, 6.3, 6.5, 7.1, 7.2, 7.3, 7.4_

- [x] 13. Checkpoint — Verify IndexInput changes and early integration
  - Ensure all tests pass, ask the user if questions arise.

- [x] 14. IndexInput property tests
  - [x]* 14.1 Write property test: sequential read correctness (Property 18)
    - **Property 18: Sequential read correctness across block boundaries**
    - For any file size, `readByte()` loop produces same bytes as direct disk read; block boundary crossings handled transparently
    - **Validates: Requirements 6.1, 6.2, 6.3**

  - [x]* 14.2 Write property test: random access read correctness (Property 19)
    - **Property 19: Random access read correctness**
    - For any valid position, `readByte(pos)` returns same byte as disk; cache misses trigger loads and populate table
    - **Validates: Requirements 6.5, 6.6, 9.3**

  - [x]* 14.3 Write property test: clone close does not affect parent (Property 20)
    - **Property 20: Clone close does not affect parent**
    - Closing a clone sets its `currentSegment` to null but parent continues reading correctly
    - **Validates: Requirements 7.3**

  - [x]* 14.4 Write property test: idempotent close (Property 21)
    - **Property 21: Idempotent close**
    - Calling `close()` multiple times on parent, clone, or slice does not throw
    - **Validates: Requirements 7.4**

- [x] 15. Checkpoint — Verify IndexInput property tests
  - Ensure all tests pass, ask the user if questions arise.

- [x] 16. Admission control stress tests with constrained direct memory
  - [x]* 16.1 Write stress test: single-thread churn under 32MB limit (Property 25, part 1)
    - **Property 25: No OOM under admission-controlled churn**
    - Single thread churns 256MB through 4MB cache with `-XX:MaxDirectMemorySize=32m`; no `OutOfMemoryError`
    - Configure JVM flag in `build.gradle` test task for this test class
    - **Validates: Requirements 11.5, 12.1**

  - [x]* 16.2 Write stress test: multi-thread churn under 32MB limit (Property 25, part 2)
    - **Property 25: No OOM under admission-controlled churn (multi-thread)**
    - 8 threads churn 512MB total through 4MB cache with `-XX:MaxDirectMemorySize=32m`; no `OutOfMemoryError`
    - **Validates: Requirements 12.2**

  - [x]* 16.3 Write test documenting failure mode without admission control
    - Document `OutOfMemoryError` or elevated peak memory when admission control is disabled
    - _Requirements: 12.3, 12.4_

- [x] 17. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- All code is Java 21 with `--enable-preview` for MemorySegment/VarHandle APIs
- Property-based tests use jqwik (`net.jqwik:jqwik:1.9.+`) with JUnit 5
- Concurrency tests (task 10) are NOT optional — the user explicitly requested aggressive concurrency testing for SparseLongBlockTable
- Admission control stress tests (task 16) require `-XX:MaxDirectMemorySize=32m` JVM flag
- ByteBuffers are never reused — GC collects after eviction
- SparseLongBlockTable is a read-side cache; CaffeineCache is the source of truth
- BlockCacheKeyV2 stores `blockId` (pre-shifted), not `blockOffset`
- hashCode is `vfd * 31 ^ Long.hashCode(blockId)`
- No Cleaner needed for clone/slice lifecycle — Lucene guarantees clones don't outlive parent
- VirtualFileDescriptorRegistry uses unique vfd per `openInput()` call, no path dedup
- **Reorder rationale:** Admission controller (task 7) is built before cache integration (task 8) and concurrency stress (task 10), so the full stack — including memory pressure behavior — is exercised during stress testing. This is safer than testing concurrency in isolation and discovering admission-related bugs later.
- **Early integration test rationale:** Task 12.3 runs a smoke test immediately after DirectByteBufferIndexInput changes, catching subtle wiring bugs (blockId vs blockOffset, key construction, eviction listener hookup) before the more exhaustive property tests in task 14.
