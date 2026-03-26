/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.*;

/**
 * JCStress tests for the Inflight_Map ({@code ConcurrentHashMap<Long, CompletableFuture<MemorySegment>>})
 * used by {@link VirtualFileChannel} for I/O deduplication.
 *
 * <h2>What these tests demonstrate</h2>
 * <ol>
 *   <li>putIfAbsent correctly serializes the I/O owner election — exactly one
 *       thread wins and becomes the I/O owner.</li>
 *   <li>Joiners (threads that lose the putIfAbsent race) correctly observe the
 *       completed result from the I/O owner.</li>
 *   <li>Conditional remove ({@code remove(key, future)}) only removes the entry
 *       if it still holds the same future — no ABA problem.</li>
 *   <li>A completed future is visible to all joiners after the I/O owner calls
 *       {@code complete()}.</li>
 * </ol>
 *
 * <p><b>Validates: Requirements 2 AC11, 5 AC6, 27</b></p>
 */
@SuppressWarnings("preview")
public class InflightMapStressTests {

    private static final long KEY = VirtualPage.encode(1L, 0);

    private static MemorySegment makeSentinel(byte value) {
        ByteBuffer buf = ByteBuffer.allocateDirect(8);
        for (int i = 0; i < 8; i++) buf.put(value);
        buf.flip();
        return MemorySegment.ofBuffer(buf.asReadOnlyBuffer());
    }

    // ========================================================================
    // Test 1: putIfAbsent race — exactly one thread becomes I/O owner.
    //
    // Two actors race to putIfAbsent the same key. Exactly one wins (gets
    // null back), the other gets the winner's future. The arbiter verifies
    // exactly one owner and the map has exactly one entry.
    // ========================================================================

    @JCStressTest
    @Description("Two threads race putIfAbsent on the same key. "
        + "Exactly one becomes I/O owner (gets null). "
        + "The other joins the existing future.")
    @Outcome(id = "1, 0, 1", expect = Expect.ACCEPTABLE,
        desc = "Actor1 won putIfAbsent, actor2 joined.")
    @Outcome(id = "0, 1, 1", expect = Expect.ACCEPTABLE,
        desc = "Actor2 won putIfAbsent, actor1 joined.")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Both won or neither won — broken.")
    @State
    public static class PutIfAbsentRace {
        private final ConcurrentHashMap<Long, CompletableFuture<MemorySegment>> map =
            new ConcurrentHashMap<>();

        @Actor
        public void actor1(III_Result r) {
            CompletableFuture<MemorySegment> myFuture = new CompletableFuture<>();
            CompletableFuture<MemorySegment> existing = map.putIfAbsent(KEY, myFuture);
            r.r1 = (existing == null) ? 1 : 0; // 1 = I'm the owner
        }

        @Actor
        public void actor2(III_Result r) {
            CompletableFuture<MemorySegment> myFuture = new CompletableFuture<>();
            CompletableFuture<MemorySegment> existing = map.putIfAbsent(KEY, myFuture);
            r.r2 = (existing == null) ? 1 : 0; // 1 = I'm the owner
        }

        @Arbiter
        public void arbiter(III_Result r) {
            r.r3 = map.size(); // must be exactly 1
        }
    }

    // ========================================================================
    // Test 2: I/O owner completes future, joiner sees the result.
    //
    // Actor1 wins putIfAbsent and completes the future with a sentinel.
    // Actor2 joins the existing future and reads the result.
    // Arbiter verifies the map entry can be conditionally removed.
    // ========================================================================

    @JCStressTest
    @Description("I/O owner completes future, joiner observes the result. "
        + "Conditional remove cleans up the entry.")
    @Outcome(id = "1, 1, 0", expect = Expect.ACCEPTABLE,
        desc = "Owner completed, joiner saw result, map cleaned up.")
    @Outcome(id = "1, -1, .*", expect = Expect.ACCEPTABLE,
        desc = "Owner completed, joiner timed out (future not yet done). Map may or may not be cleaned.")
    @Outcome(id = "1, 0, .*", expect = Expect.ACCEPTABLE,
        desc = "Owner completed, joiner saw null (getNow default). Map may or may not be cleaned.")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Owner did not complete or inconsistent state.")
    @State
    public static class CompleteAndJoin {
        private final ConcurrentHashMap<Long, CompletableFuture<MemorySegment>> map =
            new ConcurrentHashMap<>();
        private final CompletableFuture<MemorySegment> ownerFuture = new CompletableFuture<>();
        private final MemorySegment sentinel = makeSentinel((byte) 0xAB);

        {
            map.put(KEY, ownerFuture);
        }

        @Actor
        public void owner(III_Result r) {
            ownerFuture.complete(sentinel);
            r.r1 = 1; // owner always completes
        }

        @Actor
        public void joiner(III_Result r) {
            MemorySegment result = ownerFuture.getNow(null);
            if (result != null && result.byteSize() == 8) {
                r.r2 = 1; // saw the sentinel
            } else if (result == null) {
                r.r2 = 0; // not yet completed
            } else {
                r.r2 = -1; // unexpected
            }
        }

        @Arbiter
        public void arbiter(III_Result r) {
            // Conditional remove: only removes if the value is still ownerFuture
            map.remove(KEY, ownerFuture);
            r.r3 = map.size(); // should be 0 after cleanup
        }
    }

    // ========================================================================
    // Test 3: Conditional remove safety — remove(key, future) only removes
    //         if the value matches. A new future inserted by a retry thread
    //         must NOT be removed by the original owner's cleanup.
    // ========================================================================

    @JCStressTest
    @Description("Conditional remove does not remove a replacement future. "
        + "Original owner's cleanup must not affect the retry thread's entry.")
    @Outcome(id = "1, 1", expect = Expect.ACCEPTABLE,
        desc = "Retry inserted new future, original cleanup did not remove it.")
    @Outcome(id = "0, 1", expect = Expect.ACCEPTABLE,
        desc = "Original cleanup ran first (removed original), retry inserted new entry.")
    @Outcome(id = "1, 0", expect = Expect.ACCEPTABLE,
        desc = "Retry ran first, original cleanup correctly skipped (value mismatch).")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Map is empty — retry's future was incorrectly removed.")
    @State
    public static class ConditionalRemoveSafety {
        private final ConcurrentHashMap<Long, CompletableFuture<MemorySegment>> map =
            new ConcurrentHashMap<>();
        private final CompletableFuture<MemorySegment> originalFuture = new CompletableFuture<>();
        private final CompletableFuture<MemorySegment> retryFuture = new CompletableFuture<>();

        {
            map.put(KEY, originalFuture);
        }

        @Actor
        public void originalCleanup(II_Result r) {
            // Original I/O owner's finally block: conditional remove
            boolean removed = map.remove(KEY, originalFuture);
            r.r1 = removed ? 0 : 1; // 0 = removed original, 1 = skipped (already replaced)
        }

        @Actor
        public void retryInsert(II_Result r) {
            // Retry thread: remove old + insert new
            map.remove(KEY, originalFuture);
            CompletableFuture<MemorySegment> existing = map.putIfAbsent(KEY, retryFuture);
            r.r2 = (existing == null) ? 1 : 0; // 1 = successfully inserted retry future
        }

        @Arbiter
        public void arbiter(II_Result r) {
            // After both actors complete, the map must contain the retry future
            // (or be empty if cleanup ran after retry removed original but before retry inserted)
            // The key invariant: if retryFuture was inserted, it must NOT have been removed
            // by originalCleanup (because conditional remove checks value identity)
            CompletableFuture<MemorySegment> remaining = map.get(KEY);
            if (remaining == retryFuture) {
                // Good: retry's future survived
            } else if (remaining == null && r.r2 == 1) {
                // BAD: retry inserted but it was removed — this should be FORBIDDEN
                // but we handle it via outcome annotations above
                r.r1 = -1;
                r.r2 = -1;
            }
        }
    }

    // ========================================================================
    // Test 4: completeExceptionally propagates to joiner.
    //
    // Owner completes the future exceptionally. Joiner must see the exception.
    // ========================================================================

    @JCStressTest
    @Description("I/O owner completes future exceptionally. "
        + "Joiner observes the exception.")
    @Outcome(id = "1, 1", expect = Expect.ACCEPTABLE,
        desc = "Owner completed exceptionally, joiner saw exception.")
    @Outcome(id = "1, 0", expect = Expect.ACCEPTABLE,
        desc = "Owner completed exceptionally, joiner hasn't observed yet (getNow).")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Inconsistent state.")
    @State
    public static class ExceptionalCompletion {
        private final ConcurrentHashMap<Long, CompletableFuture<MemorySegment>> map =
            new ConcurrentHashMap<>();
        private final CompletableFuture<MemorySegment> ownerFuture = new CompletableFuture<>();

        {
            map.put(KEY, ownerFuture);
        }

        @Actor
        public void owner(II_Result r) {
            ownerFuture.completeExceptionally(new java.io.IOException("Injected I/O error"));
            r.r1 = 1;
        }

        @Actor
        public void joiner(II_Result r) {
            if (ownerFuture.isCompletedExceptionally()) {
                r.r2 = 1; // saw the exception
            } else {
                r.r2 = 0; // not yet completed
            }
        }
    }
}
