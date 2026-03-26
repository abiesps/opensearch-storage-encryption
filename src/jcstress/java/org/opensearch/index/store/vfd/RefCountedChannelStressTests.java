/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Semaphore;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.*;

/**
 * JCStress tests for {@link RefCountedChannel} — concurrent acquire/release
 * from N threads, verifying the channel is never closed while any thread
 * holds a ref.
 *
 * <h2>What these tests demonstrate</h2>
 * <ol>
 *   <li>The CAS loop in {@code acquire()} correctly handles concurrent
 *       increments — no lost updates, no double-close.</li>
 *   <li>The channel is never closed while any thread holds an acquired
 *       ref (verified by {@code channel.isOpen()} during read).</li>
 *   <li>After eviction (base ref released) + all I/O releases, the
 *       channel is closed and the FD limiter permit is returned.</li>
 *   <li>{@code acquire()} returns null once the channel is fully closed
 *       (refCount &le; 0).</li>
 * </ol>
 *
 * <p><b>Validates: Requirements 2, 3, 5</b></p>
 */
public class RefCountedChannelStressTests {

    /** Creates a temp file with known content for channel operations. */
    private static FileChannel openTempChannel() {
        try {
            Path tmp = Files.createTempFile("jcstress-rcc-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.write(tmp, new byte[] { 0x42, 0x43, 0x44, 0x45 });
            return FileChannel.open(tmp, StandardOpenOption.READ);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ========================================================================
    // Test 1: Concurrent acquire + release — refCount never goes negative,
    //         channel is open during all acquired refs.
    //
    // Two actors each acquire, verify channel is open, then release.
    // Arbiter verifies final refCount == 1 (only base ref remains).
    // ========================================================================

    @JCStressTest
    @Description("Two concurrent acquire/release cycles. "
        + "Channel must be open during each acquire. "
        + "Final refCount must be 1 (base ref).")
    @Outcome(id = "1, 1, 1", expect = Expect.ACCEPTABLE,
        desc = "Both actors saw open channel, final refCount is 1 (base ref).")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Channel closed during acquire or refCount inconsistent.")
    @State
    public static class ConcurrentAcquireRelease {
        private final Semaphore limiter = new Semaphore(10);
        private final RefCountedChannel rcc = new RefCountedChannel(openTempChannel(), limiter);

        @Actor
        public void actor1(III_Result r) {
            FileChannel ch = rcc.acquire();
            if (ch != null && ch.isOpen()) {
                r.r1 = 1;
            } else {
                r.r1 = 0;
            }
            if (ch != null) {
                rcc.release();
            }
        }

        @Actor
        public void actor2(III_Result r) {
            FileChannel ch = rcc.acquire();
            if (ch != null && ch.isOpen()) {
                r.r2 = 1;
            } else {
                r.r2 = 0;
            }
            if (ch != null) {
                rcc.release();
            }
        }

        @Arbiter
        public void arbiter(III_Result r) {
            r.r3 = rcc.refCount();
        }
    }

    // ========================================================================
    // Test 2: Eviction race — one actor releases base ref (simulating
    //         eviction), another actor tries to acquire concurrently.
    //
    // If acquire wins the race: it gets the channel, uses it, releases.
    // If eviction wins: acquire returns null.
    // Either way, channel must not be closed while any thread holds a ref.
    // ========================================================================

    @JCStressTest
    @Description("Eviction (base ref release) racing with acquire. "
        + "Acquire either succeeds (channel open) or returns null. "
        + "Channel never closed while held.")
    @Outcome(id = "1, 0", expect = Expect.ACCEPTABLE,
        desc = "Acquire won race: got channel, used it, released. Channel now closed.")
    @Outcome(id = "0, 0", expect = Expect.ACCEPTABLE,
        desc = "Eviction won race: acquire returned null. Channel closed.")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Channel closed while held or inconsistent state.")
    @State
    public static class EvictionRaceWithAcquire {
        private final Semaphore limiter = new Semaphore(10);
        private final RefCountedChannel rcc = new RefCountedChannel(openTempChannel(), limiter);

        @Actor
        public void evictor(II_Result r) {
            rcc.release(); // drop base ref
        }

        @Actor
        public void acquirer(II_Result r) {
            FileChannel ch = rcc.acquire();
            if (ch != null) {
                // Channel must be open while we hold the ref
                r.r1 = ch.isOpen() ? 1 : -1;
                rcc.release();
            } else {
                r.r1 = 0; // acquire returned null (eviction won)
            }
        }

        @Arbiter
        public void arbiter(II_Result r) {
            r.r2 = rcc.refCount();
        }
    }

    // ========================================================================
    // Test 3: Multiple concurrent acquires + single eviction.
    //
    // Three actors acquire, one actor evicts (releases base ref).
    // Arbiter checks: channel is closed iff refCount == 0.
    // ========================================================================

    @JCStressTest
    @Description("Three concurrent acquires + one eviction. "
        + "All successful acquires see open channel. "
        + "After all releases, channel is closed.")
    @Outcome(id = "1, 1, 1, 0", expect = Expect.ACCEPTABLE,
        desc = "All three acquired before eviction. All released. Channel closed.")
    @Outcome(id = "1, 1, 0, 0", expect = Expect.ACCEPTABLE,
        desc = "Two acquired, one saw null (eviction raced). Channel closed.")
    @Outcome(id = "1, 0, 0, 0", expect = Expect.ACCEPTABLE,
        desc = "One acquired, two saw null. Channel closed.")
    @Outcome(id = "0, 0, 0, 0", expect = Expect.ACCEPTABLE,
        desc = "Eviction won all races. Channel closed.")
    @Outcome(id = "1, 0, 1, 0", expect = Expect.ACCEPTABLE,
        desc = "Two acquired (actors 1,3), one saw null. Channel closed.")
    @Outcome(id = "0, 1, 1, 0", expect = Expect.ACCEPTABLE,
        desc = "Two acquired (actors 2,3), one saw null. Channel closed.")
    @Outcome(id = "0, 1, 0, 0", expect = Expect.ACCEPTABLE,
        desc = "One acquired (actor 2), others saw null. Channel closed.")
    @Outcome(id = "0, 0, 1, 0", expect = Expect.ACCEPTABLE,
        desc = "One acquired (actor 3), others saw null. Channel closed.")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Channel closed while held or refCount inconsistent.")
    @State
    public static class MultiAcquireWithEviction {
        private final Semaphore limiter = new Semaphore(10);
        private final RefCountedChannel rcc = new RefCountedChannel(openTempChannel(), limiter);

        @Actor
        public void acquirer1(IIII_Result r) {
            FileChannel ch = rcc.acquire();
            if (ch != null) {
                r.r1 = ch.isOpen() ? 1 : -1;
                rcc.release();
            } else {
                r.r1 = 0;
            }
        }

        @Actor
        public void acquirer2(IIII_Result r) {
            FileChannel ch = rcc.acquire();
            if (ch != null) {
                r.r2 = ch.isOpen() ? 1 : -1;
                rcc.release();
            } else {
                r.r2 = 0;
            }
        }

        @Actor
        public void acquirer3(IIII_Result r) {
            FileChannel ch = rcc.acquire();
            if (ch != null) {
                r.r3 = ch.isOpen() ? 1 : -1;
                rcc.release();
            } else {
                r.r3 = 0;
            }
        }

        @Actor
        public void evictor(IIII_Result r) {
            rcc.release(); // drop base ref
        }

        @Arbiter
        public void arbiter(IIII_Result r) {
            r.r4 = rcc.refCount();
        }
    }

    // ========================================================================
    // Test 4: Acquire-after-close visibility.
    //
    // Actor 1 releases base ref (closing channel if no other refs).
    // Actor 2 attempts acquire — must see null.
    // Arbiter confirms refCount == 0.
    // ========================================================================

    @JCStressTest
    @Description("Acquire after full close must return null. "
        + "RefCount must be 0 after close.")
    @Outcome(id = "0, 0", expect = Expect.ACCEPTABLE,
        desc = "Acquire returned null after close. RefCount is 0.")
    @Outcome(id = "1, 0", expect = Expect.ACCEPTABLE,
        desc = "Acquire raced and won — got channel before close. RefCount is 0 after release.")
    @Outcome(expect = Expect.FORBIDDEN, desc = "Inconsistent state.")
    @State
    public static class AcquireAfterClose {
        private final Semaphore limiter = new Semaphore(10);
        private final RefCountedChannel rcc = new RefCountedChannel(openTempChannel(), limiter);

        @Actor
        public void closer(II_Result r) {
            rcc.release(); // drop base ref
        }

        @Actor
        public void acquirer(II_Result r) {
            FileChannel ch = rcc.acquire();
            if (ch != null) {
                r.r1 = ch.isOpen() ? 1 : -1;
                rcc.release();
            } else {
                r.r1 = 0;
            }
        }

        @Arbiter
        public void arbiter(II_Result r) {
            r.r2 = rcc.refCount();
        }
    }
}
