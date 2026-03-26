/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache_v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** MB-6: GC Disabled Mode. Validates: Requirements 11.1, 11.2, 11.3, 11.4 */
class GcDisabledModeTests {
    private static final int BLOCK_SIZE = 8192;
    private static void forceField(DirectMemoryAllocator a, String name, Object val) throws Exception {
        Field f = DirectMemoryAllocator.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(a, val);
    }
    @Test
    void gcDisabledMode_noGcHints_onlyCacheShrinkUsed() throws Exception {
        long maxDirect = DirectMemoryAllocator.resolveMaxDirectMemory();
        DirectMemoryAllocator allocator = new DirectMemoryAllocator(maxDirect, BLOCK_SIZE, 4096, 0.3, 2.0, 0.5, 0, 0, 999999, 0.10, 0.001);
        forceField(allocator, "systemGcDisabled", true);
        assertTrue(allocator.isSystemGcDisabled());
        long origMax = 10000;
        AtomicLong curMax = new AtomicLong(origMax);
        allocator.registerCacheController(new CacheController() {
            public long cacheHeldBytes() { return 0; }
            public void setMaxBlocks(long n) { curMax.set(Math.max(1, n)); }
            public void cleanUp() { }
            public long currentMaxBlocks() { return curMax.get(); }
            public long originalMaxBlocks() { return origMax; }
        });
        List<ByteBuffer> held = new ArrayList<>();
        int count = (int) ((maxDirect * 0.80) / BLOCK_SIZE);
        for (int i = 0; i < count; i++) {
            try { held.add(allocator.allocate(BLOCK_SIZE)); }
            catch (MemoryBackPressureException e) { break; }
        }
        assertTrue(held.size() > 100);
        for (int i = 0; i < 200; i++) { allocator.diagnoseAndRespond(); }
        long gcHints = allocator.getGcHintCount();
        long shrinks = allocator.getCacheShrinkCount();
        long tier3 = allocator.getTier3LastResortCount();
        System.err.println("[MB6] gcHints=" + gcHints + " shrinks=" + shrinks + " tier3=" + tier3 + " curMax=" + curMax.get() + " pressure=" + allocator.getLastPressureLevel() + " headroom=" + allocator.getLastHeadroomBytes() + " target=" + allocator.getTargetHeadroomBytes() + " gcDebtEma=" + allocator.getGcDebtEma() + " deficitEma=" + allocator.getDeficitEma() + " nativeGap=" + allocator.getNativeGap() + " gcDebtGrowth=" + allocator.getGcDebtGrowthRate() + " held=" + held.size());
        assertEquals(0, gcHints, "gcHintCount must be 0");
        assertTrue(allocator.isSystemGcDisabled());
        assertTrue(allocator.getAllocatedByUs() >= 0);
        assertEquals(0, allocator.getGcHintBecauseDebt());
        assertEquals(0, allocator.getGcHintWithShrink());
        if (tier3 > 0) { assertTrue(shrinks > 0 || curMax.get() <= (long)(origMax * 0.5), "Tier3+GCdisabled: shrinks=" + shrinks + " curMax=" + curMax.get()); }
        held.clear();
    }
}
