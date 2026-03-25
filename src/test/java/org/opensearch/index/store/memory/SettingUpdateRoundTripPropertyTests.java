/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

/**
 * Property-based tests for setting update round-trip on {@link DirectMemoryAllocator}.
 *
 * <p>Feature: allocator-settings-metrics, Property 4: Setting update round-trip
 *
 * <p>For any valid setting value, writing via setter and reading via the volatile
 * field getter returns the same value (with ms→ns conversion for cooldown fields).
 *
 * <p><b>Validates: Requirements 2.2</b>
 */
class SettingUpdateRoundTripPropertyTests {

    private DirectMemoryAllocator createAllocator() {
        return new DirectMemoryAllocator(1L << 30, 8192);
    }

    @Property(tries = 100)
    void sampleIntervalRoundTrip(@ForAll @IntRange(min = 0, max = 20) int exponent) {
        int value = 1 << exponent;
        DirectMemoryAllocator alloc = createAllocator();
        alloc.setSampleInterval(value);
        assertEquals(value, alloc.getVSampleInterval(),
            "vSampleInterval should equal the set value");
    }

    @Property(tries = 100)
    void emaAlphaRoundTrip(@ForAll @DoubleRange(min = 0.001, max = 0.999) double value) {
        DirectMemoryAllocator alloc = createAllocator();
        alloc.setEmaAlpha(value);
        assertEquals(value, alloc.getVEmaAlpha(), 1e-15,
            "vEmaAlpha should equal the set value");
    }

    @Property(tries = 100)
    void safetyMultiplierRoundTrip(@ForAll @DoubleRange(min = 1.0, max = 100.0) double value) {
        DirectMemoryAllocator alloc = createAllocator();
        alloc.setSafetyMultiplier(value);
        assertEquals(value, alloc.getVSafetyMultiplier(), 1e-15,
            "vSafetyMultiplier should equal the set value");
    }

    @Property(tries = 100)
    void minCacheFractionRoundTrip(@ForAll @DoubleRange(min = 0.001, max = 0.999) double value) {
        DirectMemoryAllocator alloc = createAllocator();
        alloc.setMinCacheFraction(value);
        assertEquals(value, alloc.getVMinCacheFraction(), 1e-15,
            "vMinCacheFraction should equal the set value");
    }

    @Property(tries = 100)
    void gcHintCooldownMsRoundTrip(@ForAll @LongRange(min = 0, max = 10_000) long valueMs) {
        DirectMemoryAllocator alloc = createAllocator();
        alloc.setGcHintCooldownMs(valueMs);
        assertEquals(valueMs * 1_000_000L, alloc.getVGcHintCooldownNanos(),
            "vGcHintCooldownNanos should equal valueMs * 1_000_000");
    }

    @Property(tries = 100)
    void shrinkCooldownMsRoundTrip(@ForAll @LongRange(min = 0, max = 10_000) long valueMs) {
        DirectMemoryAllocator alloc = createAllocator();
        alloc.setShrinkCooldownMs(valueMs);
        assertEquals(valueMs * 1_000_000L, alloc.getVShrinkCooldownNanos(),
            "vShrinkCooldownNanos should equal valueMs * 1_000_000");
    }

    @Property(tries = 100)
    void maxSampleIntervalMsRoundTrip(@ForAll @LongRange(min = 0, max = 10_000) long valueMs) {
        DirectMemoryAllocator alloc = createAllocator();
        alloc.setMaxSampleIntervalMs(valueMs);
        assertEquals(valueMs * 1_000_000L, alloc.getVMaxSampleIntervalNanos(),
            "vMaxSampleIntervalNanos should equal valueMs * 1_000_000");
    }

    @Property(tries = 100)
    void minHeadroomFractionRoundTrip(@ForAll @DoubleRange(min = 0.001, max = 0.999) double value) {
        DirectMemoryAllocator alloc = createAllocator();
        alloc.setMinHeadroomFraction(value);
        assertEquals(value, alloc.getVMinHeadroomFraction(), 1e-15,
            "vMinHeadroomFraction should equal the set value");
    }

    @Property(tries = 100)
    void stallRatioThresholdRoundTrip(@ForAll @DoubleRange(min = 0.001, max = 0.999) double value) {
        DirectMemoryAllocator alloc = createAllocator();
        alloc.setStallRatioThreshold(value);
        assertEquals(value, alloc.getVStallRatioThreshold(), 1e-15,
            "vStallRatioThreshold should equal the set value");
    }
}
