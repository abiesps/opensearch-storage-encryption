/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.memory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

/**
 * Property-based tests for {@link AllocatorSettings} validation.
 *
 * <p>Feature: allocator-settings-metrics
 */
class AllocatorSettingsPropertyTests {

    // ======================== Helpers ========================

    /**
     * Attempts to parse a value through a Setting by building a Settings object
     * and calling {@code setting.get(settings)}. Returns normally if accepted,
     * throws if rejected.
     */
    private static <T> T getSettingValue(Setting<T> setting, String rawValue) {
        Settings settings = Settings.builder()
            .put(setting.getKey(), rawValue)
            .build();
        return setting.get(settings);
    }

    // ======================== Property 1: Power-of-two validation ========================

    /**
     * Property 1: Power-of-two setting validation.
     *
     * <p>For any positive power of two, {@code sample_interval} accepts the value.
     *
     * <p><b>Validates: Requirements 1.2</b>
     */
    @Property(tries = 100)
    void sampleIntervalAcceptsPositivePowersOfTwo(
        @ForAll @IntRange(min = 0, max = 30) int exponent
    ) {
        int powerOfTwo = 1 << exponent;
        assertDoesNotThrow(
            () -> getSettingValue(AllocatorSettings.SAMPLE_INTERVAL, Integer.toString(powerOfTwo)),
            "sample_interval should accept " + powerOfTwo + " (2^" + exponent + ")"
        );
    }

    /**
     * Property 1: Power-of-two setting validation (rejection).
     *
     * <p>For any integer that is not a positive power of two,
     * {@code sample_interval} rejects the value.
     *
     * <p><b>Validates: Requirements 1.2</b>
     */
    @Property(tries = 100)
    void sampleIntervalRejectsNonPowersOfTwo(
        @ForAll @IntRange(min = -1000, max = 100_000) int value
    ) {
        Assume.that(value <= 0 || Integer.bitCount(value) != 1);
        assertThrows(
            IllegalArgumentException.class,
            () -> getSettingValue(AllocatorSettings.SAMPLE_INTERVAL, Integer.toString(value)),
            "sample_interval should reject " + value
        );
    }

    // ======================== Property 2: Open-interval (0,1) validation ========================

    /**
     * Property 2: Open-interval (0,1) setting validation (acceptance).
     *
     * <p>For any double strictly inside (0, 1), the four open-interval settings accept the value.
     *
     * <p><b>Validates: Requirements 1.3, 1.5, 1.9, 1.10</b>
     */
    @Property(tries = 100)
    void openIntervalSettingsAcceptValuesInRange(
        @ForAll @DoubleRange(min = 0.001, max = 0.999) double value
    ) {
        String raw = Double.toString(value);
        for (Setting<Double> setting : openIntervalSettings()) {
            assertDoesNotThrow(
                () -> getSettingValue(setting, raw),
                setting.getKey() + " should accept " + value
            );
        }
    }

    /**
     * Property 2: Open-interval (0,1) setting validation (rejection of boundaries and outside).
     *
     * <p>For any double &le; 0 or &ge; 1, the four open-interval settings reject the value.
     *
     * <p><b>Validates: Requirements 1.3, 1.5, 1.9, 1.10</b>
     */
    @Property(tries = 100)
    void openIntervalSettingsRejectOutOfRange(
        @ForAll @DoubleRange(min = -100.0, max = 100.0) double value
    ) {
        Assume.that(value <= 0.0 || value >= 1.0);
        Assume.that(!Double.isNaN(value));
        String raw = Double.toString(value);
        for (Setting<Double> setting : openIntervalSettings()) {
            assertThrows(
                IllegalArgumentException.class,
                () -> getSettingValue(setting, raw),
                setting.getKey() + " should reject " + value
            );
        }
    }

    @SuppressWarnings("unchecked")
    private static Setting<Double>[] openIntervalSettings() {
        return new Setting[] {
            AllocatorSettings.EMA_ALPHA,
            AllocatorSettings.MIN_CACHE_FRACTION,
            AllocatorSettings.MIN_HEADROOM_FRACTION,
            AllocatorSettings.STALL_RATIO_THRESHOLD
        };
    }

    // ======================== Property 3: Lower-bounded validation ========================

    /**
     * Property 3: Lower-bounded setting validation — safety_multiplier accepts values &ge; 1.0.
     *
     * <p><b>Validates: Requirements 1.4</b>
     */
    @Property(tries = 100)
    void safetyMultiplierAcceptsValuesAtOrAboveBound(
        @ForAll @DoubleRange(min = 1.0, max = 1000.0) double value
    ) {
        assertDoesNotThrow(
            () -> getSettingValue(AllocatorSettings.SAFETY_MULTIPLIER, Double.toString(value)),
            "safety_multiplier should accept " + value
        );
    }

    /**
     * Property 3: Lower-bounded setting validation — safety_multiplier rejects values &lt; 1.0.
     *
     * <p><b>Validates: Requirements 1.4</b>
     */
    @Property(tries = 100)
    void safetyMultiplierRejectsBelowBound(
        @ForAll @DoubleRange(min = -100.0, max = 100.0) double value
    ) {
        Assume.that(value < 1.0);
        Assume.that(!Double.isNaN(value));
        assertThrows(
            IllegalArgumentException.class,
            () -> getSettingValue(AllocatorSettings.SAFETY_MULTIPLIER, Double.toString(value)),
            "safety_multiplier should reject " + value
        );
    }

    /**
     * Property 3: Lower-bounded setting validation — non-negative ms settings accept values &ge; 0.
     *
     * <p><b>Validates: Requirements 1.6, 1.7, 1.8</b>
     */
    @Property(tries = 100)
    void nonNegativeMsSettingsAcceptNonNegative(
        @ForAll @LongRange(min = 0, max = 100_000) long value
    ) {
        String raw = Long.toString(value);
        for (Setting<Long> setting : nonNegativeMsSettings()) {
            assertDoesNotThrow(
                () -> getSettingValue(setting, raw),
                setting.getKey() + " should accept " + value
            );
        }
    }

    /**
     * Property 3: Lower-bounded setting validation — non-negative ms settings reject negatives.
     *
     * <p><b>Validates: Requirements 1.6, 1.7, 1.8</b>
     */
    @Property(tries = 100)
    void nonNegativeMsSettingsRejectNegative(
        @ForAll @LongRange(min = -100_000, max = -1) long value
    ) {
        String raw = Long.toString(value);
        for (Setting<Long> setting : nonNegativeMsSettings()) {
            assertThrows(
                IllegalArgumentException.class,
                () -> getSettingValue(setting, raw),
                setting.getKey() + " should reject " + value
            );
        }
    }

    @SuppressWarnings("unchecked")
    private static Setting<Long>[] nonNegativeMsSettings() {
        return new Setting[] {
            AllocatorSettings.GC_HINT_COOLDOWN_MS,
            AllocatorSettings.SHRINK_COOLDOWN_MS,
            AllocatorSettings.MAX_SAMPLE_INTERVAL_MS
        };
    }
}
