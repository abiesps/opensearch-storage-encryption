/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.memory;

import java.util.List;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;

/**
 * Defines the 9 dynamic cluster settings for {@link DirectMemoryAllocator}.
 *
 * <p>All settings have {@link Property#NodeScope} and {@link Property#Dynamic},
 * allowing operators to tune allocator behaviour at runtime without node restarts.
 *
 * <p>Validation constraints are enforced at the {@code Setting} layer:
 * <ul>
 *   <li>{@code sample_interval}: positive power of two</li>
 *   <li>{@code ema_alpha}, {@code min_cache_fraction}, {@code min_headroom_fraction},
 *       {@code stall_ratio_threshold}: open interval (0, 1)</li>
 *   <li>{@code safety_multiplier}: &ge; 1.0</li>
 *   <li>{@code gc_hint_cooldown_ms}, {@code shrink_cooldown_ms},
 *       {@code max_sample_interval_ms}: &ge; 0</li>
 * </ul>
 */
public final class AllocatorSettings {

    private AllocatorSettings() {}

    private static final String PREFIX = "node.store.memory.";

    // ======================== Validators ========================

    /** Validator: positive power of two. */
    private static final Setting.Validator<Integer> POWER_OF_TWO_VALIDATOR = value -> {
        if (value == null || value <= 0 || Integer.bitCount(value) != 1) {
            throw new IllegalArgumentException(
                "sample_interval must be a positive power of two, got: " + value
            );
        }
    };

    /** Validator: open interval (0, 1) exclusive. */
    private static final Setting.Validator<Double> OPEN_UNIT_INTERVAL_VALIDATOR = value -> {
        if (value == null || value <= 0.0 || value >= 1.0) {
            throw new IllegalArgumentException(
                "value must be in the open interval (0, 1), got: " + value
            );
        }
    };

    // ======================== Setting definitions ========================

    /**
     * Allocations between diagnostic samples. Must be a positive power of two.
     */
    public static final Setting<Integer> SAMPLE_INTERVAL = Setting.intSetting(
        PREFIX + "sample_interval",
        DirectMemoryAllocator.DEFAULT_SAMPLE_INTERVAL,
        1,
        POWER_OF_TWO_VALIDATOR,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * EMA smoothing factor for GC-debt tracking. Open interval (0, 1).
     */
    public static final Setting<Double> EMA_ALPHA = Setting.doubleSetting(
        PREFIX + "ema_alpha",
        DirectMemoryAllocator.DEFAULT_EMA_ALPHA,
        OPEN_UNIT_INTERVAL_VALIDATOR,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Multiplier on GC_Debt_EMA for target headroom calculation. Must be &ge; 1.0.
     */
    public static final Setting<Double> SAFETY_MULTIPLIER = Setting.doubleSetting(
        PREFIX + "safety_multiplier",
        DirectMemoryAllocator.DEFAULT_SAFETY_MULTIPLIER,
        1.0,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Minimum cache capacity as a fraction of original. Open interval (0, 1).
     */
    public static final Setting<Double> MIN_CACHE_FRACTION = Setting.doubleSetting(
        PREFIX + "min_cache_fraction",
        DirectMemoryAllocator.DEFAULT_MIN_CACHE_FRACTION,
        OPEN_UNIT_INTERVAL_VALIDATOR,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Minimum milliseconds between System.gc() hints. Must be &ge; 0.
     */
    public static final Setting<Long> GC_HINT_COOLDOWN_MS = Setting.longSetting(
        PREFIX + "gc_hint_cooldown_ms",
        DirectMemoryAllocator.DEFAULT_GC_HINT_COOLDOWN_MS,
        0L,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Minimum milliseconds between cache shrink operations. Must be &ge; 0.
     */
    public static final Setting<Long> SHRINK_COOLDOWN_MS = Setting.longSetting(
        PREFIX + "shrink_cooldown_ms",
        DirectMemoryAllocator.DEFAULT_SHRINK_COOLDOWN_MS,
        0L,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Maximum wall-clock milliseconds between diagnostic samples. Must be &ge; 0.
     */
    public static final Setting<Long> MAX_SAMPLE_INTERVAL_MS = Setting.longSetting(
        PREFIX + "max_sample_interval_ms",
        DirectMemoryAllocator.DEFAULT_MAX_SAMPLE_INTERVAL_MS,
        0L,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Minimum headroom as a fraction of maxDirectMemoryBytes. Open interval (0, 1).
     */
    public static final Setting<Double> MIN_HEADROOM_FRACTION = Setting.doubleSetting(
        PREFIX + "min_headroom_fraction",
        DirectMemoryAllocator.DEFAULT_MIN_HEADROOM_FRACTION,
        OPEN_UNIT_INTERVAL_VALIDATOR,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Stall ratio threshold for MXBean override. Open interval (0, 1).
     */
    public static final Setting<Double> STALL_RATIO_THRESHOLD = Setting.doubleSetting(
        PREFIX + "stall_ratio_threshold",
        DirectMemoryAllocator.DEFAULT_STALL_RATIO_THRESHOLD,
        OPEN_UNIT_INTERVAL_VALIDATOR,
        Property.NodeScope,
        Property.Dynamic
    );

    // ======================== Convenience list ========================

    /** All 9 allocator settings for convenient registration in {@code getSettings()}. */
    public static final List<Setting<?>> ALL = List.of(
        SAMPLE_INTERVAL,
        EMA_ALPHA,
        SAFETY_MULTIPLIER,
        MIN_CACHE_FRACTION,
        GC_HINT_COOLDOWN_MS,
        SHRINK_COOLDOWN_MS,
        MAX_SAMPLE_INTERVAL_MS,
        MIN_HEADROOM_FRACTION,
        STALL_RATIO_THRESHOLD
    );
}
