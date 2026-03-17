/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.store.block_cache_v2;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke test to verify jqwik property-based testing infrastructure works.
 */
class JqwikSmokeTests {

    @Property(tries = 10)
    void absoluteValueIsNonNegative(@ForAll @IntRange(min = 0, max = Integer.MAX_VALUE) int n) {
        assertTrue(n >= 0, "Non-negative int should be >= 0");
    }
}
