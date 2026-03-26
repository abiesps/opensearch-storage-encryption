/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.harness;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates keys following a Zipfian distribution. Lower keys are "hotter"
 * (accessed more frequently), creating realistic hot/cold skew in the cache.
 *
 * <p>Uses the rejection-inversion method for O(1) generation without
 * pre-computing a CDF table.
 */
public final class ZipfianKeyGenerator {

    private final long keySpace;
    private final double skew;
    private final double hInv;

    /**
     * @param keySpace total number of distinct keys (e.g. maxBlocks * 4)
     * @param skew     Zipfian exponent, typically 0.99. Higher = more skewed.
     */
    public ZipfianKeyGenerator(long keySpace, double skew) {
        this.keySpace = keySpace;
        this.skew = skew;
        this.hInv = 1.0 / h(keySpace + 0.5);
    }

    /** Returns the next Zipfian-distributed key in [0, keySpace). */
    public long nextKey() {
        double u = ThreadLocalRandom.current().nextDouble();
        return hInverse(u / hInv + h(0.5)) - 1;
    }

    private double h(double x) {
        double exp = 1.0 - skew;
        return Math.pow(x, exp) / exp;
    }

    private long hInverse(double x) {
        double exp = 1.0 - skew;
        double result = Math.pow(x * exp, 1.0 / exp);
        return Math.min(keySpace, Math.max(1, (long) (result + 0.5)));
    }
}
