/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

/**
 * Thrown when the {@link MemoryPool} is exhausted and a prefetch operation
 * is skipped rather than blocking. Demand reads block with timeout instead
 * of throwing this exception.
 *
 * @opensearch.internal
 */
public class MemoryBackpressureException extends RuntimeException {

    public MemoryBackpressureException(String message) {
        super(message);
    }
}
