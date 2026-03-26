/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

/**
 * Thrown when a demand read times out waiting for a buffer from the
 * {@link MemoryPool}. This indicates sustained pool exhaustion beyond
 * the configured timeout.
 *
 * @opensearch.internal
 */
public class PoolAcquireTimeoutException extends RuntimeException {

    public PoolAcquireTimeoutException(String message) {
        super(message);
    }
}
