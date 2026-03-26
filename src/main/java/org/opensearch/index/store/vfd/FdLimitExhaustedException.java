/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.vfd;

import java.io.IOException;

/**
 * Thrown when the FD_Open_Limiter semaphore cannot be acquired within the
 * configured timeout, indicating that the system-wide open FileChannel
 * limit has been exhausted.
 *
 * @opensearch.internal
 */
public class FdLimitExhaustedException extends IOException {

    public FdLimitExhaustedException(String message) {
        super(message);
    }

    public FdLimitExhaustedException(String message, Throwable cause) {
        super(message, cause);
    }
}
