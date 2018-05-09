/*
 *
 *  Copyright 2018 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.genie.common.internal.exceptions.unchecked;

/**
 * Base class for Genie runtime exceptions.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class GenieRuntimeException extends RuntimeException {
    /**
     * Constructor.
     */
    public GenieRuntimeException() {
        super();
    }

    /**
     * Constructor.
     *
     * @param message The detail message
     */
    public GenieRuntimeException(final String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message The detail message
     * @param cause   The root cause of this exception
     */
    public GenieRuntimeException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor.
     *
     * @param cause The root cause of this exception
     */
    public GenieRuntimeException(final Throwable cause) {
        super(cause);
    }
}
