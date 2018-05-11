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
 * An exception to represent the case where a job was expected to exist (e.g. saving a job specification for a job) but
 * it didn't.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class GenieJobNotFoundException extends GenieRuntimeException {
    /**
     * Constructor.
     */
    public GenieJobNotFoundException() {
        super();
    }

    /**
     * Constructor.
     *
     * @param message The detail message
     */
    public GenieJobNotFoundException(final String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message The detail message
     * @param cause   The root cause of this exception
     */
    public GenieJobNotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor.
     *
     * @param cause The root cause of this exception
     */
    public GenieJobNotFoundException(final Throwable cause) {
        super(cause);
    }
}

