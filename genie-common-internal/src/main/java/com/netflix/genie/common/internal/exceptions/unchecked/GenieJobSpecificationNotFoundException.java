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
 * An exception to represent the case where a job specification was expected to exist but it doesn't.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class GenieJobSpecificationNotFoundException extends GenieRuntimeException {
    /**
     * Constructor.
     */
    public GenieJobSpecificationNotFoundException() {
        super();
    }

    /**
     * Constructor.
     *
     * @param message The detail message
     */
    public GenieJobSpecificationNotFoundException(final String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message The detail message
     * @param cause   The root cause of this exception
     */
    public GenieJobSpecificationNotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor.
     *
     * @param cause The root cause of this exception
     */
    public GenieJobSpecificationNotFoundException(final Throwable cause) {
        super(cause);
    }
}

