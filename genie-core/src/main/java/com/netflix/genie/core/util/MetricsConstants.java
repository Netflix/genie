/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.core.util;

/**
 * Used to store contants related to metric names.
 *
 * @author tgianos
 * @since 3.0.0
 */
public final class MetricsConstants {

    /**
     * For counting how often bad requests happen in the system.
     */
    public static final String GENIE_EXCEPTIONS_BAD_REQUEST_RATE = "genie.exceptions.badRequest.rate";

    /**
     * For counting how often conflicts happen in the system.
     */
    public static final String GENIE_EXCEPTIONS_CONFLICT_RATE = "genie.exceptions.conflict.rate";

    /**
     * For counting how often not found exceptions happen in the system.
     */
    public static final String GENIE_EXCEPTIONS_NOT_FOUND_RATE = "genie.exceptions.notFound.rate";

    /**
     * For counting how often precondition exceptions happen in the system.
     */
    public static final String GENIE_EXCEPTIONS_PRECONDITION_RATE = "genie.exceptions.precondition.rate";

    /**
     * For counting how often server exceptions happen in the system.
     */
    public static final String GENIE_EXCEPTIONS_SERVER_RATE = "genie.exceptions.server.rate";

    /**
     * For counting how often server unavailable exceptions in the system.
     */
    public static final String GENIE_EXCEPTIONS_SERVER_UNAVAILABLE_RATE = "genie.exceptions.serverUnavailable.rate";

    /**
     * For counting how often timeout exceptions happen in the system.
     */
    public static final String GENIE_EXCEPTIONS_TIMEOUT_RATE = "genie.exceptions.timeout.rate";

    /**
     * For counting how often bad requests happen in the system.
     */
    public static final String GENIE_EXCEPTIONS_OTHER_RATE = "genie.exceptions.other.rate";

    /**
     * For counting how often constraint violations happen in the system.
     */
    public static final String GENIE_EXCEPTIONS_CONSTRAINT_VIOLATION_RATE = "genie.exceptions.constraintViolation.rate";

    /**
     * For counting how often requests are rejected due to user exceeding limits.
     */
    public static final String GENIE_EXCEPTIONS_USER_LIMIT_EXCEEDED_RATE = "genie.exceptions.userLimitExceeded.rate";

    /**
     * Utility class protected constructor.
     */
    protected MetricsConstants() {
    }
}
