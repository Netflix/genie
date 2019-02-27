/*
 *
 *  Copyright 2019 Netflix, Inc.
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

package com.netflix.genie.common.internal.exceptions;

import javax.validation.constraints.NotBlank;

/**
 * Exception thrown when a the server fails to acquire a stream to communicate with a particular agent.
 * Reasons may vary: client disconnected, all streams already in use, ...
 *
 * @author mprimi
 * @since 4.0.0
 */
public class StreamUnavailableException extends Exception {

    /**
     * Construct with message.
     *
     * @param message a message
     */
    public StreamUnavailableException(final @NotBlank String message) {
        super(message);
    }

    /**
     * Construct with message and cause.
     *
     * @param message a message
     * @param cause   a cause
     */
    public StreamUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
