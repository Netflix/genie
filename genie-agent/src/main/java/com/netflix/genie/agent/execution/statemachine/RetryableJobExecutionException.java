/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.agent.execution.statemachine;

/**
 * Exception thrown by state machine transition for retryable errors (e.g, network error talking to server).
 *
 * @author mprimi
 * @since 4.0.0
 */
public class RetryableJobExecutionException extends RuntimeException {

    /**
     * Constructor.
     *
     * @param message message
     * @param cause   cause
     */
    public RetryableJobExecutionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
