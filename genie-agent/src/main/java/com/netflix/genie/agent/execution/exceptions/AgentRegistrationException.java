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

package com.netflix.genie.agent.execution.exceptions;

/**
 * Error to register the agent with the server.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class AgentRegistrationException extends Exception {
    /**
     * Construct with a message.
     *
     * @param message message
     */
    public AgentRegistrationException(final String message) {
        super(message);
    }

    /**
     * Construct with a message and cause.
     *
     * @param message message
     * @param cause cause
     */
    public AgentRegistrationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
