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
package com.netflix.genie.web.exceptions.checked;

import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;

/**
 * An exception for when the server can't launch an agent for whatever reason.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class AgentLaunchException extends GenieCheckedException {
    /**
     * Constructor.
     */
    public AgentLaunchException() {
        super();
    }

    /**
     * Constructor.
     *
     * @param message The error message to associate with this exception
     */
    public AgentLaunchException(final String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause The root cause of this exception
     */
    public AgentLaunchException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     *
     * @param message The error message to associate with this exception
     * @param cause   The root cause of this exception
     */
    public AgentLaunchException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
