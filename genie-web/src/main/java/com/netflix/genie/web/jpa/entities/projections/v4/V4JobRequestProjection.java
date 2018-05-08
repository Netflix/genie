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
package com.netflix.genie.web.jpa.entities.projections.v4;

import com.netflix.genie.web.jpa.entities.projections.JobRequestProjection;

import java.util.Map;
import java.util.Optional;

/**
 * Projection of just the fields needed for a V4 {@link com.netflix.genie.common.internal.dto.v4.JobRequest}.
 *
 * @author tgianos
 * @since 4.0.0
 */
public interface V4JobRequestProjection extends JobRequestProjection {

    /**
     * Get whether the unique identifier of this request was requested by the user or not.
     *
     * @return True if it was requested by the user
     */
    // TODO: As we move more to V4 as default move this to more shared location
    boolean isRequestedId();

    /**
     * Get all the environment variables that were requested be added to the Job Runtime environment by the user.
     *
     * @return The requested environment variables
     */
    Map<String, String> getRequestedEnvironmentVariables();

    /**
     * Get the requested directory on disk where the Genie job working directory should be placed if a user asked to
     * override the default.
     *
     * @return The requested job directory location wrapped in an {@link Optional}
     */
    Optional<String> getRequestedJobDirectoryLocation();

    /**
     * Get the extension configuration of a job agent environment.
     *
     * @return The extension configuration (as valid JSON string) if it exists wrapped in an {@link Optional}
     */
    Optional<String> getRequestedAgentEnvironmentExt();

    /**
     * Get the extension configuration of a job agent configuration.
     *
     * @return The extension configuration (as valid JSON string) if it exists wrapped in an {@link Optional}
     */
    Optional<String> getRequestedAgentConfigExt();

    /**
     * Get whether the job was an interactive job or not when launched.
     *
     * @return true if the job was interactive
     */
    boolean isInteractive();
}
