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

package com.netflix.genie.agent;

import javax.validation.constraints.NotBlank;

/**
 * Container for Genie agent metadata and runtime information.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface AgentMetadata {

    /**
     * Get the agent version string, as it appears in the agent jar metadata.
     *
     * @return a version string or a fallback
     */
    @NotBlank String getAgentVersion();

    /**
     * Get the name of the host the agent is running on.
     *
     * @return a hostname or a fallback string
     */
    @NotBlank String getAgentHostName();

    /**
     * Get the agent process ID.
     *
     * @return the agent process ID or a fallback string
     */
    @NotBlank String getAgentPid();
}
