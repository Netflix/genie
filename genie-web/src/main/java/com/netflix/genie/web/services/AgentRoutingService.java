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

package com.netflix.genie.web.services;

import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import java.util.Optional;

/**
 * Service that tracks agent connections on the local Genie node and provides routing information for
 * agent connected to other nodes.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Validated
public interface AgentRoutingService {

    /**
     * Look up the hostname of the Genie node currently handling the agent connection for a given job.
     *
     * @param jobId the job id
     * @return a boxed hostname string, empty if the connection for the given job id is not found
     */
    Optional<String> getHostnameForAgentConnection(@NotBlank final String jobId);

    /**
     * Tells wether the agent running a given job is connected to the local node.
     *
     * @param jobId the job id
     * @return true if the agent has an active connection to this node
     */
    boolean isAgentConnectionLocal(@NotBlank final String jobId);

    /**
     * Handle a new agent connection.
     *
     * @param jobId the job id the connected agent is running
     */
    void handleClientConnected(@NotBlank String jobId);

    /**
     * Handle connected agent disconnection.
     *
     * @param jobId the job id the disconnected agent is running
     */
    void handleClientDisconnected(@NotBlank String jobId);

}
