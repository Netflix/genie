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
 * Persistence service to store and lookup which server is a given job/agent connected to.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Validated
public interface AgentConnectionPersistenceService {

    /**
     * Store a new connection currently active from the given agent to the given node.
     *
     * @param jobId    the id of a job the agent is running
     * @param hostname the server owning the connection
     */
    void saveAgentConnection(
        @NotBlank String jobId,
        @NotBlank final String hostname
    );

    /**
     * Remove an existing connection currently active from the given agent to the local node.
     * The entity won't be deleted if it was already modified by a different server.
     *
     * @param jobId    the id of a job the agent is running
     * @param hostname the hostname expected to be associated to the connection
     */
    void removeAgentConnection(
        @NotBlank String jobId,
        @NotBlank final String hostname
    );

    /**
     * Lookup the hostname/address of the server with an active connection to a given agent.
     *
     * @param jobId the id of a job the agent is running
     * @return the server hostname or empty
     */
    Optional<String> lookupAgentConnectionServer(
        @NotBlank String jobId
    );
}
