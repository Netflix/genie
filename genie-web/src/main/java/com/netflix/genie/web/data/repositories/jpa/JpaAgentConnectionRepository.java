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
package com.netflix.genie.web.data.repositories.jpa;

import com.netflix.genie.web.data.entities.AgentConnectionEntity;

import javax.validation.constraints.NotBlank;
import java.util.Optional;

/**
 * JPA repository for active agent-to-server connections.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface JpaAgentConnectionRepository extends JpaIdRepository<AgentConnectionEntity> {

    /**
     * Find entity by jobId.
     *
     * @param jobId job id
     * @return an optional, containing the entity if it exists
     */
    Optional<AgentConnectionEntity> findByJobId(@NotBlank String jobId);

    /**
     * Find the number of agents currently connected to the given server identified by {@code serverHostName}.
     *
     * @param serverHostName The hostname of the server to get the number of connections for
     * @return The number of connections on the given server
     */
    long countByServerHostnameEquals(@NotBlank String serverHostName);


    /**
     * Delete all rows associated with a given server identified by {@code serverHostName}.
     *
     * @param serverHostName The hostname of the server whose connections to purge
     * @return the number of deleted records
     */
    int deleteByServerHostnameEquals(String serverHostName);
}
