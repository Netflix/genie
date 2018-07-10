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

package com.netflix.genie.agent.execution.services;

import javax.validation.constraints.NotBlank;

/**
 * Service that maintains an active connection with a Genie server node by sending heart beats.
 * The agent messages are tagged with the job id this agent claimed and is executing.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface AgentHeartBeatService {

    /**
     * Starts the service.
     *
     * @param claimedJobId the job id claimed by this agent
     */
    void start(@NotBlank String claimedJobId);

    /**
     * Stop the service.
     */
    void stop();

    /**
     * Whether the agent is currently connected to a Genie node server.
     *
     * @return true if the agent has an active, working connection with a Genie server node.
     */
    boolean isConnected();
}
