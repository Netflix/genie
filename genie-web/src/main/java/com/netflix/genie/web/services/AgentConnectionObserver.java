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

import javax.validation.constraints.NotNull;

/**
 * Interface to observe the connection status of an agent to the local genie server.
 *
 * @author standon
 * @since 4.0.0
 */
@Validated
public interface AgentConnectionObserver {

    /**
     * Handle connection of the agent to the local server.
     *
     * @param jobId job id of the job run by the agent
     */
    void onConnected(@NotNull final String jobId);

    /**
     * Handle disconnection of the agent from the local server.
     *
     * @param jobId job id of the job run by the agent
     */
    void onDisconnected(@NotNull final String jobId);
}
