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
package com.netflix.genie.web.agent.services;

/**
 * Tracks active connections and heartbeats coming from agents actively executing a job.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface AgentConnectionTrackingService {

    /**
     * Handle a heartbeat.
     *
     * @param streamId     the unique id of the connection
     * @param claimedJobId the job id claimed by the agent
     */
    void notifyHeartbeat(String streamId, String claimedJobId);

    /**
     * Handle a disconnection.
     *
     * @param streamId     the unique id of the connection
     * @param claimedJobId the job id claimed by the agent
     */
    void notifyDisconnected(String streamId, String claimedJobId);

    /**
     * Get the count of locally connected agents.
     *
     * @return the number of agents connected.
     */
    long getConnectedAgentsCount();
}
