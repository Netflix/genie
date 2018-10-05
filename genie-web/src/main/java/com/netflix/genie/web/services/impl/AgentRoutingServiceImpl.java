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

package com.netflix.genie.web.services.impl;

import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.services.AgentConnectionObserver;
import com.netflix.genie.web.services.AgentConnectionPersistenceService;
import com.netflix.genie.web.services.AgentRoutingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.TaskExecutor;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import java.util.List;
import java.util.Optional;

/**
 * Implementation of {@link AgentRoutingService}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Validated
@Slf4j
public class AgentRoutingServiceImpl implements AgentRoutingService {

    private final AgentConnectionPersistenceService agentConnectionPersistenceService;
    private final GenieHostInfo genieHostInfo;
    private final List<AgentConnectionObserver> agentConnectionObservers;
    private final TaskExecutor connectionObserversNotificationExecutor;

    /**
     * Constructor.
     *
     * @param agentConnectionPersistenceService         agent connection persistence service
     * @param genieHostInfo                             local genie node host information
     * @param agentConnectionObservers                  agent connection observers
     * @param connectionObserversNotificationExecutor   executor for notifying the agent connection observers
     */
    public AgentRoutingServiceImpl(
        final AgentConnectionPersistenceService agentConnectionPersistenceService,
        final GenieHostInfo genieHostInfo,
        final List<AgentConnectionObserver> agentConnectionObservers,
        final TaskExecutor connectionObserversNotificationExecutor
    ) {
        this.agentConnectionPersistenceService = agentConnectionPersistenceService;
        this.genieHostInfo = genieHostInfo;
        this.agentConnectionObservers = agentConnectionObservers;
        this.connectionObserversNotificationExecutor = connectionObserversNotificationExecutor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getHostnameForAgentConnection(final @NotBlank String jobId) {
        return this.agentConnectionPersistenceService.lookupAgentConnectionServer(jobId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAgentConnectionLocal(final @NotBlank String jobId) {
        final Optional<String> hostname = getHostnameForAgentConnection(jobId);
        return hostname.isPresent() && hostname.get().equals(genieHostInfo.getHostname());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleClientConnected(@NotBlank final String jobId) {
        log.info("Agent executing job {} connected", jobId);
        this.agentConnectionPersistenceService.saveAgentConnection(jobId, genieHostInfo.getHostname());

        for (AgentConnectionObserver agentConnectionObserver : agentConnectionObservers) {
            this.connectionObserversNotificationExecutor.execute(
                new AgentConnectedNotificationTask(agentConnectionObserver, jobId)
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleClientDisconnected(@NotBlank final String jobId) {
        log.info("Agent executing job {} disconnected", jobId);
        this.agentConnectionPersistenceService.removeAgentConnection(jobId, genieHostInfo.getHostname());

        for (AgentConnectionObserver agentConnectionObserver : agentConnectionObservers) {
            this.connectionObserversNotificationExecutor.execute(
                new AgentDisconnectedNotificationTask(agentConnectionObserver, jobId)
            );
        }
    }

    /**
     * Task to deliver agent connected notification to a AgentConnectionObserver.
     */
    static class AgentConnectedNotificationTask implements Runnable {

        private final AgentConnectionObserver agentConnectionObserver;
        private final String jobId;

        /**
         * Constructor.
         *
         * @param agentConnectionObserver connection observer which needs to be notified
         * @param jobId                   job id of the job handled the agent connected
         */
        AgentConnectedNotificationTask(final AgentConnectionObserver agentConnectionObserver,
                                       final String jobId) {
            this.agentConnectionObserver = agentConnectionObserver;
            this.jobId = jobId;
        }

        @Override
        public void run() {
            this.agentConnectionObserver.onConnected(this.jobId);
        }
    }

    /**
     * Task to deliver agent disconnected notification to a AgentConnectionObserver .
     */
    static class AgentDisconnectedNotificationTask implements Runnable {

        private final AgentConnectionObserver agentConnectionObserver;
        private final String jobId;

        /**
         * Constructor.
         *
         * @param agentConnectionObserver connection observer which needs to be notified
         * @param jobId                   job id of the job handled the agent connected
         */
        AgentDisconnectedNotificationTask(final AgentConnectionObserver agentConnectionObserver,
                                          final String jobId) {
            this.agentConnectionObserver = agentConnectionObserver;
            this.jobId = jobId;
        }

        @Override
        public void run() {
            this.agentConnectionObserver.onDisconnected(this.jobId);
        }
    }
}
