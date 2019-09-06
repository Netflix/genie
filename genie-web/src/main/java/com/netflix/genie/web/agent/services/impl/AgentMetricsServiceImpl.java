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
package com.netflix.genie.web.agent.services.impl;

import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.services.AgentMetricsService;
import com.netflix.genie.web.data.services.AgentConnectionPersistenceService;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Default implementation of {@link AgentMetricsService}.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class AgentMetricsServiceImpl implements AgentMetricsService {

    static final String CONNECTED_GUAGE_METRIC_NAME = "genie.agents.connected.gauge";

    private final GenieHostInfo genieHostInfo;
    private final AgentConnectionPersistenceService agentConnectionPersistenceService;

    /**
     * Constructor.
     *
     * @param agentConnectionPersistenceService The service to get connection information from
     * @param genieHostInfo                     Information regarding to the host this Genie server is running on
     * @param registry                          The metrics repository
     */
    public AgentMetricsServiceImpl(
        final GenieHostInfo genieHostInfo,
        final AgentConnectionPersistenceService agentConnectionPersistenceService,
        final MeterRegistry registry
    ) {
        this.genieHostInfo = genieHostInfo;
        this.agentConnectionPersistenceService = agentConnectionPersistenceService;

        // Schedule metrics collection
        registry.gauge(CONNECTED_GUAGE_METRIC_NAME, this, AgentMetricsServiceImpl::getNumConnectedAgents);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getNumConnectedAgents() {
        return this.agentConnectionPersistenceService.getNumAgentConnectionsOnServer(this.genieHostInfo.getHostname());
    }
}
