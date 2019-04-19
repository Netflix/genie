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
package com.netflix.genie.web.health;

import com.netflix.genie.web.services.AgentMetricsService;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Provides a health indicator relative to the behavior of Genie Agents and this Server.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class GenieAgentHealthIndicator implements HealthIndicator {

    static final String NUM_CONNECTED_AGENTS = "numConnectedAgents";

    private final AgentMetricsService agentMetricsService;

    /**
     * Constructor.
     *
     * @param agentMetricsService For collecting metrics about the Agents connected to this server.
     */
    public GenieAgentHealthIndicator(final AgentMetricsService agentMetricsService) {
        this.agentMetricsService = agentMetricsService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Health health() {
        final Health.Builder builder = new Health.Builder();

        // TODO: For now just set it to always be up till we have better metrics. Likely want to tie this to gRPC
        //       health or something else related there. Or if number of connections exceeds something we're
        //       comfortable with
        builder.up();

        builder.withDetail(NUM_CONNECTED_AGENTS, this.agentMetricsService.getNumConnectedAgents());
        return builder.build();
    }
}
