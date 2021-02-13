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
package com.netflix.genie.web.agent.services.impl;

import com.google.common.collect.Sets;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import java.util.Optional;
import java.util.Set;

/**
 * Implementation of {@link AgentRoutingService} that assumes a single Genie node and tracks connections in-memory.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Validated
@Slf4j
public class AgentRoutingServiceSingleNodeImpl implements AgentRoutingService {

    private final GenieHostInfo genieHostInfo;
    private final Set<String> connectedAgents = Sets.newConcurrentHashSet();

    /**
     * Constructor.
     *
     * @param genieHostInfo local genie node host information
     */
    public AgentRoutingServiceSingleNodeImpl(final GenieHostInfo genieHostInfo) {
        this.genieHostInfo = genieHostInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getHostnameForAgentConnection(final @NotBlank String jobId) {
        return isAgentConnected(jobId) ? Optional.of(this.genieHostInfo.getHostname()) : Optional.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAgentConnectionLocal(final @NotBlank String jobId) {
        return isAgentConnected(jobId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleClientConnected(@NotBlank final String jobId) {
        log.info("Agent executing job {} connected", jobId);
        this.connectedAgents.add(jobId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleClientDisconnected(@NotBlank final String jobId) {
        log.info("Agent executing job {} disconnected", jobId);
        this.connectedAgents.remove(jobId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAgentConnected(final String jobId) {
        return this.connectedAgents.contains(jobId);
    }
}
