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

import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.PersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
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

    private final PersistenceService persistenceService;
    private final GenieHostInfo genieHostInfo;

    /**
     * Constructor.
     *
     * @param persistenceService persistence service
     * @param genieHostInfo      local genie node host information
     */
    public AgentRoutingServiceImpl(final PersistenceService persistenceService, final GenieHostInfo genieHostInfo) {
        this.persistenceService = persistenceService;
        this.genieHostInfo = genieHostInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getHostnameForAgentConnection(final @NotBlank String jobId) {
        return this.persistenceService.lookupAgentConnectionServer(jobId);
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
        this.persistenceService.saveAgentConnection(jobId, genieHostInfo.getHostname());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleClientDisconnected(@NotBlank final String jobId) {
        log.info("Agent executing job {} disconnected", jobId);
        this.persistenceService.removeAgentConnection(jobId, genieHostInfo.getHostname());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAgentConnected(final String jobId) {
        return getHostnameForAgentConnection(jobId).isPresent();
    }
}
