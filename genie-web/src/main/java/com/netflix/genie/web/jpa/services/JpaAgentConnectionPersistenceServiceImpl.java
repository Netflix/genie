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

package com.netflix.genie.web.jpa.services;

import com.netflix.genie.web.jpa.entities.AgentConnectionEntity;
import com.netflix.genie.web.jpa.repositories.JpaAgentConnectionRepository;
import com.netflix.genie.web.services.AgentConnectionPersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.constraints.NotBlank;
import java.util.Optional;

/**
 * JPA implementation of AgentConnectionPersistenceService.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Transactional
public class JpaAgentConnectionPersistenceServiceImpl implements AgentConnectionPersistenceService {

    private final JpaAgentConnectionRepository agentConnectionRepository;

    /**
     * Constructor.
     *
     * @param agentConnectionRepository agent connection repository
     */
    public JpaAgentConnectionPersistenceServiceImpl(
        final JpaAgentConnectionRepository agentConnectionRepository
    ) {
        this.agentConnectionRepository = agentConnectionRepository;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveAgentConnection(@NotBlank final String jobId, @NotBlank final String hostname) {
        final Optional<AgentConnectionEntity> existingEntity = getAgentConnection(jobId);
        if (existingEntity.isPresent()) {
            existingEntity.get().setServerHostname(hostname);
        } else {
            final AgentConnectionEntity newEntity = toEntity(jobId, hostname);
            this.agentConnectionRepository.save(newEntity);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAgentConnection(@NotBlank final String jobId, @NotBlank final String hostname) {
        final Optional<AgentConnectionEntity> existingEntity = getAgentConnection(jobId);
        if (existingEntity.isPresent()
            && existingEntity.get().getServerHostname().equals(hostname)) {
            this.agentConnectionRepository.delete(existingEntity.get());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Optional<String> lookupAgentConnectionServer(@NotBlank final String jobId) {
        final Optional<AgentConnectionEntity> agentConnectionEntity =
            this.agentConnectionRepository.findByJobId(jobId);

        return agentConnectionEntity.map(AgentConnectionEntity::getServerHostname);
    }

    @Transactional(readOnly = true)
    private Optional<AgentConnectionEntity> getAgentConnection(@NotBlank final String jobId) {
        return this.agentConnectionRepository.findByJobId(jobId);
    }

    private AgentConnectionEntity toEntity(
        final @NotBlank String jobId,
        final @NotBlank String hostname
    ) {
        return new AgentConnectionEntity(jobId, hostname);
    }
}
