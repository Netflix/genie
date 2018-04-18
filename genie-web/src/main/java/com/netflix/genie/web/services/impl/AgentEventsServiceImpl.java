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

import com.netflix.genie.common.dto.v4.AgentEvent;
import com.netflix.genie.web.services.AgentEventsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;

/**
 * Implementation of AgentEventsService.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ParametersAreNonnullByDefault
@Validated
@Service
@Slf4j
class AgentEventsServiceImpl implements AgentEventsService {

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleAgentEvent(@Valid final AgentEvent.JobStatusUpdate jobStatusUpdate) {
        print(jobStatusUpdate);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleAgentEvent(@Valid final AgentEvent.StateChange stateChange) {
        print(stateChange);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleAgentEvent(@Valid final AgentEvent.StateActionExecution stateActionExecution) {
        print(stateActionExecution);
    }

    // TODO: placeholder for actual handling of events
    private void print(final AgentEvent agentEvent) {
        log.info("Got an agent event: {}", agentEvent);
    }

}
