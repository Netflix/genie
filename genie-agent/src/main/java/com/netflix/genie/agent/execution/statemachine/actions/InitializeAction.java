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

package com.netflix.genie.agent.execution.statemachine.actions;

import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.exceptions.AgentRegistrationException;
import com.netflix.genie.agent.execution.services.AgentRegistrationService;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.agent.execution.statemachine.States;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.statemachine.StateContext;
import org.springframework.stereotype.Component;

/**
 * Action performed when in state INITIALIZE.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class InitializeAction extends BaseStateAction implements StateAction.Initialize {

    private final AgentRegistrationService agentRegistrationService;
    private final ExecutionContext executionContext;

    InitializeAction(
        final AgentRegistrationService agentRegistrationService,
        final ExecutionContext executionContext
    ) {
        this.agentRegistrationService = agentRegistrationService;
        this.executionContext = executionContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Events executeStateAction(final StateContext<States, Events> context) {
        log.info("Initializing...");

        final String agentId;
        try {
            agentId = agentRegistrationService.registerAgent();
        } catch (final AgentRegistrationException e) {
            throw new RuntimeException("Failed to obtain agent id", e);
        }

        log.info("Obtained agent ID: {}", agentId);

        executionContext.setAgentId(agentId);

        return Events.INITIALIZE_COMPLETE;
    }
}
