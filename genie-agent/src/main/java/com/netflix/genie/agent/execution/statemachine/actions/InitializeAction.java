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

import com.netflix.genie.agent.AgentMetadata;
import com.netflix.genie.agent.cli.UserConsole;
import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.exceptions.HandshakeException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.common.internal.dtos.v4.AgentClientMetadata;
import lombok.extern.slf4j.Slf4j;

/**
 * Action performed when in state INITIALIZE.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
class InitializeAction extends BaseStateAction implements StateAction.Initialize {

    private final AgentJobService agentJobService;
    private final AgentMetadata agentMetadata;

    InitializeAction(
        final ExecutionContext executionContext,
        final AgentJobService agentJobService,
        final AgentMetadata agentMetadata
    ) {
        super(executionContext);
        this.agentJobService = agentJobService;
        this.agentMetadata = agentMetadata;
    }

    @Override
    protected void executePreActionValidation() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Events executeStateAction(final ExecutionContext executionContext) {
        UserConsole.getLogger().info("Initializing job execution...");

        final AgentClientMetadata agentClientMetadata = new AgentClientMetadata(
            agentMetadata.getAgentHostName(),
            agentMetadata.getAgentVersion(),
            Integer.parseInt(agentMetadata.getAgentPid())
        );

        try {
            agentJobService.handshake(agentClientMetadata);
        } catch (final HandshakeException e) {
            throw new RuntimeException("Could not shake hands with server", e);
        }

        return Events.INITIALIZE_COMPLETE;
    }

    @Override
    protected void executePostActionValidation() {
    }
}
