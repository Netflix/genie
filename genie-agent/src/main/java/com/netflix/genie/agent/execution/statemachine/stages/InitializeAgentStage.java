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
package com.netflix.genie.agent.execution.statemachine.stages;

import com.netflix.genie.agent.AgentMetadata;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import lombok.extern.slf4j.Slf4j;

/**
 * Performs generic initialization.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class InitializeAgentStage extends ExecutionStage {
    private AgentMetadata agentMetadata;

    /**
     * Constructor.
     *
     * @param agentMetadata agent metadata
     */
    public InitializeAgentStage(final AgentMetadata agentMetadata) {
        super(States.INITIALIZE_AGENT);
        this.agentMetadata = agentMetadata;
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {
        log.info("Creating agent client metadata");
        final AgentClientMetadata agentClientMetadata = new AgentClientMetadata(
            agentMetadata.getAgentHostName(),
            agentMetadata.getAgentVersion(),
            Integer.parseInt(agentMetadata.getAgentPid())
        );
        executionContext.setAgentClientMetadata(agentClientMetadata);
    }
}
