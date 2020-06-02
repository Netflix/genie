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

import com.netflix.genie.agent.execution.exceptions.ConfigureException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.agent.properties.AgentProperties;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.Map;

/**
 * Configures the agent with server-provided runtime parameters that are independent of the job.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class ConfigureAgentStage extends ExecutionStage {

    private final AgentJobService agentJobService;

    /**
     * Constructor.
     *
     * @param agentJobService the agent job service
     */
    public ConfigureAgentStage(
        final AgentJobService agentJobService
    ) {
        super(States.CONFIGURE_AGENT);
        this.agentJobService = agentJobService;
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {

        final AgentClientMetadata agentClientMetadata = executionContext.getAgentClientMetadata();
        final AgentProperties agentProperties = executionContext.getAgentProperties();

        // Obtain server-provided properties
        final Map<String, String> serverPropertiesMap;
        try {
            serverPropertiesMap = this.agentJobService.configure(agentClientMetadata);
        } catch (ConfigureException e) {
            throw new RetryableJobExecutionException("Failed to obtain configuration", e);
        }

        for (final Map.Entry<String, String> entry : serverPropertiesMap.entrySet()) {
            log.info("Received property {}={}", entry.getKey(), entry.getValue());
        }

        // Bind properties received
        final ConfigurationPropertySource serverPropertiesSource =
            new MapConfigurationPropertySource(serverPropertiesMap);

        new Binder(serverPropertiesSource)
            .bind(
                AgentProperties.PREFIX,
                Bindable.ofInstance(agentProperties)
            );
    }
}
