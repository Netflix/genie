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

import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.cli.JobRequestConverter;
import com.netflix.genie.agent.execution.CleanupStrategy;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Sets up context state based on the type of execution and other command-line parameters.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class ConfigureExecutionStage extends ExecutionStage {

    private final JobRequestConverter jobRequestConverter;
    private final ArgumentDelegates.JobRequestArguments jobRequestArguments;
    private final ArgumentDelegates.RuntimeConfigurationArguments runtimeConfigurationArguments;
    private final ArgumentDelegates.CleanupArguments cleanupArguments;

    /**
     * Constructor.
     *
     * @param jobRequestConverter           job request converter
     * @param jobRequestArguments           job request arguments group
     * @param runtimeConfigurationArguments runtime configuration arguments group
     * @param cleanupArguments              cleanup arguments group
     */
    public ConfigureExecutionStage(
        final JobRequestConverter jobRequestConverter,
        final ArgumentDelegates.JobRequestArguments jobRequestArguments,
        final ArgumentDelegates.RuntimeConfigurationArguments runtimeConfigurationArguments,
        final ArgumentDelegates.CleanupArguments cleanupArguments
    ) {
        super(States.CONFIGURE_EXECUTION);
        this.jobRequestConverter = jobRequestConverter;
        this.jobRequestArguments = jobRequestArguments;
        this.runtimeConfigurationArguments = runtimeConfigurationArguments;
        this.cleanupArguments = cleanupArguments;
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {

        // Determine whether job has pre-resolved job specification
        final boolean isPreResolved = this.jobRequestArguments.isJobRequestedViaAPI();
        executionContext.setPreResolved(isPreResolved);

        final boolean isRunInJobDirectory = this.runtimeConfigurationArguments.isLaunchInJobDirectory();
        executionContext.setRunFromJobDirectory(isRunInJobDirectory);

        final CleanupStrategy cleanupStrategy = this.cleanupArguments.getCleanupStrategy();
        executionContext.setCleanupStrategy(cleanupStrategy);

        final String requestedJobId = this.jobRequestArguments.getJobId();

        if (isPreResolved) {
            log.info("Configuring execution for pre-resolved job");

            if (StringUtils.isBlank(requestedJobId)) {
                throw createFatalException(new IllegalArgumentException("Missing required argument job id"));
            }

        } else {
            log.info("Configuring execution for CLI job");

            // Create job request from arguments if this is not a pre-reserved job
            try {
                final AgentJobRequest agentJobRequest =
                    this.jobRequestConverter.agentJobRequestArgsToDTO(jobRequestArguments);
                executionContext.setAgentJobRequest(agentJobRequest);
            } catch (final JobRequestConverter.ConversionException e) {
                throw createFatalException(e);
            }
        }

        // Save the job id requested.
        // May be blank/null if the request is not for a pre-resolved "API" job.
        executionContext.setRequestedJobId(requestedJobId);
    }
}
