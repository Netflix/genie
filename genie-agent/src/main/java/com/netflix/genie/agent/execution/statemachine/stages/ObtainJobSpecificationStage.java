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

import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import lombok.extern.slf4j.Slf4j;

/**
 * Obtains job specification from server.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class ObtainJobSpecificationStage extends ExecutionStage {
    private final AgentJobService agentJobService;

    /**
     * Constructor.
     *
     * @param agentJobService agent job service
     */
    public ObtainJobSpecificationStage(final AgentJobService agentJobService) {
        super(States.OBTAIN_JOB_SPECIFICATION);
        this.agentJobService = agentJobService;
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {

        final String reservedJobId = executionContext.getReservedJobId();
        assert reservedJobId != null;

        final JobSpecification jobSpecification;

        try {

            if (executionContext.isPreResolved()) {
                log.info("Retrieving pre-resolved job specification");
                jobSpecification = this.agentJobService.getJobSpecification(reservedJobId);

            } else {
                log.info("Requesting job specification resolution");
                jobSpecification = agentJobService.resolveJobSpecification(reservedJobId);
                executionContext.setCurrentJobStatus(JobStatus.RESOLVED);
            }

        } catch (final GenieRuntimeException e) {
            throw createRetryableException(e);
        } catch (final JobSpecificationResolutionException e) {
            throw createFatalException(e);
        }

        executionContext.setJobSpecification(jobSpecification);

        log.info("Successfully obtained specification");

    }
}
