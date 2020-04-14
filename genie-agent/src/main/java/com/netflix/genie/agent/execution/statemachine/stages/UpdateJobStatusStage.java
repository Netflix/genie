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

import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import lombok.extern.slf4j.Slf4j;

/**
 * Updates the server-side status of the job.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
abstract class UpdateJobStatusStage extends ExecutionStage {
    private final AgentJobService agentJobService;

    /**
     * Constructor.
     *
     * @param agentJobService agent job service
     * @param state           associated state
     */
    protected UpdateJobStatusStage(
        final AgentJobService agentJobService,
        final States state
    ) {
        super(state);
        this.agentJobService = agentJobService;
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {

        final String claimedJobId = executionContext.getReservedJobId();
        final JobStatus currentJobStatus = executionContext.getCurrentJobStatus();
        final JobStatus nextJobStatus = executionContext.getNextJobStatus();
        final String nextJobStatusMessage = executionContext.getNextJobStatusMessage();

        assert claimedJobId != null;
        assert currentJobStatus != null;
        assert nextJobStatus != null;

        if (nextJobStatus != JobStatus.INVALID) {
            assert nextJobStatusMessage != null;

            log.info("Updating job status to: {} - {}", nextJobStatus, nextJobStatusMessage);
            try {
                this.agentJobService.changeJobStatus(
                    claimedJobId,
                    currentJobStatus,
                    nextJobStatus,
                    nextJobStatusMessage
                );
            } catch (final GenieRuntimeException e) {
                throw createRetryableException(e);
            } catch (ChangeJobStatusException e) {
                throw createFatalException(e);
            }

            executionContext.setCurrentJobStatus(nextJobStatus);

        } else {
            log.info("Skipping job status update");
        }
    }
}
