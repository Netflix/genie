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

import com.netflix.genie.agent.execution.process.JobProcessResult;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import lombok.extern.slf4j.Slf4j;

import java.util.EnumSet;

/**
 * Sends the server the final job status, if the job reached a state where it is appropriate to do so.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class DetermineJobFinalStatusStage extends ExecutionStage {

    private static final EnumSet<JobStatus> INTERMEDIATE_ACTIVE_STATUSES = EnumSet.of(
        JobStatus.ACCEPTED,
        JobStatus.RESERVED,
        JobStatus.RESOLVED,
        JobStatus.CLAIMED,
        JobStatus.INIT,
        JobStatus.RUNNING
    );

    /**
     * Constructor.
     */
    public DetermineJobFinalStatusStage() {
        super(States.DETERMINE_FINAL_STATUS);
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {

        final JobStatus currentJobStatus = executionContext.getCurrentJobStatus();
        assert currentJobStatus != null;

        final boolean launched = executionContext.isJobLaunched();
        final boolean killed = executionContext.isJobKilled();
        final FatalJobExecutionException fatalJobExecutionException =
            executionContext.getExecutionAbortedFatalException();
        final JobProcessResult jobProcessResult = executionContext.getJobProcessResult();

        final JobStatus finalJobStatus;
        final String finalJobStatusMessage;

        // This state is never skipped. There are multiple paths leading here.
        // Look at context and determine the proper final status to send to the server (if any).

        if (launched && jobProcessResult != null) {
            // Mark success/failed/killed according to jobProcessResult
            finalJobStatus = jobProcessResult.getFinalStatus();
            finalJobStatusMessage = jobProcessResult.getFinalStatusMessage();
            log.info("Job executed, final status is: {}", finalJobStatus);

        } else if (INTERMEDIATE_ACTIVE_STATUSES.contains(currentJobStatus) && fatalJobExecutionException != null) {
            // Execution was aborted before getting to launch
            finalJobStatus = JobStatus.FAILED;
            finalJobStatusMessage = fatalJobExecutionException.getSourceState().getFatalErrorStatusMessage();
            log.info("Job execution failed, last reported state was {}", currentJobStatus);

        } else if (INTERMEDIATE_ACTIVE_STATUSES.contains(currentJobStatus) && killed) {
            finalJobStatus = JobStatus.KILLED;
            finalJobStatusMessage = JobStatusMessages.JOB_KILLED_BY_USER;
            log.info("Job killed, last reported state was {}", currentJobStatus);

        } else {
            log.info("Job did not reach an active state");
            finalJobStatus = JobStatus.INVALID;
            finalJobStatusMessage = JobStatusMessages.UNKNOWN_JOB_STATE;

        }

        executionContext.setNextJobStatus(finalJobStatus);
        executionContext.setNextJobStatusMessage(finalJobStatusMessage);
    }
}
