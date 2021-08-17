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

import brave.Tracer;
import com.netflix.genie.agent.cli.logging.ConsoleLog;
import com.netflix.genie.agent.execution.exceptions.GetJobStatusException;
import com.netflix.genie.agent.execution.exceptions.JobIdUnavailableException;
import com.netflix.genie.agent.execution.exceptions.JobReservationException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import com.netflix.genie.common.internal.tracing.TracingConstants;
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import lombok.extern.slf4j.Slf4j;

/**
 * Performs job reservation, or ensures the job is pre-reserved and ready to be claimed.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class ReserveJobIdStage extends ExecutionStage {
    private final AgentJobService agentJobService;
    private final Tracer tracer;
    private final BraveTagAdapter tagAdapter;

    /**
     * Constructor.
     *
     * @param agentJobService   agent job service.
     * @param tracingComponents The {@link BraveTracingComponents} instance
     */
    public ReserveJobIdStage(
        final AgentJobService agentJobService,
        final BraveTracingComponents tracingComponents
    ) {
        super(States.RESERVE_JOB_ID);
        this.agentJobService = agentJobService;
        this.tracer = tracingComponents.getTracer();
        this.tagAdapter = tracingComponents.getTagAdapter();
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {
        final String requestedJobId = executionContext.getRequestedJobId();
        final String reservedJobId;

        if (executionContext.isPreResolved()) {
            assert requestedJobId != null;
            log.info("Confirming job reservation");

            final JobStatus jobStatus;
            try {
                jobStatus = this.agentJobService.getJobStatus(requestedJobId);
            } catch (GetJobStatusException | GenieRuntimeException e) {
                throw new RetryableJobExecutionException("Failed to retrieve job status", e);
            }

            if (jobStatus != JobStatus.ACCEPTED) {
                throw createFatalException(
                    new IllegalStateException("Unexpected job status: " + jobStatus + " job cannot be claimed")
                );
            }

            executionContext.setCurrentJobStatus(JobStatus.ACCEPTED);
            reservedJobId = requestedJobId;
        } else {
            log.info("Requesting job id reservation");

            final AgentJobRequest jobRequest = executionContext.getAgentJobRequest();
            final AgentClientMetadata agentClientMetadata = executionContext.getAgentClientMetadata();

            assert jobRequest != null;
            assert agentClientMetadata != null;

            try {
                reservedJobId = this.agentJobService.reserveJobId(jobRequest, agentClientMetadata);
            } catch (final GenieRuntimeException e) {
                throw createRetryableException(e);
            } catch (final JobIdUnavailableException | JobReservationException e) {
                throw createFatalException(e);
            }

            executionContext.setCurrentJobStatus(JobStatus.RESERVED);

            ConsoleLog.getLogger().info("Successfully reserved job id: {}", reservedJobId);
        }

        executionContext.setReservedJobId(reservedJobId);
        this.tagAdapter.tag(this.tracer.currentSpanCustomizer(), TracingConstants.JOB_ID_TAG, reservedJobId);
    }
}
