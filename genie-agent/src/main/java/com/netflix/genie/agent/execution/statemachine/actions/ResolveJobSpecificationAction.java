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
import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.cli.JobRequestConverter;
import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.exceptions.JobIdUnavailableException;
import com.netflix.genie.agent.execution.exceptions.JobReservationException;
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.dto.v4.AgentJobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Action performed when in state RESOLVE_JOB_SPECIFICATION.
 * If this action executes successfully, the context contains a resolved job specification.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class ResolveJobSpecificationAction extends BaseStateAction implements StateAction.ResolveJobSpecification {

    private final ArgumentDelegates.JobRequestArguments jobRequestArguments;
    private final AgentJobService agentJobService;
    private final AgentMetadata agentMetadata;
    private final JobRequestConverter jobRequestConverter;

    ResolveJobSpecificationAction(
        final ExecutionContext executionContext,
        final ArgumentDelegates.JobRequestArguments jobRequestArguments,
        final AgentJobService agentJobService,
        final AgentMetadata agentMetadata,
        final JobRequestConverter jobRequestConverter
    ) {
        super(executionContext);
        this.jobRequestArguments = jobRequestArguments;
        this.agentJobService = agentJobService;
        this.agentMetadata = agentMetadata;
        this.jobRequestConverter = jobRequestConverter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Events executeStateAction(final ExecutionContext executionContext) {

        log.info("Resolving job specification...");

        final JobSpecification jobSpecification;
        final String jobId;

        final AgentClientMetadata agentClientMetadata = new AgentClientMetadata(
            agentMetadata.getAgentHostName(),
            agentMetadata.getAgentVersion(),
            Integer.parseInt(agentMetadata.getAgentPid())
        );

        // 2 cases:
        // - Job requested via API and agent launched by the server (with specification pre-resolved)
        // - Job launched directly via agent CLI (with job request to be resolved into a specification)
        if (jobRequestArguments.isJobRequestedViaAPI()) {
            // Set current status to expected server-side state.
            executionContext.setCurrentJobStatus(JobStatus.ACCEPTED);

            // Expect job id to be provided on the command-line
            jobId = jobRequestArguments.getJobId();

            // Obtain the specification pre-resolved on server
            try {
                jobSpecification = agentJobService.getJobSpecification(jobId);
            } catch (final JobSpecificationResolutionException e) {
                throw new RuntimeException("Could not get specification for job " + jobId, e);
            }

        } else {
            final AgentJobRequest agentJobRequest;
            try {
                agentJobRequest = jobRequestConverter.agentJobRequestArgsToDTO(jobRequestArguments);
            } catch (final JobRequestConverter.ConversionException e) {
                throw new RuntimeException("Failed to compose job request", e);
            }

            // Reserve a job ID
            try {
                jobId = agentJobService.reserveJobId(agentJobRequest, agentClientMetadata);
            } catch (final JobIdUnavailableException e) {
                throw new RuntimeException("The requested job id is already in use", e);
            } catch (final JobReservationException e) {
                throw new RuntimeException("Failed to issue job reservation", e);
            }

            executionContext.setCurrentJobStatus(JobStatus.RESERVED);

            // Request server-side resolution
            try {
                jobSpecification = agentJobService.resolveJobSpecification(jobId);
            } catch (final JobSpecificationResolutionException e) {
                throw new RuntimeException("Failed to request job specification resolution for job id: " + jobId, e);
            }

            executionContext.setCurrentJobStatus(JobStatus.RESOLVED);
        }

        // Claim this job, excluding other agents from doing the same
        try {
            agentJobService.claimJob(jobId, agentClientMetadata);
        } catch (final JobReservationException e) {
            throw new RuntimeException("Failed to claim job id: " + jobId, e);
        }

        // Update context
        executionContext.setCurrentJobStatus(JobStatus.CLAIMED);
        executionContext.setJobSpecification(jobSpecification);
        executionContext.setClaimedJobId(jobId);

        return Events.RESOLVE_JOB_SPECIFICATION_COMPLETE;
    }
}
