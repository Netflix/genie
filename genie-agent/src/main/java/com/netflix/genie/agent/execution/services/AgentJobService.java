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
package com.netflix.genie.agent.execution.services;

import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException;
import com.netflix.genie.agent.execution.exceptions.JobIdUnavailableException;
import com.netflix.genie.agent.execution.exceptions.JobReservationException;
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.dto.v4.AgentJobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

/**
 * Agent side job specification service for resolving and retrieving job specifications from the server.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface AgentJobService {


    /**
     * Request a given job id to be reserved for this job, send along the job details, to be persisted by the server.
     * The request may or may not contain a job id.
     *
     * @param jobRequest          the job parameters and agent metadata
     * @param agentClientMetadata metadata about the client making this request
     * @return the job id assigned by the server (matches the one in the request, if one was present)
     * @throws JobReservationException   if the server failed to fulfill this request
     * @throws JobIdUnavailableException if the id requested has already been used
     */
    String reserveJobId(
        @Valid final AgentJobRequest jobRequest,
        @Valid final AgentClientMetadata agentClientMetadata
    ) throws JobReservationException, JobIdUnavailableException;

    /**
     * Given the parameters supplied by the job request attempt to resolve a job specification on the server.
     *
     * @param id The id of the job to resolve a job specification for
     * @return The job specification
     * @throws JobSpecificationResolutionException if the specification cannot be resolved
     */
    JobSpecification resolveJobSpecification(
        @NotBlank final String id
    ) throws JobSpecificationResolutionException;

    /**
     * Given a job id retrieve the job specification from the server.
     *
     * @param id The id of the job to get the specification for
     * @return The job specification
     * @throws JobSpecificationResolutionException if the specification cannot be retrieved
     */
    JobSpecification getJobSpecification(
        @NotBlank final String id
    ) throws JobSpecificationResolutionException;

    /**
     * Invoke the job specification resolution logic without persisting anything on the server.
     *
     * @param jobRequest The various parameters required to perform the dry run should be contained in this request
     * @return The job specification
     * @throws JobSpecificationResolutionException When an error occurred during attempted resolution
     */
    JobSpecification resolveJobSpecificationDryRun(
        @Valid final AgentJobRequest jobRequest
    ) throws JobSpecificationResolutionException;

    /**
     * Claim a given job, telling the server that this agent is about to begin execution.
     *
     * @param jobId               the id of the job
     * @param agentClientMetadata metadata for the agent claiming this job
     * @throws JobReservationException When the the claim request fails is invalid (reasons include: job already
     *                                 claimed, invalid job ID, failure to reach the server
     */
    void claimJob(
        @NotBlank final String jobId,
        @Valid final AgentClientMetadata agentClientMetadata
    ) throws JobReservationException;

    /**
     * Notify the server of a change of job status.
     *
     * @param jobId            the id of the job
     * @param currentJobStatus the expected current status of the job
     * @param newJobStatus     the new status of the job
     * @param message          an optional message tha accompanies this change of status
     * @throws ChangeJobStatusException when the agent fails to update the job status
     */
    void changeJobStatus(
        @NotBlank final String jobId,
        final JobStatus currentJobStatus,
        final JobStatus newJobStatus,
        final String message
    ) throws ChangeJobStatusException;
}
