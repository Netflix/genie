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

import com.netflix.genie.agent.execution.exceptions.ChangeJobArchiveStatusException;
import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException;
import com.netflix.genie.agent.execution.exceptions.ConfigureException;
import com.netflix.genie.agent.execution.exceptions.GetJobStatusException;
import com.netflix.genie.agent.execution.exceptions.HandshakeException;
import com.netflix.genie.agent.execution.exceptions.JobIdUnavailableException;
import com.netflix.genie.agent.execution.exceptions.JobReservationException;
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.util.Map;

/**
 * Agent side job specification service for resolving and retrieving job specifications from the server.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface AgentJobService {

    /**
     * Perform server handshake. Before going any further, ensure the server is reachable and that this agent is
     * compatible with it.
     *
     * @param agentClientMetadata metadata about the client making this request
     * @throws HandshakeException if the server rejects this client
     */
    void handshake(
        @Valid AgentClientMetadata agentClientMetadata
    ) throws HandshakeException;

    /**
     * Obtain server-provided configuration properties.
     *
     * @param agentClientMetadata metadata about the client making this request
     * @return a map of properties
     * @throws ConfigureException if the server properties cannot be obtained
     */
    Map<String, String> configure(
        @Valid AgentClientMetadata agentClientMetadata
    ) throws ConfigureException;

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
        @Valid AgentJobRequest jobRequest,
        @Valid AgentClientMetadata agentClientMetadata
    ) throws JobReservationException, JobIdUnavailableException;

    /**
     * Given the parameters supplied by the job request attempt to resolve a job specification on the server.
     *
     * @param id The id of the job to resolve a job specification for
     * @return The job specification
     * @throws JobSpecificationResolutionException if the specification cannot be resolved
     */
    JobSpecification resolveJobSpecification(
        @NotBlank String id
    ) throws JobSpecificationResolutionException;

    /**
     * Given a job id retrieve the job specification from the server.
     *
     * @param id The id of the job to get the specification for
     * @return The job specification
     * @throws JobSpecificationResolutionException if the specification cannot be retrieved
     */
    JobSpecification getJobSpecification(
        @NotBlank String id
    ) throws JobSpecificationResolutionException;

    /**
     * Invoke the job specification resolution logic without persisting anything on the server.
     *
     * @param jobRequest The various parameters required to perform the dry run should be contained in this request
     * @return The job specification
     * @throws JobSpecificationResolutionException When an error occurred during attempted resolution
     */
    JobSpecification resolveJobSpecificationDryRun(
        @Valid AgentJobRequest jobRequest
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
        @NotBlank String jobId,
        @Valid AgentClientMetadata agentClientMetadata
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
        @NotBlank String jobId,
        JobStatus currentJobStatus,
        JobStatus newJobStatus,
        String message
    ) throws ChangeJobStatusException;

    /**
     * Retrieve the current job status for the given job id.
     *
     * @param jobId the id of the job
     * @return the job status seen by the server
     * @throws GetJobStatusException when the agent fails to retrieve the job status
     */
    JobStatus getJobStatus(
        @NotBlank String jobId
    ) throws GetJobStatusException;


    /**
     * Notify the server of a change of job files archive status.
     *
     * @param jobId         the id of the job
     * @param archiveStatus the new archive status of the job
     * @throws ChangeJobArchiveStatusException when the agent fails to update the job archive status
     */
    void changeJobArchiveStatus(
        @NotBlank String jobId,
        ArchiveStatus archiveStatus
    ) throws ChangeJobArchiveStatusException;
}
