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
package com.netflix.genie.web.agent.services;

import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieAgentRejectedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieIdAlreadyExistsException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobResolutionRuntimeException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobSpecificationNotFoundException;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.util.Map;

/**
 * A Service to collect the logic for implementing calls from the Agent when a job is launched via the CLI.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface AgentJobService {

    /**
     * Shake hands and allow a client or reject it based on the supplied {@link AgentClientMetadata}.
     *
     * @param agentMetadata The metadata about the agent starting to run a given job
     * @throws GenieAgentRejectedException  If the server rejects the client based on its metadata
     * @throws ConstraintViolationException If the arguments fail validation
     */
    void handshake(@Valid AgentClientMetadata agentMetadata);

    /**
     * Provide configuration properties for an agent that is beginning to execute a job.
     *
     * @param agentMetadata The metadata about the agent starting to run a given job
     * @return a map of properties for the agent
     * @throws ConstraintViolationException If the arguments fail validation
     */
    Map<String, String> getAgentProperties(@Valid AgentClientMetadata agentMetadata);

    /**
     * Reserve a job id and persist job details in the database based on the supplied {@link JobRequest}.
     *
     * @param jobRequest          The job request containing all the metadata needed to reserve a job id
     * @param agentClientMetadata The metadata about the agent driving this job request
     * @return The unique id of the job which was saved in the database
     * @throws GenieIdAlreadyExistsException If the id requested along with the job request is already in use
     * @throws ConstraintViolationException  If the arguments fail validation
     */
    String reserveJobId(@Valid JobRequest jobRequest, @Valid AgentClientMetadata agentClientMetadata);

    /**
     * Resolve the job specification for job identified by the given id. This method will persist the job specification
     * details to the database.
     *
     * @param id The id of the job to resolve the specification for. Must already have a reserved an id in the database
     * @return The job specification if one could be resolved
     * @throws GenieJobResolutionException        On error resolving the job given the input parameters and system state
     * @throws ConstraintViolationException       If the arguments fail validation
     * @throws GenieJobResolutionRuntimeException If job resolution fails due to a runtime error (as opposed to
     *                                            unsatisfiable constraints)
     */
    JobSpecification resolveJobSpecification(
        @NotBlank String id
    ) throws GenieJobResolutionException, GenieJobResolutionRuntimeException;

    /**
     * Get a job specification if has been resolved.
     *
     * @param id the id of the job to retrieve the specification for
     * @return The job specification for the job
     * @throws GenieJobNotFoundException              If the job has not yet had its ID reserved and/or can't be found
     * @throws GenieJobSpecificationNotFoundException If the job exists but the specification hasn't been
     *                                                resolved or saved yet
     * @throws ConstraintViolationException           If the arguments fail validation
     */
    JobSpecification getJobSpecification(@NotBlank String id);

    /**
     * Run the job specification resolution algorithm on the given input but save nothing in the system.
     *
     * @param jobRequest The job request containing all the metadata needed to resolve a job specification
     * @return The job specification that would have been resolved for the given input
     * @throws GenieJobResolutionException  On error resolving the job given the input parameters and system state
     * @throws ConstraintViolationException If the arguments fail validation
     */
    JobSpecification dryRunJobSpecificationResolution(@Valid JobRequest jobRequest) throws GenieJobResolutionException;

    /**
     * Set a job identified by {@code id} to be owned by the agent identified by {@code agentClientMetadata}. The
     * job status in the system will be set to {@link com.netflix.genie.common.dto.JobStatus#CLAIMED}
     *
     * @param id                  The id of the job to claim. Must exist in the system.
     * @param agentClientMetadata The metadata about the client claiming the job
     * @throws GenieJobNotFoundException       if no job with the given {@code id} exists
     * @throws GenieJobAlreadyClaimedException if the job with the given {@code id} already has been claimed
     * @throws GenieInvalidStatusException     if the current job status is not
     *                                         {@link com.netflix.genie.common.dto.JobStatus#RESOLVED}
     * @throws ConstraintViolationException    If the arguments fail validation
     */
    void claimJob(@NotBlank String id, @Valid AgentClientMetadata agentClientMetadata);

    /**
     * Update the status of the job identified with {@code id} to be {@code newStatus} provided that the current status
     * of the job matches {@code newStatus}. Optionally a status message can be provided to provide more details to
     * users. If the {@code newStatus} is {@link JobStatus#RUNNING} the start time will be set. If the {@code newStatus}
     * is a member of {@link JobStatus#getFinishedStatuses()} and the job had a started time set the finished time of
     * the job will be set.
     *
     * @param id               The id of the job to update status for. Must exist in the system.
     * @param currentStatus    The status the caller to this API thinks the job currently has
     * @param newStatus        The new status the caller would like to update the status to
     * @param newStatusMessage An optional status message to associate with this change
     * @throws GenieJobNotFoundException    if no job with the given {@code id} exists
     * @throws GenieInvalidStatusException  if the current status of the job identified by {@code id} in the system
     *                                      doesn't match the supplied {@code currentStatus}.
     *                                      Also if the {@code currentStatus} equals the {@code newStatus}.
     * @throws ConstraintViolationException If the arguments fail validation
     */
    void updateJobStatus(
        @NotBlank String id,
        JobStatus currentStatus,
        JobStatus newStatus,
        @Nullable String newStatusMessage
    );

    /**
     * Retrieve the status of the job identified with {@code id}.
     *
     * @param id The id of the job to look up.
     * @return the current status of the job, as seen by this node
     * @throws GenieJobNotFoundException    if no job with the given {@code id} exists
     * @throws ConstraintViolationException If the arguments fail validation
     */
    JobStatus getJobStatus(@NotBlank String id);

    /**
     * Update the archive status status of the job identified with {@code id} to be {@code newStatus}.
     * Notice this is a 'blind write', the currently persisted value will always be overwritten.
     *
     * @param id               The id of the job to update status for. Must exist in the system.
     * @param newArchiveStatus The new archive status the caller would like to update the status to
     * @throws GenieJobNotFoundException    if no job with the given {@code id} exists
     * @throws ConstraintViolationException If the arguments fail validation
     */
    void updateJobArchiveStatus(
        @NotBlank String id,
        ArchiveStatus newArchiveStatus
    );
}
