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
package com.netflix.genie.web.data.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.dtos.AgentClientMetadata;
import com.netflix.genie.common.internal.dtos.Application;
import com.netflix.genie.common.internal.dtos.ApplicationRequest;
import com.netflix.genie.common.internal.dtos.ApplicationStatus;
import com.netflix.genie.common.internal.dtos.ArchiveStatus;
import com.netflix.genie.common.internal.dtos.Cluster;
import com.netflix.genie.common.internal.dtos.ClusterRequest;
import com.netflix.genie.common.internal.dtos.ClusterStatus;
import com.netflix.genie.common.internal.dtos.Command;
import com.netflix.genie.common.internal.dtos.CommandRequest;
import com.netflix.genie.common.internal.dtos.CommandStatus;
import com.netflix.genie.common.internal.dtos.CommonResource;
import com.netflix.genie.common.internal.dtos.Criterion;
import com.netflix.genie.common.internal.dtos.FinishedJob;
import com.netflix.genie.common.internal.dtos.JobRequest;
import com.netflix.genie.common.internal.dtos.JobSpecification;
import com.netflix.genie.common.internal.dtos.JobStatus;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException;
import com.netflix.genie.web.data.services.impl.jpa.queries.aggregates.JobInfoAggregate;
import com.netflix.genie.web.dtos.JobSubmission;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.exceptions.checked.PreconditionFailedException;
import com.netflix.genie.web.services.AttachmentService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Service API for all Genie persistence related operations.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface PersistenceService {

    //region Application APIs

    /**
     * Save a new application.
     *
     * @param applicationRequest The {@link ApplicationRequest} containing the metadata of the application to create
     * @return The unique id of the application that was created
     * @throws IdAlreadyExistsException If the ID is already used by another application
     */
    String saveApplication(@Valid ApplicationRequest applicationRequest) throws IdAlreadyExistsException;

    /**
     * Get the application metadata for given id.
     *
     * @param id unique id for application configuration to get. Not null/empty.
     * @return The {@link Application}
     * @throws NotFoundException if no application with {@literal id} exists
     */
    Application getApplication(@NotBlank String id) throws NotFoundException;

    /**
     * Find applications which match the given filter criteria.
     *
     * @param name     Name of the application. Can be null or empty.
     * @param user     The user who created the application. Can be null/empty
     * @param statuses The statuses of the applications to find. Can be null.
     * @param tags     Tags allocated to this application
     * @param type     The type of the application to find
     * @param page     The page requested for the search results
     * @return The page of found applications
     */
    Page<Application> findApplications(
        @Nullable String name,
        @Nullable String user,
        @Nullable Set<ApplicationStatus> statuses,
        @Nullable Set<String> tags,
        @Nullable String type,
        Pageable page
    );

    /**
     * Update an {@link Application}.
     *
     * @param id        The id of the application to update
     * @param updateApp Information to update for the application configuration with
     * @throws NotFoundException           If no {@link Application} with {@literal id} exists
     * @throws PreconditionFailedException If {@literal id} and {@literal updateApp} id don't match
     */
    void updateApplication(
        @NotBlank String id,
        @Valid Application updateApp
    ) throws NotFoundException, PreconditionFailedException;

    /**
     * Delete all applications from the system.
     *
     * @throws PreconditionFailedException When any {@link Application} is still associated with a command
     */
    void deleteAllApplications() throws PreconditionFailedException;

    /**
     * Delete an {@link Application} from the system.
     *
     * @param id unique id of the application to delete
     * @throws PreconditionFailedException When the {@link Application} is still used by any command
     */
    void deleteApplication(@NotBlank String id) throws PreconditionFailedException;

    /**
     * Get all the commands the application with given id is associated with.
     *
     * @param id       The id of the application to get the commands for.
     * @param statuses The desired status(es) to filter the associated commands for
     * @return The commands the application is used by
     * @throws NotFoundException If no {@link Application} with {@literal id} exists
     */
    Set<Command> getCommandsForApplication(
        @NotBlank String id,
        @Nullable Set<CommandStatus> statuses
    ) throws NotFoundException;

    /**
     * Delete any unused applications that were created before the given time.
     * Unused means they aren't linked to any other resources in the Genie system like jobs or commands and therefore
     * referential integrity is maintained.
     *
     * @param createdThreshold The instant in time that any application had to be created before (exclusive) to be
     *                         considered for deletion. Presents ability to filter out newly created applications if
     *                         desired.
     * @param batchSize        The maximum number of applications to delete in a single transaction
     * @return The number of successfully deleted applications
     */
    long deleteUnusedApplications(@NotNull Instant createdThreshold, @Min(1) int batchSize);
    //endregion

    //region Cluster APIs

    /**
     * Save a {@link Cluster}.
     *
     * @param clusterRequest The cluster information to save
     * @return The id of the saved cluster
     * @throws IdAlreadyExistsException If a {@link Cluster} with the given {@literal id} already exists
     */
    String saveCluster(@Valid ClusterRequest clusterRequest) throws IdAlreadyExistsException;

    /**
     * Get the {@link Cluster} identified by the given {@literal id}.
     *
     * @param id unique id of the {@link Cluster} to get
     * @return The {@link Cluster}
     * @throws NotFoundException If no {@link Cluster} with {@literal id} exists
     */
    Cluster getCluster(@NotBlank String id) throws NotFoundException;

    /**
     * Find and {@link Cluster}s that match the given parameters. Null or empty parameters are ignored.
     *
     * @param name          cluster name
     * @param statuses      {@link ClusterStatus} that clusters must be in to be matched
     * @param tags          tags attached to this cluster
     * @param minUpdateTime min time when cluster was updated
     * @param maxUpdateTime max time when cluster was updated
     * @param page          The page to get
     * @return All the clusters matching the criteria
     */
    Page<Cluster> findClusters(
        @Nullable String name,
        @Nullable Set<ClusterStatus> statuses,
        @Nullable Set<String> tags,
        @Nullable Instant minUpdateTime,
        @Nullable Instant maxUpdateTime,
        Pageable page
    );

    /**
     * Update a {@link Cluster} with the given information.
     *
     * @param id            The id of the cluster to update
     * @param updateCluster The information to update the cluster with
     * @throws NotFoundException           If no {@link Cluster} with {@literal id} exists
     * @throws PreconditionFailedException If the {@literal id} doesn't match the {@literal updateCluster} id
     */
    void updateCluster(
        @NotBlank String id,
        @Valid Cluster updateCluster
    ) throws NotFoundException, PreconditionFailedException;

    /**
     * Delete all clusters from database.
     *
     * @throws PreconditionFailedException If the cluster is still associated with any job
     */
    void deleteAllClusters() throws PreconditionFailedException;

    /**
     * Delete a {@link Cluster} by id.
     *
     * @param id unique id of the cluster to delete
     * @throws PreconditionFailedException If the cluster is still associated with any job
     */
    void deleteCluster(@NotBlank String id) throws PreconditionFailedException;

    /**
     * Delete all clusters that are in one of the given states, aren't attached to any jobs and were created before
     * the given time.
     *
     * @param deleteStatuses          The set of {@link ClusterStatus} a cluster must be in to be considered for
     *                                deletion.
     * @param clusterCreatedThreshold The instant in time before which a cluster must have been created to be
     *                                considered for deletion. Exclusive.
     * @param batchSize               The maximum number of clusters to delete in a single transaction
     * @return The number of clusters deleted
     */
    long deleteUnusedClusters(
        Set<ClusterStatus> deleteStatuses,
        @NotNull Instant clusterCreatedThreshold,
        @Min(1) int batchSize
    );

    /**
     * Find all the {@link Cluster}'s that match the given {@link Criterion}.
     *
     * @param criterion        The {@link Criterion} supplied that each cluster needs to completely match to be returned
     * @param addDefaultStatus {@literal true} if the a default status should be added to the supplied
     *                         {@link Criterion} if the supplied criterion doesn't already have a status
     * @return All the {@link Cluster}'s which matched the {@link Criterion}
     */
    Set<Cluster> findClustersMatchingCriterion(@Valid Criterion criterion, boolean addDefaultStatus);

    /**
     * Find all the {@link Cluster}'s that match any of the given {@link Criterion}.
     *
     * @param criteria         The set of {@link Criterion} supplied that a cluster needs to completely match at least
     *                         one of to be returned
     * @param addDefaultStatus {@literal true} if the a default status should be added to the supplied
     *                         {@link Criterion} if the supplied criterion doesn't already have a status
     * @return All the {@link Cluster}'s which matched the {@link Criterion}
     */
    Set<Cluster> findClustersMatchingAnyCriterion(@NotEmpty Set<@Valid Criterion> criteria, boolean addDefaultStatus);
    //endregion

    //region Command APIs

    /**
     * Save a {@link Command} in the system based on the given {@link CommandRequest}.
     *
     * @param commandRequest encapsulates the command configuration information to create
     * @return The id of the command that was saved
     * @throws IdAlreadyExistsException If there was a conflict on the unique id for the command
     */
    String saveCommand(@Valid CommandRequest commandRequest) throws IdAlreadyExistsException;

    /**
     * Get the metadata for the {@link Command} identified by the id.
     *
     * @param id unique id for command to get. Not null/empty.
     * @return The command
     * @throws NotFoundException If no {@link Command} exists with the given {@literal id}
     */
    Command getCommand(@NotBlank String id) throws NotFoundException;

    /**
     * Find commands matching the given filter criteria.
     *
     * @param name     Name of command config
     * @param user     The name of the user who created the configuration
     * @param statuses The status of the commands to get. Can be null.
     * @param tags     tags allocated to this command
     * @param page     The page of results to get
     * @return All the commands matching the specified criteria
     */
    Page<Command> findCommands(
        @Nullable String name,
        @Nullable String user,
        @Nullable Set<CommandStatus> statuses,
        @Nullable Set<String> tags,
        Pageable page
    );

    /**
     * Update a {@link Command}.
     *
     * @param id            The id of the command configuration to update. Not null or empty.
     * @param updateCommand contains the information to update the command with
     * @throws NotFoundException           If no {@link Command} exists with the given {@literal id}
     * @throws PreconditionFailedException When the {@literal id} doesn't match the id of {@literal updateCommand}
     */
    void updateCommand(
        @NotBlank String id,
        @Valid Command updateCommand
    ) throws NotFoundException, PreconditionFailedException;

    /**
     * Delete all commands from the system.
     *
     * @throws PreconditionFailedException If any constraint is violated trying to delete a command
     */
    void deleteAllCommands() throws PreconditionFailedException;

    /**
     * Delete a {@link Command} from system.
     *
     * @param id unique if of the command to delete
     * @throws NotFoundException If no {@link Command} exists with the given {@literal id}
     */
    void deleteCommand(@NotBlank String id) throws NotFoundException;

    /**
     * Add applications for the command.
     *
     * @param id             The id of the command to add the application file to. Not null/empty/blank.
     * @param applicationIds The ids of the applications to add. Not null.
     * @throws NotFoundException           If no {@link Command} exists with the given {@literal id} or one of the
     *                                     applications doesn't exist
     * @throws PreconditionFailedException If an application with one of the ids is already associated with the command
     */
    void addApplicationsForCommand(
        @NotBlank String id,
        @NotEmpty List<@NotBlank String> applicationIds
    ) throws NotFoundException, PreconditionFailedException;

    /**
     * Set the applications for the command.
     *
     * @param id             The id of the command to add the application file to. Not null/empty/blank.
     * @param applicationIds The ids of the applications to set. Not null.
     * @throws NotFoundException           If no {@link Command} exists with the given {@literal id} or one of the
     *                                     applications doesn't exist
     * @throws PreconditionFailedException If there are duplicate application ids in the list
     */
    void setApplicationsForCommand(
        @NotBlank String id,
        @NotNull List<@NotBlank String> applicationIds
    ) throws NotFoundException, PreconditionFailedException;

    /**
     * Get the applications for a given command.
     *
     * @param id The id of the command to get the application for. Not null/empty/blank.
     * @return The applications or exception if none exist.
     * @throws NotFoundException If no {@link Command} exists with the given {@literal id}
     */
    List<Application> getApplicationsForCommand(String id) throws NotFoundException;

    /**
     * Remove all the applications from the command.
     *
     * @param id The id of the command to remove the application from. Not null/empty/blank.
     * @throws NotFoundException           If no {@link Command} exists with the given {@literal id}
     * @throws PreconditionFailedException If applications are unable to be removed
     */
    void removeApplicationsForCommand(@NotBlank String id) throws NotFoundException, PreconditionFailedException;

    /**
     * Remove the application from the command.
     *
     * @param id    The id of the command to remove the application from. Not null/empty/blank.
     * @param appId The id of the application to remove. Not null/empty/blank
     * @throws NotFoundException If no {@link Command} exists with the given {@literal id}
     */
    void removeApplicationForCommand(@NotBlank String id, @NotBlank String appId) throws NotFoundException;

    /**
     * Get all the clusters the command with given id is associated with.
     *
     * @param id       The id of the command to get the clusters for.
     * @param statuses The status of the clusters returned
     * @return The clusters the command is available on.
     * @throws NotFoundException If no {@link Command} exists with the given {@literal id}
     */
    Set<Cluster> getClustersForCommand(
        @NotBlank String id,
        @Nullable Set<ClusterStatus> statuses
    ) throws NotFoundException;

    /**
     * For the given command {@literal id} return the Cluster {@link Criterion} in priority order that is currently
     * associated with this command if any.
     *
     * @param id The id of the command to get the criteria for
     * @return The cluster criteria in priority order
     * @throws NotFoundException If no command with {@literal id} exists
     */
    List<Criterion> getClusterCriteriaForCommand(String id) throws NotFoundException;

    /**
     * Add a new {@link Criterion} to the existing list of cluster criteria for the command identified by {@literal id}.
     * This new criterion will be the lowest priority criterion.
     *
     * @param id        The id of the command to add to
     * @param criterion The new {@link Criterion} to add
     * @throws NotFoundException If no command with {@literal id} exists
     */
    void addClusterCriterionForCommand(String id, @Valid Criterion criterion) throws NotFoundException;

    /**
     * Add a new {@link Criterion} to the existing list of cluster criteria for the command identified by {@literal id}.
     * The {@literal priority} is the place in the list this new criterion should be placed. A value of {@literal 0}
     * indicates it should be placed at the front of the list with the highest possible priority. {@literal 1} would be
     * second in the list etc. If {@literal priority} is {@literal >} the current size of the cluster criteria list
     * this new criterion will be placed at the end as the lowest priority item.
     *
     * @param id        The id of the command to add to
     * @param criterion The new {@link Criterion} to add
     * @param priority  The place in the existing cluster criteria list this new criterion should be placed. Min 0.
     * @throws NotFoundException If no command with {@literal id} exists
     */
    void addClusterCriterionForCommand(
        String id,
        @Valid Criterion criterion,
        @Min(0) int priority
    ) throws NotFoundException;

    /**
     * For the command identified by {@literal id} reset the entire list of cluster criteria to match the contents of
     * {@literal clusterCriteria}.
     *
     * @param id              The id of the command to set the cluster criteria for
     * @param clusterCriteria The priority list of {@link Criterion} to set
     * @throws NotFoundException If no command with {@literal id} exists
     */
    void setClusterCriteriaForCommand(String id, List<@Valid Criterion> clusterCriteria) throws NotFoundException;

    /**
     * Remove the {@link Criterion} with the given {@literal priority} from the current list of cluster criteria
     * associated with the command identified by {@literal id}. A value of {@literal 0} for {@literal priority}
     * will result in the first element in the list being removed, {@literal 1} the second element and so on.
     *
     * @param id       The id of the command to remove the criterion from
     * @param priority The priority of the criterion to remove
     * @throws NotFoundException If no command with {@literal id} exists
     */
    void removeClusterCriterionForCommand(String id, @Min(0) int priority) throws NotFoundException;

    /**
     * Remove all the {@link Criterion} currently associated with the command identified by {@literal id}.
     *
     * @param id The id of the command to remove the criteria from
     * @throws NotFoundException If no command with {@literal id} exists
     */
    void removeAllClusterCriteriaForCommand(String id) throws NotFoundException;

    /**
     * Find all the {@link Command}'s that match the given {@link Criterion}.
     *
     * @param criterion        The {@link Criterion} supplied that each command needs to completely match to be returned
     * @param addDefaultStatus {@literal true} if a default status should be added to the supplied criterion if a
     *                         status isn't already present
     * @return All the {@link Command}'s which matched the {@link Criterion}
     */
    Set<Command> findCommandsMatchingCriterion(@Valid Criterion criterion, boolean addDefaultStatus);

    /**
     * Update the status of a command to the {@literal desiredStatus} if its status is in {@literal currentStatuses},
     * it was created before {@literal commandCreatedThreshold} and it hasn't been used in any job.
     *
     * @param desiredStatus           The new status the matching commands should have
     * @param commandCreatedThreshold The instant in time which a command must have been created before to be
     *                                considered for update. Exclusive
     * @param currentStatuses         The set of current statuses a command must have to be considered for update
     * @param batchSize               The maximum number of commands to update in a single transaction
     * @return The number of commands whose statuses were updated to {@literal desiredStatus}
     */
    int updateStatusForUnusedCommands(
        CommandStatus desiredStatus,
        Instant commandCreatedThreshold,
        Set<CommandStatus> currentStatuses,
        int batchSize
    );

    /**
     * Bulk delete commands from the database where their status is in {@literal deleteStatuses} they were created
     * before {@literal commandCreatedThreshold} and they aren't attached to any jobs still in the database.
     *
     * @param deleteStatuses          The set of statuses a command must be in in order to be considered for deletion
     * @param commandCreatedThreshold The instant in time a command must have been created before to be considered for
     *                                deletion. Exclusive.
     * @param batchSize               The maximum number of commands to delete in a single transaction
     * @return The number of commands that were deleted
     */
    long deleteUnusedCommands(
        Set<CommandStatus> deleteStatuses,
        @NotNull Instant commandCreatedThreshold,
        @Min(1) int batchSize
    );
    //endregion

    //region Job APIs

    //region V3 Job APIs

    /**
     * Get job information for given job id.
     *
     * @param id id of job to look up
     * @return the job
     * @throws GenieException if there is an error
     */
    Job getJob(@NotBlank String id) throws GenieException;

    /**
     * Get job execution for given job id.
     *
     * @param id id of job execution to look up
     * @return the job
     * @throws GenieException if there is an error
     */
    JobExecution getJobExecution(@NotBlank String id) throws GenieException;

    /**
     * Get the metadata about a job.
     *
     * @param id The id of the job to get metadata for
     * @return The metadata for a job
     * @throws GenieException If any error occurs
     */
    JobMetadata getJobMetadata(@NotBlank String id) throws GenieException;

    /**
     * Find jobs which match the given filter criteria.
     *
     * @param id               id for job
     * @param name             name of job
     * @param user             user who submitted job
     * @param statuses         statuses of job
     * @param tags             tags for the job
     * @param clusterName      name of cluster for job
     * @param clusterId        id of cluster for job
     * @param commandName      name of the command run in the job
     * @param commandId        id of the command run in the job
     * @param minStarted       The time which the job had to start after in order to be return (inclusive)
     * @param maxStarted       The time which the job had to start before in order to be returned (exclusive)
     * @param minFinished      The time which the job had to finish after in order to be return (inclusive)
     * @param maxFinished      The time which the job had to finish before in order to be returned (exclusive)
     * @param grouping         The job grouping to search for
     * @param groupingInstance The job grouping instance to search for
     * @param page             Page information of job to get
     * @return Metadata information on jobs which match the criteria
     */
    @SuppressWarnings("checkstyle:parameternumber")
    Page<JobSearchResult> findJobs(
        @Nullable String id,
        @Nullable String name,
        @Nullable String user,
        @Nullable Set<com.netflix.genie.common.dto.JobStatus> statuses,
        @Nullable Set<String> tags,
        @Nullable String clusterName,
        @Nullable String clusterId,
        @Nullable String commandName,
        @Nullable String commandId,
        @Nullable Instant minStarted,
        @Nullable Instant maxStarted,
        @Nullable Instant minFinished,
        @Nullable Instant maxFinished,
        @Nullable String grouping,
        @Nullable String groupingInstance,
        @NotNull Pageable page
    );
    //endregion

    //region V4 Job APIs

    /**
     * This method will delete a chunk of jobs whose creation time is earlier than the given date.
     *
     * @param creationThreshold The instant in time before which all jobs should be deleted
     * @param excludeStatuses   The set of statuses that should be excluded from deletion if a job is in one of these
     *                          statuses
     * @param batchSize         The maximum number of jobs that should be deleted per query
     * @return the number of deleted jobs
     */
    long deleteJobsCreatedBefore(
        @NotNull Instant creationThreshold,
        @NotNull Set<JobStatus> excludeStatuses,
        @Min(1) int batchSize
    );

    /**
     * Save the given job submission information in the underlying data store.
     * <p>
     * The job record will be created with initial state of {@link JobStatus#RESERVED} meaning that the unique id
     * returned by this API has been reserved within Genie and no other job can use it. If
     * {@link JobSubmission} contains some attachments these attachments will be persisted to a
     * configured storage system (i.e. local disk, S3, etc) and added to the set of dependencies for the job.
     * The underlying attachment storage system must be accessible by the agent process configured by the system. For
     * example if the server is set up to write attachments to local disk but the agent is not running locally but
     * instead on the remote system it will not be able to access those attachments (as dependencies) and fail.
     * See {@link AttachmentService} for more information.
     *
     * @param jobSubmission All the information the system has gathered regarding the job submission from the user
     *                      either via the API or via the agent CLI
     * @return The unique id of the job within the Genie ecosystem
     * @throws IdAlreadyExistsException If the id the user requested already exists in the system for another job
     */
    @Nonnull
    String saveJobSubmission(@Valid JobSubmission jobSubmission) throws IdAlreadyExistsException;

    /**
     * Get the original request for a job.
     *
     * @param id The unique id of the job to get
     * @return The job request if one was found
     * @throws NotFoundException If no job with {@code id} exists
     */
    JobRequest getJobRequest(@NotBlank String id) throws NotFoundException;

    /**
     * Save the given resolved details for a job. Sets the job status to {@link JobStatus#RESOLVED}.
     *
     * @param id          The id of the job
     * @param resolvedJob The resolved information for the job
     * @throws NotFoundException When the job identified by {@code id} can't be found and the
     *                           specification can't be saved or one of the resolved cluster, command
     *                           or applications not longer exist in the system.
     */
    void saveResolvedJob(@NotBlank String id, @Valid ResolvedJob resolvedJob) throws NotFoundException;

    /**
     * Get the saved job specification for the given job. If the job hasn't had a job specification resolved an empty
     * {@link Optional} will be returned.
     *
     * @param id The id of the job
     * @return The {@link JobSpecification} if one is present else an empty {@link Optional}
     * @throws NotFoundException If no job with {@code id} exists
     */
    Optional<JobSpecification> getJobSpecification(@NotBlank String id) throws NotFoundException;

    /**
     * Set a job identified by {@code id} to be owned by the agent identified by {@code agentClientMetadata}. The
     * job status in the system will be set to {@link JobStatus#CLAIMED}
     *
     * @param id                  The id of the job to claim. Must exist in the system.
     * @param agentClientMetadata The metadata about the client claiming the job
     * @throws NotFoundException               if no job with the given {@code id} exists
     * @throws GenieJobAlreadyClaimedException if the job with the given {@code id} already has been claimed
     * @throws GenieInvalidStatusException     if the current job status is not {@link JobStatus#RESOLVED}
     */
    void claimJob(
        @NotBlank String id,
        @Valid AgentClientMetadata agentClientMetadata
    ) throws NotFoundException, GenieJobAlreadyClaimedException, GenieInvalidStatusException;

    /**
     * Update the status of the job identified with {@code id} to be {@code newStatus} provided that the current status
     * of the job matches {@code newStatus}. Optionally a status message can be provided to provide more details to
     * users. If the {@code newStatus} is {@link JobStatus#RUNNING} the start time will be set. If the {@code newStatus}
     * is a member of {@link JobStatus#getFinishedStatuses()} and the job had a started time set the finished time of
     * the job will be set. If the {@literal currentStatus} is different from what the source of truth thinks this
     * function will skip the update and just return the current source of truth value.
     *
     * @param id               The id of the job to update status for. Must exist in the system.
     * @param currentStatus    The status the caller to this API thinks the job currently has
     * @param newStatus        The new status the caller would like to update the status to
     * @param newStatusMessage An optional status message to associate with this change
     * @return The job status in the source of truth
     * @throws NotFoundException if no job with the given {@code id} exists
     */
    JobStatus updateJobStatus(
        @NotBlank String id,
        @NotNull JobStatus currentStatus,
        @NotNull JobStatus newStatus,
        @Nullable String newStatusMessage
    ) throws NotFoundException;

    /**
     * Update the status and status message of the job.
     *
     * @param id            The id of the job to update the status for.
     * @param archiveStatus The updated archive status for the job.
     * @throws NotFoundException If no job with the given {@code id} exists
     */
    void updateJobArchiveStatus(
        @NotBlank(message = "No job id entered. Unable to update.") String id,
        @NotNull(message = "Status cannot be null.") ArchiveStatus archiveStatus
    ) throws NotFoundException;

    /**
     * Get the status for a job with the given {@code id}.
     *
     * @param id The id of the job to get status for
     * @return The job status
     * @throws NotFoundException If no job with the given {@code id} exists
     */
    JobStatus getJobStatus(@NotBlank String id) throws NotFoundException;

    /**
     * Get the archive status for a job with the given {@code id}.
     *
     * @param id The id of the job to get status for
     * @return The job archive status
     * @throws NotFoundException If no job with the given {@code id} exists
     */
    ArchiveStatus getJobArchiveStatus(@NotBlank String id) throws NotFoundException;

    /**
     * Get the location a job directory was archived to if at all.
     *
     * @param id The id of the job to get the location for
     * @return The archive location or {@link Optional#empty()}
     * @throws NotFoundException When there is no job with id {@code id}
     */
    Optional<String> getJobArchiveLocation(@NotBlank String id) throws NotFoundException;

    /**
     * Get a DTO representing a finished job.
     *
     * @param id The id of the job to retrieve
     * @return the finished job
     * @throws NotFoundException           if no job with the given {@code id} exists
     * @throws GenieInvalidStatusException if the current status of the job is not final
     */
    FinishedJob getFinishedJob(@NotBlank String id) throws NotFoundException, GenieInvalidStatusException;

    /**
     * Get whether the job with the given ID was submitted via the REST API or other mechanism.
     *
     * @param id The id of the job. Not blank.
     * @return {@literal true} if the job was submitted via the API. {@literal false} otherwise
     * @throws NotFoundException If no job with {@literal id} exists
     */
    boolean isApiJob(@NotBlank String id) throws NotFoundException;

    /**
     * Get the cluster the job used or is using.
     *
     * @param id The id of the job to get the cluster for
     * @return The {@link Cluster}
     * @throws NotFoundException If either the job or the cluster is not found
     */
    Cluster getJobCluster(@NotBlank String id) throws NotFoundException;

    /**
     * Get the command the job used or is using.
     *
     * @param id The id of the job to get the command for
     * @return The {@link Command}
     * @throws NotFoundException If either the job or the command is not found
     */
    Command getJobCommand(@NotBlank String id) throws NotFoundException;

    /**
     * Get the applications the job used or is currently using.
     *
     * @param id The id of the job to get the applications for
     * @return The {@link Application}s
     * @throws NotFoundException If either the job or the applications were not found
     */
    List<Application> getJobApplications(@NotBlank String id) throws NotFoundException;

    /**
     * Get the count of 'active' jobs for a given user across all instances.
     *
     * @param user The user name
     * @return the number of active jobs for a given user
     */
    long getActiveJobCountForUser(@NotBlank String user);

    /**
     * Get a map of summaries of resources usage for each user with at least one active job.
     *
     * @param statuses The set of {@link JobStatus} a job must be in to be considered in this request
     * @param api      Whether the job was submitted via the api ({@literal true}) or the agent cli ({@literal false})
     * @return a map of user resources summaries, keyed on user name
     */
    Map<String, UserResourcesSummary> getUserResourcesSummaries(Set<JobStatus> statuses, boolean api);

    /**
     * Get the amount of memory currently used on the given host by Genie jobs in any of the following states.
     * <p>
     * {@link JobStatus#CLAIMED}
     * {@link JobStatus#INIT}
     * {@link JobStatus#RUNNING}
     *
     * @param hostname The hostname to get the memory for
     * @return The amount of memory being used in MB by all jobs
     */
    long getUsedMemoryOnHost(@NotBlank String hostname);

    /**
     * Get the set of active jobs.
     *
     * @return The set of job ids which currently have a status which is considered active
     */
    Set<String> getActiveJobs();

    /**
     * Get the set of jobs in that have not reached CLAIMED state.
     *
     * @return The set of job ids for jobs which are active but haven't been claimed
     */
    Set<String> getUnclaimedJobs();

    /**
     * Get all the aggregate metadata information about jobs running on a given hostname.
     *
     * @param hostname The hostname the agent is running the job on
     * @return A {@link JobInfoAggregate} containing the metadata information
     */
    JobInfoAggregate getHostJobInformation(@NotBlank String hostname);

    /**
     * Get the set of jobs (agent only) whose state is in {@code statuses} and archive status is in
     * {@code archiveStatuses} and last updated before {@code updated}.
     *
     * @param statuses        the set of job statuses
     * @param archiveStatuses the set of job archive statuses
     * @param updated         the threshold of last update
     * @return a set of job ids
     */
    Set<String> getJobsWithStatusAndArchiveStatusUpdatedBefore(
        @NotEmpty Set<JobStatus> statuses,
        @NotEmpty Set<ArchiveStatus> archiveStatuses,
        @NotNull Instant updated
    );

    /**
     * Update the requested launcher extension field for this job.
     *
     * @param id                The id of the job to update the laucher extension for.
     * @param launcherExtension The updated requested launcher extension JSON blob.
     * @throws NotFoundException If no job with the given {@code id} exists
     */
    void updateRequestedLauncherExt(
        @NotBlank(message = "No job id entered. Unable to update.") String id,
        @NotNull(message = "Status cannot be null.") JsonNode launcherExtension
    ) throws NotFoundException;

    /**
     * Get the command the job used or is using.
     *
     * @param id The id of the job to get the command for
     * @return The {@link JsonNode} passed to the launcher at launch, or a
     * {@link com.fasterxml.jackson.databind.node.NullNode} if none was saved
     * @throws NotFoundException If no job with the given {@code id} exists
     */
    JsonNode getRequestedLauncherExt(@NotBlank String id) throws NotFoundException;

    /**
     * Update the launcher extension field for this job.
     *
     * @param id                The id of the job to update the launcher extension for.
     * @param launcherExtension The updated launcher extension JSON blob.
     * @throws NotFoundException If no job with the given {@code id} exists
     */
    void updateLauncherExt(
        @NotBlank(message = "No job id entered. Unable to update.") String id,
        @NotNull(message = "Status cannot be null.") JsonNode launcherExtension
    ) throws NotFoundException;

    /**
     * Get the job requested launcher extension.
     *
     * @param id The id of the job to get the command for
     * @return The {@link JsonNode} emitted by the launcher at launch
     * @throws NotFoundException If no job with the given {@code id} exists
     */
    JsonNode getLauncherExt(@NotBlank String id) throws NotFoundException;

    //endregion
    //endregion

    //region General CommonResource APIs

    /**
     * Add configuration files to the existing set for a resource.
     *
     * @param <R>           The resource type that the configs should be associated with
     * @param id            The id of the resource to add the configuration file to. Not null/empty/blank.
     * @param configs       The configuration files to add. Max file length is 1024 characters.
     * @param resourceClass The class of the resource
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> void addConfigsToResource(
        @NotBlank String id,
        Set<@Size(max = 1024) String> configs,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Get the set of configuration files associated with the resource with the given id.
     *
     * @param <R>           The resource type that the configs are associated with
     * @param id            The id of the resource to get the configuration files for. Not null/empty/blank.
     * @param resourceClass The class of the resource
     * @return The set of configuration files as paths
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> Set<String> getConfigsForResource(
        @NotBlank String id,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Update the set of configuration files associated with the resource with the given id.
     *
     * @param <R>           The resource type that the configs should be associated with
     * @param id            The id of the resource to update the configuration files for. Not null/empty/blank.
     * @param configs       The configuration files to replace existing configurations with. Not null/empty.
     * @param resourceClass The class of the resource
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> void updateConfigsForResource(
        @NotBlank String id,
        Set<@Size(max = 1024) String> configs,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Remove all configuration files from the resource.
     *
     * @param <R>           The resource type that the configs should be associated with
     * @param id            The id of the resource to remove the configuration file from. Not null/empty/blank.
     * @param resourceClass The class of the resource
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> void removeAllConfigsForResource(
        @NotBlank String id,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Remove a configuration file from the given resource.
     *
     * @param <R>           The resource type that the configs should be associated with
     * @param id            The id of the resource to remove the configuration file from. Not null/empty/blank.
     * @param config        The configuration file to remove. Not null/empty/blank.
     * @param resourceClass The class of the resource
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> void removeConfigForResource(
        @NotBlank String id,
        @NotBlank String config,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Add dependency files to the existing set for a resource.
     *
     * @param <R>           The resource type that the dependencies should be associated with
     * @param id            The id of the resource to add the dependency file to. Not null/empty/blank.
     * @param dependencies  The dependency files to add. Max file length is 1024 characters.
     * @param resourceClass The class of the resource
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> void addDependenciesToResource(
        @NotBlank String id,
        Set<@Size(max = 1024) String> dependencies,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Get the set of dependency files associated with the resource with the given id.
     *
     * @param <R>           The resource type that the dependencies are associated with
     * @param id            The id of the resource to get the dependency files for. Not null/empty/blank.
     * @param resourceClass The class of the resource
     * @return The set of dependency files as paths
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> Set<String> getDependenciesForResource(
        @NotBlank String id,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Update the set of dependency files associated with the resource with the given id.
     *
     * @param <R>           The resource type that the dependencies should be associated with
     * @param id            The id of the resource to update the dependency files for. Not null/empty/blank.
     * @param dependencies  The dependency files to replace existing dependencys with. Not null/empty.
     * @param resourceClass The class of the resource
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> void updateDependenciesForResource(
        @NotBlank String id,
        Set<@Size(max = 1024) String> dependencies,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Remove all dependency files from the resource.
     *
     * @param <R>           The resource type that the dependencies should be associated with
     * @param id            The id of the resource to remove the dependency file from. Not null/empty/blank.
     * @param resourceClass The class of the resource
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> void removeAllDependenciesForResource(
        @NotBlank String id,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Remove a dependency file from the given resource.
     *
     * @param <R>           The resource type that the dependencies should be associated with
     * @param id            The id of the resource to remove the dependency file from. Not null/empty/blank.
     * @param dependency    The dependency file to remove. Not null/empty/blank.
     * @param resourceClass The class of the resource
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> void removeDependencyForResource(
        @NotBlank String id,
        @NotBlank String dependency,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Add tags to the existing set for a resource.
     *
     * @param <R>           The resource type that the tags should be associated with
     * @param id            The id of the resource to add the tags to. Not null/empty/blank.
     * @param tags          The tags to add. Max file length is 255 characters.
     * @param resourceClass The class of the resource
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> void addTagsToResource(
        @NotBlank String id,
        Set<@Size(max = 255) String> tags,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Get the set of tags associated with the resource with the given id.
     *
     * @param <R>           The resource type that the tags are associated with
     * @param id            The id of the resource to get the tags for. Not null/empty/blank.
     * @param resourceClass The class of the resource
     * @return The set of tags
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> Set<String> getTagsForResource(
        @NotBlank String id,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Update the set of tags associated with the resource with the given id.
     *
     * @param <R>           The resource type that the tags should be associated with
     * @param id            The id of the resource to update the tags for. Not null/empty/blank.
     * @param tags          The tags to replace existing tags with. Not null/empty.
     * @param resourceClass The class of the resource
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> void updateTagsForResource(
        @NotBlank String id,
        Set<@Size(max = 255) String> tags,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Remove all tags from the resource.
     *
     * @param <R>           The resource type that the tags should be associated with
     * @param id            The id of the resource to remove the tags from. Not null/empty/blank.
     * @param resourceClass The class of the resource
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> void removeAllTagsForResource(
        @NotBlank String id,
        Class<R> resourceClass
    ) throws NotFoundException;

    /**
     * Remove a tag from the given resource.
     *
     * @param <R>           The resource type that the tag should be associated with
     * @param id            The id of the resource to remove the tag from. Not null/empty/blank.
     * @param tag           The tag to remove. Not null/empty/blank.
     * @param resourceClass The class of the resource
     * @throws NotFoundException If no resource of type {@link R} with {@literal id} exists
     */
    <R extends CommonResource> void removeTagForResource(
        @NotBlank String id,
        @NotBlank String tag,
        Class<R> resourceClass
    ) throws NotFoundException;
    //endregion

    //region Tag APIs

    /**
     * Delete all tags from the database that aren't referenced which were created before the supplied created
     * threshold.
     *
     * @param createdThreshold The instant in time where tags created before this time that aren't referenced
     *                         will be deleted. Inclusive
     * @param batchSize        The maximum number of tags to delete in a single transaction
     * @return The number of tags deleted
     */
    long deleteUnusedTags(@NotNull Instant createdThreshold, @Min(1) int batchSize);
    //endregion

    //region File APIs

    /**
     * Delete all files from the database that aren't referenced which were created before the supplied created
     * threshold.
     *
     * @param createdThresholdLowerBound The instant in time when files created after this time that aren't referenced
     *                                   will be selected. Inclusive.
     * @param createdThresholdUpperBound The instant in time when files created before this time that aren't referenced
     *                                   will be selected. Inclusive.
     * @param batchSize        The maximum number of files to delete in a single transaction
     * @return The number of files deleted
     */
    long deleteUnusedFiles(@NotNull Instant createdThresholdLowerBound,
                           @NotNull Instant createdThresholdUpperBound,
                           @Min(1) int batchSize);
    //endregion
}
