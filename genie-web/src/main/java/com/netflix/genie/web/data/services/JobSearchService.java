/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for searching jobs.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Validated
public interface JobSearchService {

    /**
     * Search for jobs which match the given filter criteria.
     *
     * @param id               id for job
     * @param name             name of job (can be a SQL-style pattern such as HIVE%)
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
        @Nullable Set<JobStatus> statuses,
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

    /**
     * Given a hostname return a set of all the jobs currently active on that host.
     *
     * @param hostname The host name to search for. Not null or empty.
     * @return All the jobs active on the host as a set of Job objects
     */
    Set<Job> getAllActiveJobsOnHost(@NotBlank String hostname);

    /**
     * Get a set of host names which are currently have active jobs in the Genie cluster.
     *
     * @return The set of hosts with jobs currently in an active state
     */
    Set<String> getAllHostsWithActiveJobs();

    /**
     * Get job information for given job id.
     *
     * @param id id of job to look up
     * @return the job
     * @throws GenieException if there is an error
     */
    Job getJob(@NotBlank(message = "No id entered. Unable to get job.") String id) throws GenieException;

    /**
     * Get the status of the job with the given id.
     *
     * @param id The id of the job to get status for
     * @return The job status
     * @throws GenieException When any error, including not found, is encountered
     * @deprecated Use {@link JobPersistenceService#getJobStatus(String)} instead
     */
    @Deprecated
    JobStatus getJobStatus(@NotBlank String id) throws GenieException;

    /**
     * Get job request for given job id.
     *
     * @param id id of job request to look up
     * @return the job
     * @throws GenieException if there is an error
     */
    JobRequest getJobRequest(@NotBlank String id) throws GenieException;

    /**
     * Get job execution for given job id.
     *
     * @param id id of job execution to look up
     * @return the job
     * @throws GenieException if there is an error
     */
    JobExecution getJobExecution(@NotBlank String id) throws GenieException;

    /**
     * Get the cluster the job was run on or exception if not found.
     *
     * @param id The id of the job to get the cluster for
     * @return The cluster
     * @throws GenieException If either the job or the cluster is not found
     */
    Cluster getJobCluster(@NotBlank String id) throws GenieException;

    /**
     * Get the command the job was run with or exception if not found.
     *
     * @param id The id of the job to get the command for
     * @return The command
     * @throws GenieException If either the job or the command is not found
     */
    Command getJobCommand(@NotBlank String id) throws GenieException;

    /**
     * Get the applications the job was run with or exception if not found.
     *
     * @param id The id of the job to get the applications for
     * @return The applications
     * @throws GenieException If either the job or the applications were not found
     */
    List<Application> getJobApplications(@NotBlank String id) throws GenieException;

    /**
     * Get the hostname a job is running on.
     *
     * @param jobId The id of the job to get the hostname for
     * @return The hostname
     * @throws GenieNotFoundException If the job host cannot be found
     */
    String getJobHost(@NotBlank String jobId) throws GenieNotFoundException;

    /**
     * Get the count of 'active' jobs for a given user across all instances.
     *
     * @param user The user name
     * @return the number of active jobs for a given user
     * @throws GenieException If any error occurs
     */
    long getActiveJobCountForUser(@NotBlank String user) throws GenieException;

    /**
     * Get the metadata about a job.
     *
     * @param id The id of the job to get metadata for
     * @return The metadata for a job
     * @throws GenieException If any error occurs
     */
    JobMetadata getJobMetadata(@NotBlank String id) throws GenieException;

    /**
     * Get a map of summaries of resources usage for each user with at least one running job.
     *
     * @return a map of user resources summaries, keyed on user name
     */
    Map<String, UserResourcesSummary> getUserResourcesSummaries();

    /**
     * Get the IDs of all agent jobs that are active but currently not connected to any node.
     *
     * @return a set of job ids
     */
    Set<String> getActiveDisconnectedAgentJobs();

    /**
     * Get the amount of memory currently allocated on the given host by Genie jobs that are currently active.
     *
     * @param hostname The hostname to get the memory for
     * @return The amount of memory reserved in MB by all active jobs on the given host
     * @see JobStatus#getActiveStatuses()
     */
    long getAllocatedMemoryOnHost(String hostname);

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
    long getUsedMemoryOnHost(String hostname);

    /**
     * Get the number of active Genie jobs on a given host.
     *
     * @param hostname The host to get the count for
     * @return The count of jobs in an active state on the given host
     * @see JobStatus#getActiveStatuses()
     */
    long getActiveJobCountOnHost(String hostname);
}
