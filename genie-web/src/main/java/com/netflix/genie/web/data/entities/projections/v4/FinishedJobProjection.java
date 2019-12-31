/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.data.entities.projections.v4;

import com.netflix.genie.web.data.entities.ApplicationEntity;
import com.netflix.genie.web.data.entities.ClusterEntity;
import com.netflix.genie.web.data.entities.CommandEntity;
import com.netflix.genie.web.data.entities.CriterionEntity;
import com.netflix.genie.web.data.entities.TagEntity;
import com.netflix.genie.web.data.entities.projections.BaseProjection;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Projection for a job entity that reached a terminal status.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface FinishedJobProjection extends BaseProjection {

    /**
     * Get the tags for the job.
     *
     * @return Any tags that were sent in when job was originally requested
     */
    Set<TagEntity> getTags();

    /**
     * Get the grouping this job is a part of. e.g. scheduler job name for job run many times
     *
     * @return The grouping
     */
    Optional<String> getGrouping();

    /**
     * Get the instance identifier of a grouping. e.g. the run id of a given scheduled job
     *
     * @return The grouping instance
     */
    Optional<String> getGroupingInstance();

    /**
     * Get the current status message of the job.
     *
     * @return The status message
     */
    Optional<String> getStatusMsg();

    /**
     * Get when the job was started.
     *
     * @return The start date
     */
    Optional<Instant> getStarted();

    /**
     * Get when the job was finished.
     *
     * @return The finish date
     */
    Optional<Instant> getFinished();

    /**
     * Get the metadata of this entity which is unstructured JSON.
     *
     * @return Optional of the metadata json node represented as a string
     */
    Optional<String> getMetadata();

    /**
     * Get the command arguments the user supplied for this job.
     *
     * @return The command arguments
     */
    List<String> getCommandArgs();

    /**
     * Get the memory requested to run this job with.
     *
     * @return The amount of memory the user requested for this job in MB as an Optional
     */
    Optional<Integer> getRequestedMemory();

    /**
     * Get the request api client hostname.
     *
     * @return {@link Optional} of the client host
     */
    Optional<String> getRequestApiClientHostname();

    /**
     * Get the user agent.
     *
     * @return {@link Optional} of the user agent
     */
    Optional<String> getRequestApiClientUserAgent();

    /**
     * Get the number of attachments.
     *
     * @return The number of attachments as an {@link Optional}
     */
    Optional<Integer> getNumAttachments();

    /**
     * Get the hostname of the agent that requested this job be run if there was one.
     *
     * @return The hostname wrapped in an {@link Optional}
     */
    Optional<String> getRequestAgentClientHostname();

    /**
     * Get the version of the agent that requested this job be run if there was one.
     *
     * @return The version wrapped in an {@link Optional}
     */
    Optional<String> getRequestAgentClientVersion();

    /**
     * Get the exit code from the process that ran the job.
     *
     * @return The exit code or -1 if the job hasn't finished yet
     */
    Optional<Integer> getExitCode();

    /**
     * Get the location where the job was archived.
     *
     * @return The archive location
     */
    Optional<String> getArchiveLocation();

    /**
     * Get the amount of memory (in MB) that this job is/was run with.
     *
     * @return The memory as an {@link Optional} as it could be null
     */
    Optional<Integer> getMemoryUsed();

    /**
     * Get all the cluster criteria.
     *
     * @return The criteria in priority order
     */
    List<CriterionEntity> getClusterCriteria();

    /**
     * Get the command criterion for this job.
     *
     * @return The command criterion for this job
     */
    CriterionEntity getCommandCriterion();

    /**
     * Get the applications used to run this job.
     *
     * @return The applications
     */
    List<ApplicationEntity> getApplications();

    /**
     * Get the cluster that ran or is currently running a given job.
     *
     * @return The cluster entity
     */
    Optional<ClusterEntity> getCluster();

    /**
     * Get the command that ran or is currently running a given job.
     *
     * @return The command entity
     */
    Optional<CommandEntity> getCommand();
}
