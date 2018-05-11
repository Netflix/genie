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
package com.netflix.genie.web.jpa.entities.projections.v4;

import com.netflix.genie.web.jpa.entities.CriterionEntity;
import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.jpa.entities.TagEntity;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Projection of just the fields needed for a V4 {@link com.netflix.genie.common.internal.dto.v4.JobRequest}.
 *
 * @author tgianos
 * @since 4.0.0
 */
// TODO: Clean this up as things get more finalized to break out fields into reusable super interfaces
public interface V4JobRequestProjection {
    /**
     * Get the unique identifier for this entity.
     *
     * @return The globally unique identifier of this entity
     */
    String getUniqueId();

    /**
     * Get whether the unique identifier of this request was requested by the user or not.
     *
     * @return True if it was requested by the user
     */
    // TODO: As we move more to V4 as default move this to more shared location
    boolean isRequestedId();

    /**
     * Get the name of the resource.
     *
     * @return The name of the resource
     */
    String getName();

    /**
     * Get the user who created the resource.
     *
     * @return The user who created the resource
     */
    String getUser();

    /**
     * Get the version.
     *
     * @return The version of the resource (job, app, etc)
     */
    String getVersion();

    /**
     * Get the description of this resource.
     *
     * @return The description which could be null so it's wrapped in Optional
     */
    Optional<String> getDescription();

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
     * Get all the environment variables that were requested be added to the Job Runtime environment by the user.
     *
     * @return The requested environment variables
     */
    Map<String, String> getRequestedEnvironmentVariables();

    /**
     * Get the requested directory on disk where the Genie job working directory should be placed if a user asked to
     * override the default.
     *
     * @return The requested job directory location wrapped in an {@link Optional}
     */
    Optional<String> getRequestedJobDirectoryLocation();

    /**
     * Get the extension configuration of a job agent environment.
     *
     * @return The extension configuration (as valid JSON string) if it exists wrapped in an {@link Optional}
     */
    Optional<String> getRequestedAgentEnvironmentExt();

    /**
     * Get the extension configuration of a job agent configuration.
     *
     * @return The extension configuration (as valid JSON string) if it exists wrapped in an {@link Optional}
     */
    Optional<String> getRequestedAgentConfigExt();

    /**
     * Get whether the job was an interactive job or not when launched.
     *
     * @return true if the job was interactive
     */
    boolean isInteractive();

    /**
     * Get the setup file for this resource.
     *
     * @return The setup file
     */
    Optional<FileEntity> getSetupFile();

    /**
     * Get the user group for this job.
     *
     * @return the user group
     */
    Optional<String> getGenieUserGroup();

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
     * Get all the dependency files for this job.
     *
     * @return The set of dependencies
     */
    Set<FileEntity> getDependencies();

    /**
     * Get all the configuration files for this job.
     *
     * @return The set of configs
     */
    Set<FileEntity> getConfigs();

    /**
     * Get whether log archiving was requested to be disabled for this job or not.
     *
     * @return true if log archival was disabled
     */
    boolean isArchivingDisabled();

    /**
     * Get the email of the user associated with this job if they desire an email notification at completion
     * of the job.
     *
     * @return The email
     */
    Optional<String> getEmail();

    /**
     * Get the number of CPU's requested to run this job.
     *
     * @return The number of CPU's as an Optional
     */
    Optional<Integer> getRequestedCpu();

    /**
     * Get the memory requested to run this job with.
     *
     * @return The amount of memory the user requested for this job in MB as an Optional
     */
    Optional<Integer> getRequestedMemory();

    /**
     * Get the timeout (in seconds) requested by the user for this job.
     *
     * @return The number of seconds before a timeout as an Optional
     */
    Optional<Integer> getRequestedTimeout();

    /**
     * Get any applications requested by their id.
     *
     * @return The applications
     */
    List<String> getRequestedApplications();
}
