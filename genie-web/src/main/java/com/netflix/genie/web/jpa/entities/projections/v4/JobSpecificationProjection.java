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

import com.netflix.genie.web.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.jpa.entities.ClusterEntity;
import com.netflix.genie.web.jpa.entities.CommandEntity;
import com.netflix.genie.web.jpa.entities.FileEntity;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Projection of the database fields which make up the required elements of a job specification.
 *
 * @author tgianos
 * @since 4.0.0
 */
public interface JobSpecificationProjection {
    /**
     * Get the unique identifier for this entity.
     *
     * @return The globally unique identifier of this entity
     */
    String getUniqueId();

    /**
     * Get all the configuration files for this job.
     *
     * @return The set of configs
     */
    Set<FileEntity> getConfigs();

    /**
     * Get all the dependency files for this job.
     *
     * @return The set of dependencies
     */
    Set<FileEntity> getDependencies();

    /**
     * Get the setup file for this resource.
     *
     * @return The setup file
     */
    Optional<FileEntity> getSetupFile();

    /**
     * Get the job directory location the agent should use.
     *
     * @return The job directory location if its been set wrapped in an {@link Optional}
     */
    Optional<String> getJobDirectoryLocation();

    /**
     * Get the command arguments the user supplied for this job.
     *
     * @return The command arguments
     */
    List<String> getCommandArgs();

    // TODO: Figure out how to use nested projections for linked entities. It'll work but the fact
    //       JobEntity implements these interfaces conflicts with the non-projection

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

    /**
     * Get the applications used to run this job.
     *
     * @return The applications
     */
    List<ApplicationEntity> getApplications();

    /**
     * Get whether the job was an interactive job or not when launched.
     *
     * @return true if the job was interactive
     */
    boolean isInteractive();

    /**
     * Get the final set of environment variables sent from the server to the agent for the job.
     *
     * @return The environment variables
     */
    Map<String, String> getEnvironmentVariables();

    /**
     * Get whether the job specification has been resolved yet or not.
     *
     * @return True if the job specification has been resolved
     */
    boolean isResolved();
}
