/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.jpa.entities.projections;

import com.netflix.genie.web.jpa.entities.CriterionEntity;
import com.netflix.genie.web.jpa.entities.FileEntity;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Projection to reproduce the JobRequest entity fields which were was a table present before 3.3.0.
 *
 * @author tgianos
 * @since 3.3.0
 */
public interface JobRequestProjection extends JobCommonFieldsProjection, SetupFileProjection {

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
