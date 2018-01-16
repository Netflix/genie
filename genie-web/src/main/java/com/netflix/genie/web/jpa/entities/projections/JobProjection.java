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

import com.netflix.genie.web.jpa.entities.ApplicationEntity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Projection for the fields originally available in pre-3.3.0 JobEntity classes.
 *
 * @author tgianos
 * @since 3.3.0
 */
public interface JobProjection extends JobCommonFieldsProjection, JobStatusProjection {

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
     * Get the location where the job was archived.
     *
     * @return The archive location
     */
    Optional<String> getArchiveLocation();

    /**
     * Get the name of the cluster that is running or did run this job.
     *
     * @return The cluster name or empty Optional if it hasn't been set
     */
    Optional<String> getClusterName();

    /**
     * Get the name of the command that is executing this job.
     *
     * @return The command name or empty Optional if one wasn't set yet
     */
    Optional<String> getCommandName();

    /**
     * Get the applications used to run this job.
     *
     * @return The applications
     */
    List<ApplicationEntity> getApplications();
}
