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

import java.util.Date;
import java.util.Optional;

/**
 * Projection to return only the fields desired for a job with search results.
 *
 * @author tgianos
 * @since 3.3.0
 */
public interface JobSearchProjection extends JobStatusProjection {

    /**
     * Get the unique identifier of the job.
     *
     * @return The unique identifier
     */
    String getUniqueId();

    /**
     * Get the name of the job.
     *
     * @return The name of the job
     */
    String getName();

    /**
     * Get the user who ran or is running the job.
     *
     * @return the user
     */
    String getUser();

    /**
     * Get the time the job started if it has started.
     *
     * @return The time the job started
     */
    Optional<Date> getStarted();

    /**
     * Get the time the job finished if it has finished.
     *
     * @return The time the job finished
     */
    Optional<Date> getFinished();

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
}
