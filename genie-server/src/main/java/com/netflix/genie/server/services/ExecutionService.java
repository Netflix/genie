/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.server.services;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;

/**
 * Interface for the Execution Service.<br>
 * Implementations must be thread-safe.
 *
 * @author skrishnan
 * @author amsharma
 * @author tgianos
 */
public interface ExecutionService {

    /**
     * Submit a new job.
     *
     * @param job the job to submit
     * @return The job that was submitted
     * @throws GenieException
     */
    Job submitJob(final Job job) throws GenieException;

    /**
     * Kill job based on given job iD.
     *
     * @param id id for job to kill
     * @return The killed job
     * @throws GenieException
     */
    Job killJob(final String id) throws GenieException;

    /**
     * Mark jobs as zombies if status hasn't been updated for
     * netflix.genie.server.janitor.zombie.delta.ms.
     *
     * @return Number of jobs marked as zombies
     */
    int markZombies();

    /**
     * Finalize the status of a job.
     *
     * @param id The id of the job to finalize.
     * @param exitCode The exit code of the job process.
     * @return The job status.
     * @throws GenieException
     */
    JobStatus finalizeJob(final String id, final int exitCode) throws GenieException;
}
