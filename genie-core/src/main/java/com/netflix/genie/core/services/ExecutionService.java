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
package com.netflix.genie.core.services;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Interface for the execution service used to run, kill and monitor jobs.
 *
 * @author amsharma
 * @author tgianos
 * @since 2.0.0
 */
@Validated
public interface ExecutionService {

    /**
     * Submit a new job.
     *
     * @param jobRequest The job to submit
     * @return The id of the job that was submitted
     * @throws GenieException if there is an error
     */
    String submitJob(
            @NotNull(message = "No job request entered to run")
            @Valid
            final JobRequest jobRequest
    ) throws GenieException;

    /**
     * Kill job based on given job iD.
     *
     * @param id id for job to kill
     * @return The killed job
     * @throws GenieException if there is an error
     */
    Job killJob(
            @NotBlank(message = "No id entered unable to kill job.")
            final String id
    ) throws GenieException;

    /**
     * Mark jobs as zombies if status hasn't been updated for
     * com.netflix.genie.server.janitor.zombie.delta.ms.
     *
     * @return Number of jobs marked as zombies
     */
    int markZombies();

    /**
     * Finalize the status of a job.
     *
     * @param id       The id of the job to finalize.
     * @param exitCode The exit code of the job process.
     * @return The job status.
     * @throws GenieException if there is an error
     */
    JobStatus finalizeJob(
            @NotBlank(message = "No job id entered. Unable to finalize.")
            final String id,
            final int exitCode
    ) throws GenieException;
}
