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

package com.netflix.genie.web.services;

import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.exceptions.GenieException;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Interface to hand off job execution based on different environments.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Validated
public interface JobSubmitterService {

    /**
     * Submit the job for appropriate execution based on environment.
     *
     * @param jobRequest   of job to run
     * @param cluster      The cluster this job should run on
     * @param command      the command to run this job with
     * @param applications Any applications that are needed to run the command
     * @param memory       The amount of memory (in MB) to use to run the job
     * @throws GenieException if there is an error
     */
    void submitJob(
        @NotNull(message = "No job provided. Unable to submit job for execution.")
        @Valid final JobRequest jobRequest,
        @NotNull(message = "No cluster provided. Unable to submit job for execution")
        @Valid final Cluster cluster,
        @NotNull(message = "No command provided. Unable to submit job for execution")
        @Valid final Command command,
        @NotNull(message = "No applications provided. Unable to execute") final List<Application> applications,
        @Min(value = 1, message = "Memory can't be less than 1 MB") final int memory
    ) throws GenieException;
}
