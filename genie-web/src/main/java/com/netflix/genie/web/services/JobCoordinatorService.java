/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * Job Coordination APIs.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Validated
public interface JobCoordinatorService {

    /**
     * Takes in a Job Request object and does necessary preparation for execution.
     *
     * @param jobRequest  of job to kill
     * @param jobMetadata Metadata about the http request which generated started this job process
     * @return the id of the job run
     * @throws GenieException if there is an error
     */
    String coordinateJob(
        @NotNull(message = "No job request provided. Unable to execute.")
        @Valid final JobRequest jobRequest,
        @NotNull(message = "No job metadata provided. Unable to execute.")
        @Valid final JobMetadata jobMetadata
    ) throws GenieException;

    /**
     * Kill the job identified by the given id.
     *
     * @param jobId  id of the job to kill
     * @param reason brief reason for requesting the job be killed
     * @throws GenieException if there is an error
     */
    void killJob(@NotBlank final String jobId, @NotBlank final String reason) throws GenieException;
}
