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

import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Interface to handoff job execution based on different environments.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface  JobSubmitterService {

    /**
     * Submit the job for appropriate execution based on environment.
     *
     * @param jobRequest of job to run
     * @throws GenieException if there is an error
     */
    void submitJob(
            @NotNull(message = "No job provided. Unable to submit job for execution.")
            @Valid
            final JobRequest jobRequest
    ) throws GenieException;

}
