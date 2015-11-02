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


import com.netflix.genie.common.dto.JobExecutionEnvironment;
import com.netflix.genie.common.exceptions.GenieException;
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
     * @param jobExecutionEnvironment The job to submit
     * @return The id of the job that was submitted
     * @throws GenieException if there is an error
     */
    String submitJob(
            @NotNull(message = "No job request entered to run")
            @Valid
            final JobExecutionEnvironment jobExecutionEnvironment
    ) throws GenieException;
}
