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
package com.netflix.genie.agent.execution.services;

import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException;
import com.netflix.genie.common.internal.dto.v4.AgentJobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

/**
 * Agent side job specification service for resolving and retrieving job specifications from the server.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface AgentJobService {

    /**
     * Given the parameters supplied by the job request attempt to resolve a job specification on the server.
     *
     * @param id The id of the job to resolve a job specification for
     * @return The job specification
     * @throws JobSpecificationResolutionException if the specification cannot be resolved
     */
    JobSpecification resolveJobSpecification(
        @NotBlank final String id
    ) throws JobSpecificationResolutionException;

    /**
     * Given a job id retrieve the job specification from the server.
     *
     * @param id The id of the job to get the specification for
     * @return The job specification
     * @throws JobSpecificationResolutionException if the specification cannot be retrieved
     */
    JobSpecification getJobSpecification(
        @NotBlank final String id
    ) throws JobSpecificationResolutionException;

    /**
     * Invoke the job specification resolution logic without persisting anything on the server.
     *
     * @param jobRequest The various parameters required to perform the dry run should be contained in this request
     * @return The job specification
     * @throws JobSpecificationResolutionException When an error occurred during attempted resolution
     */
    JobSpecification resolveJobSpecificationDryRun(
        @Valid final AgentJobRequest jobRequest
    ) throws JobSpecificationResolutionException;
}
