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

import com.netflix.genie.common.dto.v4.JobRequest;
import com.netflix.genie.common.dto.v4.JobSpecification;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;

/**
 * Agent side job specification service for resolving and retrieving job specifications from the server.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface AgentJobSpecificationService {

    /**
     * Given the parameters supplied by the job request attempt to resolve a job specification on the server.
     *
     * @param jobRequest The job request
     * @return The job specification
     */
    JobSpecification resolveJobSpecification(@Valid final JobRequest jobRequest);

    /**
     * Given a job id retrieve the job specification from the server.
     *
     * @param id The id of the job to get the specification for
     * @return The job specification
     */
    JobSpecification getJobSpecification(final String id);
}
