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
package com.netflix.genie.web.services;

import com.netflix.genie.common.internal.dto.v4.JobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import org.springframework.validation.annotation.Validated;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;

/**
 * Service API definition and helper methods for working with Genie Job Specifications to be used by the Agent.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ParametersAreNonnullByDefault
@Validated
public interface JobSpecificationService {

    /**
     * Given a job request resolve all the details needed for a complete job specification can be run by the user.
     *
     * @param id         The id of the job
     * @param jobRequest The job request containing all details a user wants to have for their job
     * @return The complete job specification
     */
    JobSpecification resolveJobSpecification(final String id, @Valid final JobRequest jobRequest);
}
