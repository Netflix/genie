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
 * Service API for taking inputs from a user and resolving them to concrete information that the Genie system will use
 * to execute the users job.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ParametersAreNonnullByDefault
@Validated
public interface JobResolverService {

    /**
     * Given a job request resolve all the details needed to run a job.
     *
     * @param id         The id of the job
     * @param jobRequest The job request containing all details a user wants to have for their job
     * @return The complete job specification
     */
    JobSpecification resolveJob(String id, @Valid JobRequest jobRequest);
}
