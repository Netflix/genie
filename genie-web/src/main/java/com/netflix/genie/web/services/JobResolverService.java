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

import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
import com.netflix.genie.web.dtos.ResolvedJob;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nonnull;
import javax.validation.Valid;

/**
 * Service API for taking inputs from a user and resolving them to concrete information that the Genie system will use
 * to execute the users job.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface JobResolverService {

    /**
     * Given the id of a job that was successfully submitted to the system this API will attempt to resolve all the
     * concrete details (cluster, command, resources, etc) needed for the system to actually launch the job. Once these
     * details are determined they are persisted and the job is marked as {@link JobStatus#RESOLVED}.
     *
     * @param id The id of the job to resolve. The job must exist and its status must return {@literal true} from
     *           {@link JobStatus#isResolvable()}
     * @return A {@link ResolvedJob} instance containing all the concrete information needed to execute the job
     * @throws GenieJobResolutionException When there is an issue resolving the job based on the information provided
     *                                     by the user in conjunction with the configuration available in the system
     */
    @Nonnull
    ResolvedJob resolveJob(String id) throws GenieJobResolutionException;

    /**
     * Given a job request resolve all the details needed to run a job. This API is stateless and saves nothing.
     *
     * @param id         The id of the job
     * @param jobRequest The job request containing all details a user wants to have for their job
     * @param apiJob     {@literal true} if this job was submitted via the REST API. {@literal false} otherwise.
     * @return The completely resolved job information within a {@link ResolvedJob} instance
     * @throws GenieJobResolutionException When there is an issue resolving the job based on the information provided
     *                                     by the user in conjunction with the configuration available in the system
     */
    @Nonnull
    ResolvedJob resolveJob(String id, @Valid JobRequest jobRequest, boolean apiJob) throws GenieJobResolutionException;
}
