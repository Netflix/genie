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

import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotBlank;

/**
 * Interface for services to kill jobs.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Validated
public interface JobKillService {

    /**
     * Kill the job with the given id if possible.
     *
     * @param id      id of job to kill
     * @param reason  brief reason for requesting the job be killed
     * @param request The optional {@link HttpServletRequest} information if the request needs to be forwarded
     * @throws GenieJobNotFoundException When a job identified by {@literal jobId} can't be found in the system
     * @throws GenieServerException      if there is an unrecoverable error in the internal state of the Genie cluster
     */
    void killJob(
        @NotBlank(message = "No id entered. Unable to kill job.") String id,
        @NotBlank(message = "No reason provided.") String reason,
        @Nullable HttpServletRequest request
    ) throws GenieJobNotFoundException, GenieServerException;
}
