/*
 *
 *  Copyright 2019 Netflix, Inc.
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

import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
import com.netflix.genie.web.dtos.JobSubmission;
import com.netflix.genie.web.exceptions.checked.AgentLaunchException;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nonnull;
import javax.validation.Valid;

/**
 * Top level coordination service responsible for taking a job request and running the job if possible.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface JobLaunchService {

    /**
     * Launches a job on behalf of the user.
     * <p>
     * Given the information submitted to Genie this service will attempt to run the job which will include:
     * - Saving the job submission information including attachments
     * - Resolving the resources needed to run the job and persisting them
     * - Launching the agent
     *
     * @param jobSubmission The payload of metadata and resources making up all the information needed to launch
     *                      a job
     * @return The id of the job. Upon return the job will at least be in {@link JobStatus#ACCEPTED} state
     * @throws AgentLaunchException        If the system was unable to launch an agent to handle job execution
     * @throws GenieJobResolutionException If the job, based on user input and current system state, can't be
     *                                     successfully resolved for whatever reason
     * @throws IdAlreadyExistsException    If the unique identifier for the job conflicts with an already existing job
     * @throws SaveAttachmentException     When a job is submitted with attachments but there is an error saving them
     */
    @Nonnull
    String launchJob(@Valid JobSubmission jobSubmission) throws
        AgentLaunchException,
        GenieJobResolutionException,
        IdAlreadyExistsException,
        SaveAttachmentException;
}
