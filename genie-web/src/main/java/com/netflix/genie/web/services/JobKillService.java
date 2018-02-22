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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.events.KillJobEvent;
import org.springframework.context.event.EventListener;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * Interface for services to kill jobs.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Validated
public interface JobKillService {

    /**
     * Kill the job with the given id if possible. Should publish a JobFinishedEvent when done.
     *
     * @param id     id of job to kill
     * @param reason brief reason for requesting the job be killed
     * @throws GenieException if there is an error
     */
    void killJob(@NotBlank(message = "No id entered. Unable to kill job.") final String id,
                 @NotBlank(message = "No reason provided.") final String reason) throws GenieException;

    /**
     * Listen for events where the system is requesting a certain job be killed.
     *
     * @param event The event
     * @throws GenieException On any issue during killing
     */
    @EventListener
    void onKillJobEvent(@NotNull final KillJobEvent event) throws GenieException;
}
