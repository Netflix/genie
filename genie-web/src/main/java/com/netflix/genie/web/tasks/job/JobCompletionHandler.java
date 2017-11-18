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
package com.netflix.genie.web.tasks.job;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.events.JobFinishedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Handles the job finished event.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
@Component
public class JobCompletionHandler {
    private final JobCompletionService jobCompletionService;

    /**
     * Constructor.
     *
     * @param jobCompletionService An implementation of the job completion service.
     */
    @Autowired
    public JobCompletionHandler(final JobCompletionService jobCompletionService) {
        this.jobCompletionService = jobCompletionService;
    }

    /**
     * Event listener for when a job is completed. Updates the status of the job.
     *
     * @param event The Spring Boot application ready event to startup on
     * @throws GenieException If there is any problem
     */
    @EventListener
    public void handleJobCompletion(final JobFinishedEvent event) throws GenieException {
        this.jobCompletionService.handleJobCompletion(event);
    }
}
