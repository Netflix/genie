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
package com.netflix.genie.core.jobs;

import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.exceptions.GenieTimeoutException;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.core.util.MetricsConstants;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;

/**
 * Class to wrap launching a job in an asynchronous thread from the HTTP request thread to free up the system to
 * respond to the user.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class JobLauncher implements Runnable {

    private final JobSubmitterService jobSubmitterService;
    private final JobRequest jobRequest;
    private final Registry registry;

    /**
     * Constructor.
     *
     * @param jobSubmitterService The job submission service to use
     * @param jobRequest          The job request to be submitted
     * @param registry            The registry to use for metrics
     */
    public JobLauncher(
        @NotNull final JobSubmitterService jobSubmitterService,
        @NotNull final JobRequest jobRequest,
        @NotNull final Registry registry
    ) {
        this.jobSubmitterService = jobSubmitterService;
        this.jobRequest = jobRequest;
        this.registry = registry;
    }

    /**
     * Starts the job setup and launch process once the thread is activated.
     */
    @Override
    public void run() {
        try {
            this.jobSubmitterService.submitJob(this.jobRequest);
        } catch (final GenieException e) {
            log.error("Unable to submit job due to exception: {}", e.getMessage(), e);
            if (e instanceof GenieBadRequestException) {
                this.registry.counter(MetricsConstants.GENIE_EXCEPTIONS_BAD_REQUEST_RATE).increment();
            } else if (e instanceof GenieConflictException) {
                this.registry.counter(MetricsConstants.GENIE_EXCEPTIONS_CONFLICT_RATE).increment();
            } else if (e instanceof GenieNotFoundException) {
                this.registry.counter(MetricsConstants.GENIE_EXCEPTIONS_NOT_FOUND_RATE).increment();
            } else if (e instanceof GeniePreconditionException) {
                this.registry.counter(MetricsConstants.GENIE_EXCEPTIONS_PRECONDITION_RATE).increment();
            } else if (e instanceof GenieServerException) {
                this.registry.counter(MetricsConstants.GENIE_EXCEPTIONS_SERVER_RATE).increment();
            } else if (e instanceof GenieServerUnavailableException) {
                this.registry.counter(MetricsConstants.GENIE_EXCEPTIONS_SERVER_UNAVAILABLE_RATE).increment();
            } else if (e instanceof GenieTimeoutException) {
                this.registry.counter(MetricsConstants.GENIE_EXCEPTIONS_TIMEOUT_RATE).increment();
            } else {
                this.registry.counter(MetricsConstants.GENIE_EXCEPTIONS_OTHER_RATE).increment();
            }
        }
    }
}
