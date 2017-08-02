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

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.core.util.MetricsConstants;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

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
    private final Cluster cluster;
    private final Command command;
    private final List<Application> applications;
    private final int memory;
    private final Registry registry;
    private final Timer submitTimer;
    private final Id submitExceptionCounterId;

    /**
     * Constructor.
     *
     * @param jobSubmitterService The job submission service to use
     * @param jobRequest          The job request to be submitted
     * @param cluster             The cluster to use for the job
     * @param command             The command to use for the job
     * @param applications        The applications to use for the job (if any)
     * @param memory              The amount of memory (in MB) to use to run the job
     * @param registry            The registry to use for metrics
     */
    public JobLauncher(
        @NotNull final JobSubmitterService jobSubmitterService,
        @NotNull final JobRequest jobRequest,
        @NotNull final Cluster cluster,
        @NotNull final Command command,
        @NotNull final List<Application> applications,
        @Min(1) final int memory,
        @NotNull final Registry registry
    ) {
        this.jobSubmitterService = jobSubmitterService;
        this.jobRequest = jobRequest;
        this.cluster = cluster;
        this.command = command;
        this.applications = applications;
        this.memory = memory;
        this.registry = registry;
        this.submitTimer = this.registry.timer("genie.jobs.submit.timer");
        this.submitExceptionCounterId = registry.createId("genie.jobs.submit.exception");
    }

    /**
     * Starts the job setup and launch process once the thread is activated.
     */
    @Override
    public void run() {
        this.submitTimer.record(() -> {
                // TODO: Compress the timer into a single ID tagged by exceptions
                try {
                    this.jobSubmitterService.submitJob(
                        this.jobRequest,
                        this.cluster,
                        this.command,
                        this.applications,
                        this.memory
                    );
                } catch (final GenieException e) {
                    log.error("Unable to submit job due to exception: {}", e.getMessage(), e);
                    final Id taggedId = this.submitExceptionCounterId
                        .withTag(MetricsConstants.TagKeys.EXCEPTION_CLASS, e.getClass().getCanonicalName());
                    this.registry.counter(taggedId).increment();
                }
            }
        );
    }
}
