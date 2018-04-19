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
package com.netflix.genie.web.jobs;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.services.JobSubmitterService;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Class to wrap launching a job in an asynchronous thread from the HTTP request thread to free up the system to
 * respond to the user.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class JobLauncher implements Runnable {

    static final String JOB_SUBMIT_TIMER_NAME = "genie.jobs.submit.timer";
    private final JobSubmitterService jobSubmitterService;
    private final JobRequest jobRequest;
    private final Cluster cluster;
    private final Command command;
    private final List<Application> applications;
    private final int memory;
    private final MeterRegistry registry;

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
        @NotNull final MeterRegistry registry
    ) {
        this.jobSubmitterService = jobSubmitterService;
        this.jobRequest = jobRequest;
        this.cluster = cluster;
        this.command = command;
        this.applications = applications;
        this.memory = memory;
        this.registry = registry;
    }

    /**
     * Starts the job setup and launch process once the thread is activated.
     */
    @Override
    public void run() {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            this.jobSubmitterService.submitJob(
                this.jobRequest,
                this.cluster,
                this.command,
                this.applications,
                this.memory
            );
            MetricsUtils.addSuccessTags(tags);
        } catch (final GenieException e) {
            log.error("Unable to submit job due to exception: {}", e.getMessage(), e);
            MetricsUtils.addFailureTagsWithException(tags, e);
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry.timer(JOB_SUBMIT_TIMER_NAME, tags).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }
}
