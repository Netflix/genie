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
package com.netflix.genie.web.services.impl;

import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.events.JobScheduledEvent;
import com.netflix.genie.web.jobs.JobLauncher;
import com.netflix.genie.web.services.JobStateService;
import com.netflix.genie.web.services.JobSubmitterService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.Instant;
import org.springframework.scheduling.TaskScheduler;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * A Service to monitor the state of locally run jobs.
 *
 * @author amajumdar
 * @since 3.0.0
 */
@Slf4j
public class JobStateServiceImpl implements JobStateService {
    protected final TaskScheduler scheduler;
    protected final MeterRegistry registry;
    protected final GenieEventBus genieEventBus;
    private final Map<String, JobInfo> jobs = Collections.synchronizedMap(new HashMap<>());
    private final JobSubmitterService jobSubmitterService;
    private final Counter unableToCancel;

    /**
     * Constructor.
     *
     * @param jobSubmitterService implementation of the job submitter service
     * @param scheduler           The task scheduler to use to register scheduling of job checkers
     * @param genieEventBus       The event bus to use to publish events
     * @param registry            The metrics registry
     */
    public JobStateServiceImpl(
        final JobSubmitterService jobSubmitterService,
        final TaskScheduler scheduler,
        final GenieEventBus genieEventBus,
        final MeterRegistry registry
    ) {
        this.jobSubmitterService = jobSubmitterService;
        this.scheduler = scheduler;
        this.registry = registry;
        this.genieEventBus = genieEventBus;

        // TODO: Active and running seems backwards here. Might want to review
        this.registry.gauge("genie.jobs.running.gauge", this.jobs, Map::size);
        this.registry.gauge("genie.jobs.active.gauge", this, JobStateServiceImpl::getNumActiveJobs);
        this.registry.gauge("genie.jobs.memory.used.gauge", this, JobStateServiceImpl::getUsedMemory);

        this.unableToCancel = registry.counter("genie.jobs.unableToCancel.rate");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final String jobId) {
        this.jobs.putIfAbsent(jobId, new JobInfo());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void schedule(
        final String jobId,
        final JobRequest jobRequest,
        final Cluster cluster,
        final Command command,
        final List<Application> applications,
        final int memory
    ) {
        this.handle(
            jobId,
            () -> {
                final JobInfo jobInfo = jobs.get(jobId);
                jobInfo.setMemory(memory);
                final JobLauncher jobLauncher = new JobLauncher(
                    this.jobSubmitterService,
                    jobRequest,
                    cluster,
                    command,
                    applications,
                    memory,
                    registry
                );
                final Future<?> task = this.scheduler.schedule(jobLauncher, Instant.now().toDate());
                jobInfo.setRunningTask(task);
                jobInfo.setActive(true);
                //
                // This event is fired when a job is scheduled to run on this Genie node. We'll track the future here in
                // case it needs to be killed while still in INIT state. Once it's running the onJobStarted event will
                // clear it out.
                //
                this.genieEventBus.publishSynchronousEvent(new JobScheduledEvent(jobId, task, memory, this));
                return null;
            }
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void done(final String jobId) throws GenieException {
        this.handle(
            jobId,
            () -> {
                final JobInfo jobInfo = jobs.get(jobId);
                final Future<?> task = jobInfo.getRunningTask();
                if (task != null && !task.isDone()) {
                    if (task.cancel(true)) {
                        log.debug("Successfully cancelled job task for job {}", jobId);
                    } else {
                        log.error("Unable to cancel job task for job {}", jobId);
                        this.unableToCancel.increment();
                    }
                }
                jobs.remove(jobId);
                return null;
            }
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean jobExists(final String jobId) {
        return jobs.containsKey(jobId);
    }

    private void handle(final String jobId, final Supplier<Void> supplier) {
        JobInfo jobInfo = jobs.get(jobId);
        if (jobInfo != null) {
            synchronized (jobInfo) {
                jobInfo = jobs.get(jobId);
                if (jobInfo != null) {
                    supplier.get();
                }
            }
        }
    }

    protected void setMemoryAndTask(final String jobId, final int memory, final Future<?> task) {
        handle(jobId, () -> {
            final JobInfo jobInfo = jobs.get(jobId);
            jobInfo.setMemory(memory);
            jobInfo.setRunningTask(task);
            jobInfo.setActive(true);
            return null;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumActiveJobs() {
        // Synchronized to avoid concurrent modification exception
        synchronized (this.jobs) {
            return (int) this.jobs.values().stream().filter(JobInfo::isActive).count();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUsedMemory() {
        // Synchronized to avoid concurrent modification exception
        synchronized (this.jobs) {
            return this.jobs.values().stream().map(JobInfo::getMemory)
                .reduce((a, b) -> a + b).orElse(0);
        }
    }

    @Getter
    @Setter
    private static class JobInfo {
        private Future<?> runningTask;
        private Integer memory = 0;
        private boolean active;
    }
}
