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

import com.netflix.genie.core.events.JobFinishedEvent;
import com.netflix.genie.core.events.JobStartedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

/**
 * A Task to monitor running jobs on a Genie node.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Component
@Slf4j
public class JobMonitoringCoordinator {

    private final Map<String, ScheduledFuture<?>> jobMonitors;
    private final TaskScheduler scheduler;
    private final ApplicationEventPublisher publisher;
    private final Executor executor;

    /**
     * Constructor.
     *
     * @param publisher The event publisher to use to publish events
     * @param scheduler The task scheduler to use to register scheduling of job checkers
     * @param executor  The executor to use to launch processes
     */
    @Autowired
    public JobMonitoringCoordinator(
        final ApplicationEventPublisher publisher,
        final TaskScheduler scheduler,
        final Executor executor
    ) {
        this.jobMonitors = new HashMap<>();
        this.publisher = publisher;
        this.scheduler = scheduler;
        this.executor = executor;
    }

    /**
     * This event is fired when a job is started on this Genie node. Will create a JobMonitor and schedule it
     * for monitoring.
     *
     * @param event The event of the started job
     */
    @EventListener
    public void onJobStarted(final JobStartedEvent event) {
        if (!this.jobMonitors.containsKey(event.getJobExecution().getId())) {
            final JobMonitor monitor
                = new JobMonitor(event.getJobExecution(), this.executor, this.publisher);
            final ScheduledFuture<?> future;
            switch (monitor.getScheduleType()) {
                case TRIGGER:
                    future = this.scheduler.schedule(monitor, monitor.getTrigger());
                    break;
                case FIXED_DELAY:
                    future = this.scheduler.scheduleWithFixedDelay(monitor, monitor.getFixedDelay());
                    break;
                case FIXED_RATE:
                    future = this.scheduler.scheduleAtFixedRate(monitor, monitor.getFixedRate());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown schedule type: " + monitor.getScheduleType());
            }
            this.jobMonitors.put(event.getJobExecution().getId(), future);
        }
    }

    /**
     * When a job is finished this event is fired. This method will cancel the task monitoring the job process.
     *
     * @param event the event of the finished job
     */
    @EventListener
    public void onJobFinished(final JobFinishedEvent event) {
        final String jobId = event.getJobExecution().getId();
        if (this.jobMonitors.containsKey(jobId)) {
            final ScheduledFuture<?> future = this.jobMonitors.get(jobId);
            //TODO: should we add back off it is unable to cancel?
            if (future.cancel(true)) {
                log.debug("Successfully cancelled task monitoring job {}", jobId);
                this.jobMonitors.remove(jobId);
            } else {
                log.error("Unable to cancel task monitoring job {}", jobId);
                //TODO: Add metric here
            }
        }
    }
}
