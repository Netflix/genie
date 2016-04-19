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

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.core.events.JobFinishedEvent;
import com.netflix.genie.core.events.JobStartedEvent;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.web.properties.JobOutputMaxProperties;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
    private final String hostName;
    private final JobSearchService jobSearchService;
    private final TaskScheduler scheduler;
    private final ApplicationEventPublisher publisher;
    private final Executor executor;
    private final Registry registry;
    private final File jobsDir;
    private final JobOutputMaxProperties outputMaxProperties;

    private final Counter unableToCancel;

    /**
     * Constructor.
     *
     * @param hostName            The name of the host this Genie process is running on
     * @param jobSearchService    The search service to use to find jobs
     * @param publisher           The event publisher to use to publish events
     * @param scheduler           The task scheduler to use to register scheduling of job checkers
     * @param executor            The executor to use to launch processes
     * @param registry            The metrics registry
     * @param jobsDir             The directory where job output is stored
     * @param outputMaxProperties The properties for the maximum length of job output files
     * @throws IOException on error with the filesystem
     */
    @Autowired
    public JobMonitoringCoordinator(
        final String hostName,
        final JobSearchService jobSearchService,
        final ApplicationEventPublisher publisher,
        final TaskScheduler scheduler,
        final Executor executor,
        final Registry registry,
        final Resource jobsDir,
        final JobOutputMaxProperties outputMaxProperties
    ) throws IOException {
        this.jobMonitors = new HashMap<>();
        this.hostName = hostName;
        this.jobSearchService = jobSearchService;
        this.publisher = publisher;
        this.scheduler = scheduler;
        this.executor = executor;
        this.registry = registry;
        this.jobsDir = jobsDir.getFile();
        this.outputMaxProperties = outputMaxProperties;

        // Automatically track the number of jobs running on this node
        this.registry.mapSize("genie.jobs.running.gauge", this.jobMonitors);
        this.unableToCancel = registry.counter("genie.jobs.unableToCancel.rate");
    }

    /**
     * When this application is fully up and running this method should be triggered by an event. It will query the
     * database to find any jobs already running on this node that aren't in the map. The use case for this is if
     * the Genie application crashes when it comes back up it can find the jobs again and not leave them orphaned.
     *
     * @param event The spring boot application ready event indicating the application is ready to start taking load
     */
    @EventListener
    public void attachToRunningJobs(final ApplicationReadyEvent event) {
        log.info("Application is ready according to event {}. Attempting to re-attach to any running jobs", event);
        final Set<JobExecution> executions = this.jobSearchService.getAllRunningJobExecutionsOnHost(this.hostName);
        if (executions.isEmpty()) {
            log.info("No jobs currently running on this node.");
            return;
        } else {
            log.info("{} jobs currently running on this node at startup", executions.size());
        }

        for (final JobExecution execution : executions) {
            if (this.jobMonitors.containsKey(execution.getId())) {
                log.info("Job {} is already being tracked. Ignoring.");
            } else {
                this.scheduleMonitor(execution);
                log.info("Re-attached a job monitor to job {}", execution.getId());
            }
        }
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
            this.scheduleMonitor(event.getJobExecution());
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
                this.unableToCancel.increment();
            }
        }
    }

    private void scheduleMonitor(final JobExecution jobExecution) {
        final File stdOut = new File(this.jobsDir, jobExecution.getId() + "/" + JobConstants.STDOUT_LOG_FILE_NAME);
        final File stdErr = new File(this.jobsDir, jobExecution.getId() + "/" + JobConstants.STDERR_LOG_FILE_NAME);

        final JobMonitor monitor = new JobMonitor(
            jobExecution,
            stdOut,
            stdErr,
            this.executor,
            this.publisher,
            this.registry,
            this.outputMaxProperties
        );
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
        this.jobMonitors.put(jobExecution.getId(), future);
        log.info("Scheduled job monitoring for Job {}", jobExecution.getId());
    }
}
