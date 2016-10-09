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

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.events.JobFinishedEvent;
import com.netflix.genie.core.events.JobFinishedReason;
import com.netflix.genie.core.events.JobScheduledEvent;
import com.netflix.genie.core.events.JobStartedEvent;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.core.services.JobMetricsService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

/**
 * A Task to monitor running jobs on a Genie node.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Component
@Primary
@Slf4j
public class JobMonitoringCoordinator implements JobMetricsService {

    private final Map<String, ScheduledFuture<?>> jobMonitors = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Future<?>> scheduledJobs = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Integer> jobMemories = Collections.synchronizedMap(new HashMap<>());
    private final String hostName;
    private final JobSearchService jobSearchService;
    private final TaskScheduler scheduler;
    private final ApplicationEventPublisher publisher;
    private final ApplicationEventMulticaster eventMulticaster;
    private final Executor executor;
    private final Registry registry;
    private final File jobsDir;
    private final JobsProperties jobsProperties;

    private final Counter unableToCancel;
    private final Counter unableToReAttach;

    /**
     * Constructor.
     *
     * @param hostName         The name of the host this Genie process is running on
     * @param jobSearchService The search service to use to find jobs
     * @param publisher        The application event publisher to use to publish synchronous events
     * @param eventMulticaster The event eventMulticaster to use to publish asynchronous events
     * @param scheduler        The task scheduler to use to register scheduling of job checkers
     * @param executor         The executor to use to launch processes
     * @param registry         The metrics registry
     * @param jobsDir          The directory where job output is stored
     * @param jobsProperties   The properties pertaining to jobs
     * @throws IOException on error with the filesystem
     */
    @Autowired
    public JobMonitoringCoordinator(
        final String hostName,
        final JobSearchService jobSearchService,
        final ApplicationEventPublisher publisher,
        final ApplicationEventMulticaster eventMulticaster,
        final TaskScheduler scheduler,
        final Executor executor,
        final Registry registry,
        final Resource jobsDir,
        final JobsProperties jobsProperties
    ) throws IOException {
        this.hostName = hostName;
        this.jobSearchService = jobSearchService;
        this.publisher = publisher;
        this.eventMulticaster = eventMulticaster;
        this.scheduler = scheduler;
        this.executor = executor;
        this.registry = registry;
        this.jobsDir = jobsDir.getFile();
        this.jobsProperties = jobsProperties;

        // Automatically track the number of jobs running on this node
        this.registry.mapSize("genie.jobs.running.gauge", this.jobMonitors);
        this.registry.mapSize("genie.jobs.scheduled.gauge", this.scheduledJobs);
        this.registry.methodValue("genie.jobs.active.gauge", this, "getNumActiveJobs");
        this.registry.methodValue("genie.jobs.memory.used.gauge", this, "getUsedMemory");
        this.unableToCancel = registry.counter("genie.jobs.unableToCancel.rate");
        this.unableToReAttach = registry.counter("genie.jobs.unableToReAttach.rate");
    }

    /**
     * When this application is fully up and running this method should be triggered by an event. It will query the
     * database to find any jobs already running on this node that aren't in the map. The use case for this is if
     * the Genie application crashes when it comes back up it can find the jobs again and not leave them orphaned.
     *
     * @param event The spring boot application ready event indicating the application is ready to start taking load
     * @throws GenieException on unrecoverable error
     */
    @EventListener
    public void onStartup(final ApplicationReadyEvent event) throws GenieException {
        log.info("Application is ready according to event {}. Attempting to re-attach to any active jobs", event);
        final Set<Job> jobs = this.jobSearchService.getAllActiveJobsOnHost(this.hostName);
        if (jobs.isEmpty()) {
            log.info("No jobs currently active on this node.");
            return;
        } else {
            log.info("{} jobs currently active on this node at startup", jobs.size());
        }

        for (final Job job : jobs) {
            final String id = job.getId().orElseThrow(() -> new GenieServerException("Job has no id!"));
            if (this.jobMonitors.containsKey(id) || this.scheduledJobs.containsKey(id)) {
                log.info("Job {} is already being tracked. Ignoring.", id);
            } else if (job.getStatus() != JobStatus.RUNNING) {
                this.eventMulticaster.multicastEvent(
                    new JobFinishedEvent(id, JobFinishedReason.SYSTEM_CRASH, "System crashed while job starting", this)
                );
            } else {
                try {
                    final JobExecution jobExecution = this.jobSearchService.getJobExecution(id);
                    this.jobMemories.put(id, jobExecution.getMemory().orElse(0));
                    this.scheduleMonitor(jobExecution);
                    log.info("Re-attached a job monitor to job {}", id);
                } catch (final GenieException ge) {
                    log.error("Unable to re-attach to job {}.", id);
                    this.eventMulticaster.multicastEvent(
                        new JobFinishedEvent(id, JobFinishedReason.SYSTEM_CRASH, "Unable to re-attach on startup", this)
                    );
                    this.unableToReAttach.increment();
                }
            }
        }
    }

    /**
     * This event is fired when a job is scheduled to run on this Genie node. We'll track the future here in case
     * it needs to be killed while still in INIT state. Once it's running the onJobStarted event will clear it out.
     *
     * @param event The job scheduled event with information for tracking the job through the INIT stage
     */
    @EventListener
    public synchronized void onJobScheduled(final JobScheduledEvent event) {
        // Increment the amount of memory used to account for this job
        this.jobMemories.put(event.getId(), event.getMemory());
        this.scheduledJobs.put(event.getId(), event.getTask());
    }

    /**
     * This event is fired when a job is started on this Genie node. Will create a JobMonitor and schedule it
     * for monitoring.
     *
     * @param event The event of the started job
     */
    @EventListener
    public synchronized void onJobStarted(final JobStartedEvent event) {
        final String jobId = event.getJobExecution().getId().orElseThrow(IllegalArgumentException::new);
        if (!this.jobMemories.containsKey(jobId)) {
            this.jobMemories.put(jobId, event.getJobExecution().getMemory().orElse(0));
        }
        this.scheduledJobs.remove(jobId);
        if (!this.jobMonitors.containsKey(jobId)) {
            this.scheduleMonitor(event.getJobExecution());
        }
    }

    /**
     * When a job is finished this event is fired. This method will cancel the task monitoring the job process.
     *
     * @param event the event of the finished job
     * @throws GenieException When a job execution can't be found (should never happen)
     */
    @EventListener
    public synchronized void onJobFinished(final JobFinishedEvent event) throws GenieException {
        final String jobId = event.getId();
        this.jobMemories.remove(jobId);
        if (this.jobMonitors.containsKey(jobId)) {
            //TODO: should we add back off it is unable to cancel?
            if (this.jobMonitors.get(jobId).cancel(true)) {
                log.debug("Successfully cancelled task monitoring job {}", jobId);
            } else {
                log.error("Unable to cancel task monitoring job {}", jobId);
                this.unableToCancel.increment();
            }
            this.jobMonitors.remove(jobId);
        } else if (this.scheduledJobs.containsKey(jobId)) {
            final Future<?> task = this.scheduledJobs.get(jobId);
            // If this job setup isn't actually done try killing it
            // TODO: If we can't kill should we have back-off?
            if (!task.isDone()) {
                if (task.cancel(true)) {
                    log.debug("Successfully cancelled job init task for job {}", jobId);
                } else {
                    log.error("Unable to cancel job init task for job {}", jobId);
                    this.unableToCancel.increment();
                }
            }
            this.scheduledJobs.remove(jobId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumActiveJobs() {
        return this.jobMonitors.size() + this.scheduledJobs.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUsedMemory() {
        return this.jobMemories.values().stream().reduce((a, b) -> a + b).orElse(0);
    }

    private void scheduleMonitor(final JobExecution jobExecution) {
        final String jobId = jobExecution.getId().orElseThrow(IllegalArgumentException::new);
        final File stdOut = new File(this.jobsDir, jobId + "/" + JobConstants.STDOUT_LOG_FILE_NAME);
        final File stdErr = new File(this.jobsDir, jobId + "/" + JobConstants.STDERR_LOG_FILE_NAME);

        final JobMonitor monitor = new JobMonitor(
            jobExecution,
            stdOut,
            stdErr,
            this.executor,
            this.publisher,
            this.eventMulticaster,
            this.registry,
            this.jobsProperties
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
        this.jobMonitors.put(jobId, future);
        log.info("Scheduled job monitoring for Job {}", jobExecution.getId());
    }
}
