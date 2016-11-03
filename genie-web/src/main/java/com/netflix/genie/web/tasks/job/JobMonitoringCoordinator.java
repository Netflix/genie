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
import com.netflix.genie.core.jobs.JobLauncher;
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.core.services.JobMetricsService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.Executor;
import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.ContextRefreshedEvent;
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
import java.util.function.Supplier;

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

    private final Map<String, JobInfo> jobs = Collections.synchronizedMap(new HashMap<>());
    private final String hostName;
    private final JobSearchService jobSearchService;
    private final TaskScheduler scheduler;
    private final ApplicationEventPublisher publisher;
    private final ApplicationEventMulticaster eventMulticaster;
    private final Executor executor;
    private final Registry registry;
    private final File jobsDir;
    private final JobsProperties jobsProperties;
    private final JobSubmitterService jobSubmitterService;

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
     * @param jobSubmitterService   implementation of the job submitter service
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
        final JobsProperties jobsProperties,
        final JobSubmitterService jobSubmitterService
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
        this.jobSubmitterService = jobSubmitterService;

        // Automatically track the number of jobs running on this node
        this.registry.mapSize("genie.jobs.running.gauge", this.jobs);
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
     * @param event The spring ready event indicating the application is ready to start taking load
     * @throws GenieException on unrecoverable error
     */
    @EventListener
    public void onStartup(final ContextRefreshedEvent event) throws GenieException {
        this.reAttach(event);
    }

    /**
     * This event is fired when a job is scheduled to run on this Genie node. We'll track the future here in case
     * it needs to be killed while still in INIT state. Once it's running the onJobStarted event will clear it out.
     *
     * @param event The job scheduled event with information for tracking the job through the INIT stage
     */
    @EventListener
    public void onJobScheduled(final JobScheduledEvent event) {
        final String jobId = event.getId();
        jobs.put(jobId, new JobInfo());
        handle(event.getId(), () -> {
            final JobInfo jobInfo = jobs.get(jobId);
            jobInfo.setMemory(event.getMemory());
            final JobLauncher jobLauncher = new JobLauncher(this.jobSubmitterService,
                event.getJobRequest(),
                event.getCluster(),
                event.getCommand(),
                event.getApplications(),
                event.getMemory(),
                this.registry
            );
            jobInfo.setRunningTask(scheduler.schedule(jobLauncher, Instant.now().toDate()));
            return null;
        });
    }

    /**
     * This event is fired when a job is started on this Genie node. Will create a JobMonitor and schedule it
     * for monitoring.
     *
     * @param event The event of the started job
     */
    @EventListener
    public void onJobStarted(final JobStartedEvent event) {
        final String jobId = event.getJobExecution().getId().orElseThrow(IllegalArgumentException::new);
        handle(jobId, () -> {
            final JobInfo jobInfo = jobs.get(jobId);
            jobInfo.setMemory(event.getJobExecution().getMemory().orElse(0));
            jobInfo.setRunningTask(scheduleMonitor(event.getJobExecution()));
            return null;
        });
    }

    /**
     * When a job is finished this event is fired. This method will cancel the task monitoring the job process.
     *
     * @param event the event of the finished job
     * @throws GenieException When a job execution can't be found (should never happen)
     */
    @EventListener
    public void onJobFinished(final JobFinishedEvent event) throws GenieException {
        final String jobId = event.getId();
        handle(jobId, () -> {
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
        });
    }

    private void handle(final String jobId, final Supplier<Void> supplier) {
        JobInfo jobInfo = jobs.get(jobId);
        if (jobInfo != null) {
            synchronized (jobInfo) {
                jobInfo = jobs.get(jobId);
                if (jobInfo != null) {
                    if (!jobInfo.isDone()) {
                        supplier.get();
                    } else {
                        jobs.remove(jobId);
                    }
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumActiveJobs() {
        return jobs.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUsedMemory() {
        // Synchronized to avoid concurrent modification exception
        synchronized (this.jobs) {
            return this.jobs.values().stream().map(JobInfo::getMemory).reduce((a, b) -> a + b).orElse(0);
        }
    }

    private void reAttach(final ApplicationEvent event) throws GenieException {
        log.info("Application is ready according to event {}. Attempting to re-attach to any active jobs", event);
        final Set<Job> jobsOnHost = this.jobSearchService.getAllActiveJobsOnHost(this.hostName);
        if (jobsOnHost.isEmpty()) {
            log.info("No jobs currently active on this node.");
            return;
        } else {
            log.info("{} jobs currently active on this node at startup", jobsOnHost.size());
        }

        for (final Job job : jobsOnHost) {
            final String id = job.getId().orElseThrow(() -> new GenieServerException("Job has no id!"));
            if (this.jobs.containsKey(id)) {
                log.info("Job {} is already being tracked. Ignoring.", id);
            } else if (job.getStatus() != JobStatus.RUNNING) {
                this.eventMulticaster.multicastEvent(
                    new JobFinishedEvent(id, JobFinishedReason.SYSTEM_CRASH, "System crashed while job starting", this)
                );
            } else {
                try {
                    final JobExecution jobExecution = this.jobSearchService.getJobExecution(id);
                    final JobInfo jobInfo = new JobInfo();
                    jobInfo.setMemory(jobExecution.getMemory().orElse(0));
                    jobInfo.setRunningTask(scheduleMonitor(jobExecution));
                    this.jobs.putIfAbsent(id, jobInfo);
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

    private Future<?> scheduleMonitor(final JobExecution jobExecution) {
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
        log.info("Scheduled job monitoring for Job {}", jobExecution.getId());
        return future;
    }

    @Getter
    @Setter
    private static class JobInfo {
        private Future<?> runningTask;
        private Integer memory;
        private boolean done;
    }
}
