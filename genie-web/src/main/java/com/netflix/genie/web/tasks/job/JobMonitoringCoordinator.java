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
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.events.JobFinishedEvent;
import com.netflix.genie.web.events.JobFinishedReason;
import com.netflix.genie.web.events.JobStartedEvent;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.services.JobSubmitterService;
import com.netflix.genie.web.services.impl.JobStateServiceImpl;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
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
public class JobMonitoringCoordinator extends JobStateServiceImpl {
    private final String hostname;
    private final JobSearchService jobSearchService;
    private final Executor executor;
    private final File jobsDir;
    private final JobsProperties jobsProperties;

    private final Counter unableToReAttach;

    /**
     * Constructor.
     *
     * @param genieHostInfo       Information about the host the Genie process is currently running on
     * @param jobSearchService    The search service to use to find jobs
     * @param genieEventBus       The Genie event bus to use for publishing events
     * @param scheduler           The task scheduler to use to register scheduling of job checkers
     * @param executor            The executor to use to launch processes
     * @param registry            The metrics registry
     * @param jobsDir             The directory where job output is stored
     * @param jobsProperties      The properties pertaining to jobs
     * @param jobSubmitterService implementation of the job submitter service
     * @throws IOException on error with the filesystem
     */
    @Autowired
    public JobMonitoringCoordinator(
        final GenieHostInfo genieHostInfo,
        final JobSearchService jobSearchService,
        final GenieEventBus genieEventBus,
        @Qualifier("genieTaskScheduler") final TaskScheduler scheduler,
        final Executor executor,
        final MeterRegistry registry,
        final Resource jobsDir,
        final JobsProperties jobsProperties,
        final JobSubmitterService jobSubmitterService
    ) throws IOException {
        super(jobSubmitterService, scheduler, genieEventBus, registry);
        this.hostname = genieHostInfo.getHostname();
        this.jobSearchService = jobSearchService;
        this.executor = executor;
        this.jobsDir = jobsDir.getFile();
        this.jobsProperties = jobsProperties;

        // Automatically track the number of jobs running on this node
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
     * This event is fired when a job is started on this Genie node. Will create a JobMonitor and schedule it
     * for monitoring.
     *
     * @param event The event of the started job
     */
    @EventListener
    public void onJobStarted(final JobStartedEvent event) {
        final String jobId = event.getJobExecution().getId().orElseThrow(IllegalArgumentException::new);
        setMemoryAndTask(jobId, event.getJobExecution().getMemory().orElse(0),
            scheduleMonitor(event.getJobExecution()));
    }

    /**
     * When a job is finished this event is fired. This method will cancel the task monitoring the job process.
     *
     * @param event the event of the finished job
     * @throws GenieException When a job execution can't be found (should never happen)
     */
    @EventListener
    public void onJobFinished(final JobFinishedEvent event) throws GenieException {
        this.done(event.getId());
    }

    private void reAttach(final ApplicationEvent event) throws GenieException {
        log.info("Application is ready according to event {}. Attempting to re-attach to any active jobs", event);
        final Set<Job> jobsOnHost = this.jobSearchService.getAllActiveJobsOnHost(this.hostname);
        if (jobsOnHost.isEmpty()) {
            log.info("No jobs currently active on this node.");
            return;
        } else {
            log.info("{} jobs currently active on this node at startup", jobsOnHost.size());
        }

        for (final Job job : jobsOnHost) {
            final String id = job.getId().orElseThrow(() -> new GenieServerException("Job has no id!"));
            if (jobExists(id)) {
                log.info("Job {} is already being tracked. Ignoring.", id);
            } else if (job.getStatus() != JobStatus.RUNNING) {
                this.genieEventBus.publishAsynchronousEvent(
                    new JobFinishedEvent(
                        id, JobFinishedReason.SYSTEM_CRASH, JobStatusMessages.SYSTEM_CRASHED_WHILE_JOB_STARTING, this
                    )
                );
            } else {
                try {
                    final JobExecution jobExecution = this.jobSearchService.getJobExecution(id);
                    init(id);
                    setMemoryAndTask(id, jobExecution.getMemory().orElse(0), scheduleMonitor(jobExecution));
                    log.info("Re-attached a job monitor to job {}", id);
                } catch (final GenieException ge) {
                    log.error("Unable to re-attach to job {}.", id, ge);
                    this.genieEventBus.publishAsynchronousEvent(
                        new JobFinishedEvent(
                            id, JobFinishedReason.SYSTEM_CRASH, JobStatusMessages.UNABLE_TO_RE_ATTACH_ON_STARTUP, this
                        )
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
            this.genieEventBus,
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
}
