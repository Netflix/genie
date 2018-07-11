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
package com.netflix.genie.web.tasks.node;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.properties.DiskCleanupProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.tasks.TaskUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.Executor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This task runs on every Genie node and is responsible for cleaning up the local disk so that space can be
 * recaptured.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class DiskCleanupTask implements Runnable {

    private final DiskCleanupProperties properties;
    private final File jobsDir;
    private final JobSearchService jobSearchService;
    private final boolean runAsUser;
    private final Executor processExecutor;

    private final AtomicLong numberOfDeletedJobDirs;
    private final AtomicLong numberOfDirsUnableToDelete;
    private final Counter unableToGetJobCounter;
    private final Counter unableToDeleteJobDirCounter;

    /**
     * Constructor. Schedules this task to be run by the task scheduler.
     *
     * @param properties       The disk cleanup properties to use.
     * @param scheduler        The scheduler to use to schedule the cron trigger.
     * @param jobsDir          The resource representing the location of the job directory
     * @param jobSearchService The service to find jobs with
     * @param jobsProperties   The jobs properties to use
     * @param processExecutor  The process executor to use to delete directories
     * @param registry         The metrics registry
     * @throws IOException When it is unable to open a file reference to the job directory
     */
    public DiskCleanupTask(
        @NotNull final DiskCleanupProperties properties,
        @NotNull final TaskScheduler scheduler,
        @NotNull final Resource jobsDir,
        @NotNull final JobSearchService jobSearchService,
        @NotNull final JobsProperties jobsProperties,
        @NotNull final Executor processExecutor,
        @NotNull final MeterRegistry registry
    ) throws IOException {
        // Job Directory is guaranteed to exist by the MvcConfig bean creation but just in case someone overrides
        if (!jobsDir.exists()) {
            throw new IOException("Jobs dir " + jobsDir + " doesn't exist. Unable to create task to cleanup.");
        }

        this.properties = properties;
        this.jobsDir = jobsDir.getFile();
        this.jobSearchService = jobSearchService;
        this.runAsUser = jobsProperties.getUsers().isRunAsUserEnabled();
        this.processExecutor = processExecutor;

        this.numberOfDeletedJobDirs = registry.gauge(
            "genie.tasks.diskCleanup.numberDeletedJobDirs.gauge",
            new AtomicLong()
        );
        this.numberOfDirsUnableToDelete = registry.gauge(
            "genie.tasks.diskCleanup.numberDirsUnableToDelete.gauge",
            new AtomicLong()
        );
        this.unableToGetJobCounter = registry.counter("genie.tasks.diskCleanup.unableToGetJobs.rate");
        this.unableToDeleteJobDirCounter = registry.counter("genie.tasks.diskCleanup.unableToDeleteJobsDir.rate");

        // Only schedule the task if we don't need sudo while on a non-unix system
        if (this.runAsUser && !SystemUtils.IS_OS_UNIX) {
            log.error("System is not UNIX like. Unable to schedule disk cleanup due to needing Unix commands");
        } else {
            final CronTrigger trigger = new CronTrigger(properties.getExpression(), JobConstants.UTC);
            scheduler.schedule(this, trigger);
        }
    }

    /**
     * Checks the disk for jobs on this host. Deletes any job directories that are older than the desired
     * retention and are complete.
     */
    @Override
    public void run() {
        log.info("Running disk cleanup task...");
        final File[] jobDirs = this.jobsDir.listFiles();
        if (jobDirs == null) {
            log.warn("No job dirs found. Returning.");
            this.numberOfDeletedJobDirs.set(0);
            this.numberOfDirsUnableToDelete.set(0);
            return;
        }
        // For each of the directories figure out if we need to delete the files or not
        long deletedCount = 0;
        long unableToDeleteCount = 0;
        for (final File dir : jobDirs) {
            if (!dir.isDirectory()) {
                log.info("File {} isn't a directory. Skipping.", dir.getName());
                continue;
            }

            final String id = dir.getName();
            try {
                final Job job = this.jobSearchService.getJob(id);
                if (job.getStatus().isActive()) {
                    // Don't want to delete anything still going
                    continue;
                }

                // Delete anything with a finish time before today @12 AM UTC - retention
                final Instant midnightUTC = TaskUtils.getMidnightUTC();
                final Instant retentionThreshold = midnightUTC.minus(this.properties.getRetention(), ChronoUnit.DAYS);
                final Optional<Instant> finished = job.getFinished();
                if (finished.isPresent() && finished.get().isBefore(retentionThreshold)) {
                    log.info("Attempting to delete job directory for job {}", id);
                    if (this.runAsUser) {
                        final CommandLine commandLine = new CommandLine("sudo");
                        commandLine.addArgument("rm");
                        commandLine.addArgument("-rf");
                        commandLine.addArgument(dir.getAbsolutePath());
                        this.processExecutor.execute(commandLine);
                    } else {
                        // Save forking a process ourselves if we don't have to
                        FileUtils.deleteDirectory(dir);
                    }
                    deletedCount++;
                    log.info("Successfully deleted job directory for job {}", id);
                }
            } catch (final GenieException ge) {
                log.error("Unable to get job {}. Continuing.", id, ge);
                this.unableToGetJobCounter.increment();
                unableToDeleteCount++;
            } catch (final IOException ioe) {
                log.error("Unable to delete job directory for job with id: {}", id, ioe);
                this.unableToDeleteJobDirCounter.increment();
                unableToDeleteCount++;
            }
        }
        this.numberOfDeletedJobDirs.set(deletedCount);
        this.numberOfDirsUnableToDelete.set(unableToDeleteCount);
    }
}
