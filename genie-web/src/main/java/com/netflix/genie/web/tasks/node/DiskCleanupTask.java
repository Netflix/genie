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
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.web.properties.DiskCleanupProperties;
import com.netflix.genie.web.tasks.TaskUtils;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This task runs on every Genie node and is responsible for cleaning up the local disk so that space can be
 * recaptured.
 */
@ConditionalOnProperty("genie.tasks.databaseCleanup.enabled")
@Component
@Slf4j
public class DiskCleanupTask implements Runnable {

    private final DiskCleanupProperties properties;
    private final File jobsDir;
    private final JobSearchService jobSearchService;

    private final AtomicLong numberOfDeletedJobDirs;
    private final Counter unableToGetJobCounter;
    private final Counter unableToDeleteJobDirCounter;

    /**
     * Constructor. Schedules this task to be run by the task scheduler.
     *
     * @param properties       The disk cleanup properties to use.
     * @param scheduler        The scheduler to use to schedule the cron trigger.
     * @param jobsDir          The resource representing the location of the job directory
     * @param jobSearchService The service to find jobs with
     * @param registry         The metrics registry
     * @throws IOException When it is unable to open a file reference to the job directory
     */
    @Autowired
    public DiskCleanupTask(
        @NotNull final DiskCleanupProperties properties,
        @NotNull final TaskScheduler scheduler,
        @NotNull final Resource jobsDir,
        @NotNull final JobSearchService jobSearchService,
        @NotNull final Registry registry
    ) throws IOException {
        // Job Directory is guaranteed to exist by the MvcConfig bean creation but just in case someone overrides
        if (!jobsDir.exists()) {
            throw new IOException("Jobs dir " + jobsDir + " doesn't exist. Unable to create task to cleanup.");
        }

        this.properties = properties;
        this.jobsDir = jobsDir.getFile();
        this.jobSearchService = jobSearchService;

        this.numberOfDeletedJobDirs
            = registry.gauge("genie.tasks.diskCleanup.numberDeletedJobDirs.gauge", new AtomicLong());
        this.unableToGetJobCounter = registry.counter("genie.tasks.diskCleanup.unableToGetJobs.rate");
        this.unableToDeleteJobDirCounter = registry.counter("genie.tasks.diskCleanup.unableToDeleteJobsDir.rate");

        final CronTrigger trigger = new CronTrigger(properties.getExpression(), JobConstants.UTC);
        scheduler.schedule(this, trigger);
    }

    /**
     * Checks the disk for jobs on this host. Deletes any job directories that are older than the desired
     * retention and are complete.
     */
    @Override
    public void run() {
        final File[] jobDirs = this.jobsDir.listFiles();
        if (jobDirs == null) {
            log.warn("No job dirs found. Returning.");
            this.numberOfDeletedJobDirs.set(0);
            return;
        }
        // For each of the directories figure out if we need to delete the files or not
        long deletedCount = 0;
        for (final File dir : jobDirs) {
            if (!dir.isDirectory()) {
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
                final Calendar retentionThreshold = TaskUtils.getMidnightUTC();
                TaskUtils.subtractDaysFromDate(retentionThreshold, this.properties.getRetention());
                if (job.getFinished().before(retentionThreshold.getTime())) {
                    FileUtils.deleteDirectory(dir);
                    deletedCount++;
                }
            } catch (final GenieException ge) {
                log.error("Unable to get job {}. Continuing.", id, ge);
                this.unableToGetJobCounter.increment();
            } catch (final IOException ioe) {
                log.error("Unable to delete job directory for job with id: {}", id, ioe);
                this.unableToDeleteJobDirCounter.increment();
            }
        }
        this.numberOfDeletedJobDirs.set(deletedCount);
    }
}
