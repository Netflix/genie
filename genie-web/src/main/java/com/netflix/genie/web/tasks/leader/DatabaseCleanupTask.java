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
package com.netflix.genie.web.tasks.leader;

import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.FileService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.TagService;
import com.netflix.genie.core.util.MetricsUtils;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.tasks.TaskUtils;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A task which will clean up the database of old jobs if desired.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConditionalOnProperty("genie.tasks.databaseCleanup.enabled")
@Component
@Slf4j
public class DatabaseCleanupTask extends LeadershipTask {

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private final DatabaseCleanupProperties cleanupProperties;
    private final JobPersistenceService jobPersistenceService;
    private final ClusterService clusterService;
    private final FileService fileService;
    private final TagService tagService;

    private final Registry registry;
    private final AtomicLong numDeletedJobs;
    private final AtomicLong numDeletedClusters;
    private final AtomicLong numDeletedTags;
    private final AtomicLong numDeletedFiles;
    private final Id deletionTimerId;

    /**
     * Constructor.
     *
     * @param cleanupProperties     The properties to use to configure this task
     * @param jobPersistenceService The persistence service to use to cleanup the data store
     * @param clusterService        The cluster service to use to delete terminated clusters
     * @param fileService           The file service to use to delete unused file references
     * @param tagService            The tag service to use to delete unused tag references
     * @param registry              The metrics registry
     */
    @Autowired
    public DatabaseCleanupTask(
        @NotNull final DatabaseCleanupProperties cleanupProperties,
        @NotNull final JobPersistenceService jobPersistenceService,
        @NotNull final ClusterService clusterService,
        @NotNull final FileService fileService,
        @NotNull final TagService tagService,
        @NotNull final Registry registry
    ) {
        this.registry = registry;
        this.cleanupProperties = cleanupProperties;
        this.jobPersistenceService = jobPersistenceService;
        this.clusterService = clusterService;
        this.fileService = fileService;
        this.tagService = tagService;

        this.numDeletedJobs = PolledMeter
            .using(registry)
            .withName("genie.tasks.databaseCleanup.numDeletedJobs.gauge")
            .monitorValue(new AtomicLong());
        this.numDeletedClusters = PolledMeter
            .using(registry)
            .withName("genie.tasks.databaseCleanup.numDeletedClusters.gauge")
            .monitorValue(new AtomicLong());
        this.numDeletedTags = PolledMeter
            .using(registry)
            .withName("genie.tasks.databaseCleanup.numDeletedTags.gauge")
            .monitorValue(new AtomicLong());
        this.numDeletedFiles = PolledMeter
            .using(registry)
            .withName("genie.tasks.databaseCleanup.numDeletedFiles.gauge")
            .monitorValue(new AtomicLong());
        this.deletionTimerId = registry.createId("genie.tasks.databaseCleanup.duration.timer");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenieTaskScheduleType getScheduleType() {
        return GenieTaskScheduleType.TRIGGER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Trigger getTrigger() {
        return new CronTrigger(this.cleanupProperties.getExpression(), JobConstants.UTC);
    }

    /**
     * Clean out database based on date.
     */
    @Override
    public void run() {
        final long start = System.nanoTime();
        final Map<String, String> tags = MetricsUtils.newSuccessTagsMap();
        try {
            // Delete jobs that are older than the retention threshold and are complete
            if (this.cleanupProperties.isSkipJobsCleanup()) {
                log.debug("Skipping job cleanup");
                this.numDeletedJobs.set(0);
            } else {
                final long countDeletedJobs = this.deleteJobs();
                log.info(
                    "Deleted {} jobs",
                    countDeletedJobs
                );
                this.numDeletedJobs.set(countDeletedJobs);
            }

            // Delete all clusters that are marked terminated and aren't attached to any jobs after jobs were deleted
            if (this.cleanupProperties.isSkipClustersCleanup()) {
                log.debug("Skipping clusters cleanup");
                this.numDeletedClusters.set(0);
            } else {
                final int countDeletedClusters = this.clusterService.deleteTerminatedClusters();
                log.info(
                    "Deleted {} clusters that were in TERMINATED state and weren't attached to any jobs",
                    countDeletedClusters
                );
                this.numDeletedClusters.set(countDeletedClusters);
            }

            // Get now - 1 hour to avoid deleting references that were created as part of new resources recently
            final Instant creationThreshold = Instant.now().minus(1L, ChronoUnit.HOURS);

            if (this.cleanupProperties.isSkipFilesCleanup()) {
                log.debug("Skipping files cleanup");
                this.numDeletedFiles.set(0);
            } else {
                final long countDeletedFiles = this.fileService.deleteUnusedFiles(creationThreshold);
                log.info(
                    "Deleted {} files that were unused by any resource and created over an hour ago",
                    countDeletedFiles
                );
                this.numDeletedFiles.set(countDeletedFiles);
            }

            if (this.cleanupProperties.isSkipTagsCleanup()) {
                log.debug("Skipping tags cleanup");
                this.numDeletedTags.set(0);
            } else {
                final long countDeletedTags = this.tagService.deleteUnusedTags(creationThreshold);
                log.info(
                    "Deleted {} tags that were unused by any resource and created over an hour ago",
                    countDeletedTags
                );
                this.numDeletedTags.set(countDeletedTags);
            }
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            final long finish = System.nanoTime();
            this.registry
                .timer(this.deletionTimerId.withTags(tags))
                .record(finish - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
        this.numDeletedJobs.set(0L);
        this.numDeletedClusters.set(0L);
        this.numDeletedTags.set(0L);
        this.numDeletedFiles.set(0L);
    }

    private long deleteJobs() {
        final Calendar cal = TaskUtils.getMidnightUTC();
        // Move the date back the number of days retention is set for
        TaskUtils.subtractDaysFromDate(cal, this.cleanupProperties.getRetention());
        final Date retentionLimit = cal.getTime();
        final String retentionLimitString = this.dateFormat.format(retentionLimit);
        final int batchSize = this.cleanupProperties.getMaxDeletedPerTransaction();
        final int pageSize = this.cleanupProperties.getPageSize();

        log.info(
            "Attempting to delete jobs from before {} in batches of {} jobs per iteration",
            retentionLimitString,
            batchSize
        );
        long totalDeletedJobs = 0;
        while (true) {
            final long numberDeletedJobs = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(
                retentionLimit,
                batchSize,
                pageSize
            );
            totalDeletedJobs += numberDeletedJobs;
            if (numberDeletedJobs == 0) {
                break;
            }
        }
        return totalDeletedJobs;
    }
}
