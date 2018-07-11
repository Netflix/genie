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

import com.google.common.collect.Sets;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.FilePersistenceService;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.TagPersistenceService;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.tasks.TaskUtils;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;

import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A task which will clean up the database of old jobs if desired.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class DatabaseCleanupTask extends LeadershipTask {

    private static final String DATABASE_CLEANUP_DURATION_TIMER_NAME = "genie.tasks.databaseCleanup.duration.timer";
    private final DatabaseCleanupProperties cleanupProperties;
    private final JobPersistenceService jobPersistenceService;
    private final ClusterPersistenceService clusterPersistenceService;
    private final FilePersistenceService filePersistenceService;
    private final TagPersistenceService tagPersistenceService;

    private final MeterRegistry registry;
    private final AtomicLong numDeletedJobs;
    private final AtomicLong numDeletedClusters;
    private final AtomicLong numDeletedTags;
    private final AtomicLong numDeletedFiles;

    /**
     * Constructor.
     *
     * @param cleanupProperties         The properties to use to configure this task
     * @param jobPersistenceService     The persistence service to use to cleanup the data store
     * @param clusterPersistenceService The cluster service to use to delete terminated clusters
     * @param filePersistenceService    The file service to use to delete unused file references
     * @param tagPersistenceService     The tag service to use to delete unused tag references
     * @param registry                  The metrics registry
     */
    public DatabaseCleanupTask(
        @NotNull final DatabaseCleanupProperties cleanupProperties,
        @NotNull final JobPersistenceService jobPersistenceService,
        @NotNull final ClusterPersistenceService clusterPersistenceService,
        @NotNull final FilePersistenceService filePersistenceService,
        @NotNull final TagPersistenceService tagPersistenceService,
        @NotNull final MeterRegistry registry
    ) {
        this.registry = registry;
        this.cleanupProperties = cleanupProperties;
        this.jobPersistenceService = jobPersistenceService;
        this.clusterPersistenceService = clusterPersistenceService;
        this.filePersistenceService = filePersistenceService;
        this.tagPersistenceService = tagPersistenceService;

        this.numDeletedJobs = this.registry.gauge(
            "genie.tasks.databaseCleanup.numDeletedJobs.gauge",
            new AtomicLong()
        );
        this.numDeletedClusters = this.registry.gauge(
            "genie.tasks.databaseCleanup.numDeletedClusters.gauge",
            new AtomicLong()
        );
        this.numDeletedTags = this.registry.gauge(
            "genie.tasks.databaseCleanup.numDeletedTags.gauge",
            new AtomicLong()
        );
        this.numDeletedFiles = this.registry.gauge(
            "genie.tasks.databaseCleanup.numDeletedFiles.gauge",
            new AtomicLong()
        );
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
        final Set<Tag> tags = Sets.newHashSet();
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
                final long countDeletedClusters = this.clusterPersistenceService.deleteTerminatedClusters();
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
                final long countDeletedFiles = this.filePersistenceService.deleteUnusedFiles(creationThreshold);
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
                final long countDeletedTags = this.tagPersistenceService.deleteUnusedTags(creationThreshold);
                log.info(
                    "Deleted {} tags that were unused by any resource and created over an hour ago",
                    countDeletedTags
                );
                this.numDeletedTags.set(countDeletedTags);
            }

            MetricsUtils.addSuccessTags(tags);
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry
                .timer(DATABASE_CLEANUP_DURATION_TIMER_NAME, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
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
        final Instant midnightUTC = TaskUtils.getMidnightUTC();
        final Instant retentionLimit = midnightUTC.minus(this.cleanupProperties.getRetention(), ChronoUnit.DAYS);
        final int batchSize = this.cleanupProperties.getMaxDeletedPerTransaction();
        final int pageSize = this.cleanupProperties.getPageSize();

        log.info(
            "Attempting to delete jobs from before {} in batches of {} jobs per iteration",
            retentionLimit,
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
