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
import com.netflix.genie.web.data.services.ClusterPersistenceService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.FilePersistenceService;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.data.services.TagPersistenceService;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.tasks.TaskUtils;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;

import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link LeaderTask} which will clean up the database of old records if desired.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class DatabaseCleanupTask extends LeaderTask {

    private static final String DATABASE_CLEANUP_DURATION_TIMER_NAME = "genie.tasks.databaseCleanup.duration.timer";
    private static final String CLUSTER_DELETION_TIMER = "genie.tasks.databaseCleanup.clusterDeletion.timer";
    private static final String FILE_DELETION_TIMER = "genie.tasks.databaseCleanup.fileDeletion.timer";
    private static final String TAG_DELETION_TIMER = "genie.tasks.databaseCleanup.tagDeletion.timer";

    private final DatabaseCleanupProperties cleanupProperties;
    private final Environment environment;
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
     * @param cleanupProperties The properties to use to configure this task
     * @param environment       The application environment to pull properties from
     * @param dataServices      The {@link DataServices} encapsulation instance to use
     * @param registry          The metrics registry
     */
    public DatabaseCleanupTask(
        @NotNull final DatabaseCleanupProperties cleanupProperties,
        @NotNull final Environment environment,
        @NotNull final DataServices dataServices,
        @NotNull final MeterRegistry registry
    ) {
        this.registry = registry;
        this.cleanupProperties = cleanupProperties;
        this.environment = environment;
        this.jobPersistenceService = dataServices.getJobPersistenceService();
        this.clusterPersistenceService = dataServices.getClusterPersistenceService();
        this.filePersistenceService = dataServices.getFilePersistenceService();
        this.tagPersistenceService = dataServices.getTagPersistenceService();

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
        final String expression = this.environment.getProperty(
            DatabaseCleanupProperties.EXPRESSION_PROPERTY,
            String.class,
            this.cleanupProperties.getExpression()
        );
        return new CronTrigger(expression, JobConstants.UTC);
    }

    /**
     * Clean out database based on date.
     */
    @Override
    public void run() {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            this.deleteJobs();
            this.deleteClusters();

            // Get now - 1 hour to avoid deleting references that were created as part of new resources recently
            final Instant creationThreshold = Instant.now().minus(1L, ChronoUnit.HOURS);

            this.deleteFiles(creationThreshold);
            this.deleteTags(creationThreshold);

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

    /*
     * Delete jobs that are older than the retention threshold and are complete
     */
    private void deleteJobs() {
        final boolean skipJobs = this.environment.getProperty(
            DatabaseCleanupProperties.JobDatabaseCleanupProperties.SKIP_PROPERTY,
            Boolean.class,
            this.cleanupProperties.getJobCleanup().isSkip()
        );
        if (skipJobs) {
            log.info("Skipping job cleanup");
            this.numDeletedJobs.set(0);
        } else {
            final Instant midnightUTC = TaskUtils.getMidnightUTC();
            final Instant retentionLimit = midnightUTC.minus(
                this.environment.getProperty(
                    DatabaseCleanupProperties.JobDatabaseCleanupProperties.JOB_RETENTION_PROPERTY,
                    Integer.class,
                    this.cleanupProperties.getJobCleanup().getRetention()
                ),
                ChronoUnit.DAYS
            );
            final int batchSize = this.environment.getProperty(
                DatabaseCleanupProperties.JobDatabaseCleanupProperties.MAX_DELETED_PER_TRANSACTION_PROPERTY,
                Integer.class,
                this.cleanupProperties.getJobCleanup().getMaxDeletedPerTransaction()
            );
            final int pageSize = this.environment.getProperty(
                DatabaseCleanupProperties.JobDatabaseCleanupProperties.PAGE_SIZE_PROPERTY,
                Integer.class,
                this.cleanupProperties.getJobCleanup().getPageSize()
            );

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
            log.info(
                "Deleted {} jobs",
                totalDeletedJobs
            );
            this.numDeletedJobs.set(totalDeletedJobs);
        }
    }

    /*
     * Delete all clusters that are marked terminated and aren't attached to any jobs after jobs were deleted.
     */
    private void deleteClusters() {
        final long startTime = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final boolean skipClusters = this.environment.getProperty(
                DatabaseCleanupProperties.ClusterDatabaseCleanupProperties.SKIP_PROPERTY,
                Boolean.class,
                this.cleanupProperties.getClusterCleanup().isSkip()
            );
            if (skipClusters) {
                log.info("Skipping clusters cleanup");
                this.numDeletedClusters.set(0);
            } else {
                final long countDeletedClusters = this.clusterPersistenceService.deleteTerminatedClusters();
                log.info(
                    "Deleted {} clusters that were in TERMINATED state and weren't attached to any jobs",
                    countDeletedClusters
                );
                this.numDeletedClusters.set(countDeletedClusters);
            }
        } catch (final Exception e) {
            log.error("Unable to delete clusters from database", e);
            MetricsUtils.addFailureTagsWithException(tags, e);
        } finally {
            this.registry
                .timer(CLUSTER_DELETION_TIMER, tags)
                .record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }

    private void deleteFiles(final Instant creationThreshold) {
        final long startTime = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final boolean skipFiles = this.environment.getProperty(
                DatabaseCleanupProperties.FileDatabaseCleanupProperties.SKIP_PROPERTY,
                Boolean.class,
                this.cleanupProperties.getFileCleanup().isSkip()
            );
            if (skipFiles) {
                log.info("Skipping files cleanup");
                this.numDeletedFiles.set(0);
            } else {
                final long countDeletedFiles = this.filePersistenceService.deleteUnusedFiles(creationThreshold);
                log.info(
                    "Deleted {} files that were unused by any resource and created over an hour ago",
                    countDeletedFiles
                );
                this.numDeletedFiles.set(countDeletedFiles);
            }
        } catch (final Exception e) {
            log.error("Unable to delete files from database", e);
            MetricsUtils.addFailureTagsWithException(tags, e);
        } finally {
            this.registry
                .timer(FILE_DELETION_TIMER, tags)
                .record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }

    private void deleteTags(final Instant creationThreshold) {
        final long startTime = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final boolean skipTags = this.environment.getProperty(
                DatabaseCleanupProperties.TagDatabaseCleanupProperties.SKIP_PROPERTY,
                Boolean.class,
                this.cleanupProperties.getTagCleanup().isSkip()
            );
            if (skipTags) {
                log.info("Skipping tags cleanup");
                this.numDeletedTags.set(0);
            } else {
                final long countDeletedTags = this.tagPersistenceService.deleteUnusedTags(creationThreshold);
                log.info(
                    "Deleted {} tags that were unused by any resource and created over an hour ago",
                    countDeletedTags
                );
                this.numDeletedTags.set(countDeletedTags);
            }
        } catch (final Exception e) {
            log.error("Unable to delete tags from database", e);
            MetricsUtils.addFailureTagsWithException(tags, e);
        } finally {
            this.registry
                .timer(TAG_DELETION_TIMER, tags)
                .record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }
}
