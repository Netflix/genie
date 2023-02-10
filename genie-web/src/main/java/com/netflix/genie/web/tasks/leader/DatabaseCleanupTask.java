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
import com.netflix.genie.common.internal.dtos.ClusterStatus;
import com.netflix.genie.common.internal.dtos.CommandStatus;
import com.netflix.genie.common.internal.dtos.JobStatus;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
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
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link LeaderTask} which will clean up the database of old records if desired.
 *
 * @author tgianos
 * @since 3.0.0
 */
// TODO: The intention of this class is clear, it is to have the leader trigger a database cleanup action periodically
//       at system administrators discretion. The issue here is that this current implementation bleeds a lot of
//       details about the underlying implementation into this class. If someone were to re-implement the persistence
//       tier using a different underlying technology it is unlikely they would expose tags or files as separate
//       fields. Their existence here is merely a side effect of our relational database implementation. The proper
//       thing to do here seems to be to have this task merely kick off a single API call into the persistence tier
//       and then that tier does what it thinks is best. I (TJG) might have tackled this as part of the large
//       persistence tier refactoring in 4/2020 however looking at this class it has a lot of details that need to be
//       moved properly (metrics, logging, properties) that it looks like it's own larger initiative that I don't have
//       time to tackle right now. I do think it should be done though so I'm leaving this large note so as not to
//       forget and hopefully come back to it once there is some time. - TJG 4/21/2020
@Slf4j
public class DatabaseCleanupTask extends LeaderTask {

    private static final String DATABASE_CLEANUP_DURATION_TIMER_NAME = "genie.tasks.databaseCleanup.duration.timer";
    private static final String APPLICATION_DELETION_TIMER = "genie.tasks.databaseCleanup.applicationDeletion.timer";
    private static final String CLUSTER_DELETION_TIMER = "genie.tasks.databaseCleanup.clusterDeletion.timer";
    private static final String COMMAND_DEACTIVATION_TIMER = "genie.tasks.databaseCleanup.commandDeactivation.timer";
    private static final String COMMAND_DELETION_TIMER = "genie.tasks.databaseCleanup.commandDeletion.timer";
    private static final String FILE_DELETION_TIMER = "genie.tasks.databaseCleanup.fileDeletion.timer";
    private static final String TAG_DELETION_TIMER = "genie.tasks.databaseCleanup.tagDeletion.timer";

    // TODO: May want to make this a property
    private static final Set<CommandStatus> TO_DEACTIVATE_COMMAND_STATUSES = EnumSet.of(
        CommandStatus.DEPRECATED,
        CommandStatus.ACTIVE
    );
    // TODO: May want to make this a property
    private static final Set<CommandStatus> TO_DELETE_COMMAND_STATUSES = EnumSet.of(CommandStatus.INACTIVE);
    // TODO: May want to make this a property. Currently this maintains consistent behavior with before but it would
    //       be nice to add OUT_OF_SERVICE
    private static final Set<ClusterStatus> TO_DELETE_CLUSTER_STATUSES = EnumSet.of(ClusterStatus.TERMINATED);

    private final DatabaseCleanupProperties cleanupProperties;
    private final Environment environment;
    private final PersistenceService persistenceService;

    private final MeterRegistry registry;
    private final AtomicLong numDeletedJobs;
    private final AtomicLong numDeletedClusters;
    private final AtomicLong numDeactivatedCommands;
    private final AtomicLong numDeletedCommands;
    private final AtomicLong numDeletedApplications;
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
        this.persistenceService = dataServices.getPersistenceService();

        this.numDeletedJobs = this.registry.gauge(
            "genie.tasks.databaseCleanup.numDeletedJobs.gauge",
            new AtomicLong()
        );
        this.numDeletedClusters = this.registry.gauge(
            "genie.tasks.databaseCleanup.numDeletedClusters.gauge",
            new AtomicLong()
        );
        this.numDeactivatedCommands = this.registry.gauge(
            "genie.tasks.databaseCleanup.numDeactivatedCommands.gauge",
            new AtomicLong()
        );
        this.numDeletedCommands = this.registry.gauge(
            "genie.tasks.databaseCleanup.numDeletedCommands.gauge",
            new AtomicLong()
        );
        this.numDeletedApplications = this.registry.gauge(
            "genie.tasks.databaseCleanup.numDeletedApplications.gauge",
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
        final Instant runtime = Instant.now();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            this.deleteJobs();

            // Get now - 1 hour to avoid deleting references that were created as part of new resources recently
            final Instant creationThreshold = runtime.minus(1L, ChronoUnit.HOURS);

            this.deleteClusters(creationThreshold);
            this.deleteCommands(creationThreshold);
            this.deactivateCommands(runtime);
            this.deleteApplications(creationThreshold);
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
        this.numDeactivatedCommands.set(0L);
        this.numDeletedCommands.set(0L);
        this.numDeletedApplications.set(0L);
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
            // TODO: Maybe we shouldn't reset it to midnight no matter what... just go with runtime minus something
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
                DatabaseCleanupProperties.JobDatabaseCleanupProperties.PAGE_SIZE_PROPERTY,
                Integer.class,
                this.cleanupProperties.getJobCleanup().getPageSize()
            );

            log.info(
                "Attempting to delete jobs from before {} in batches of {} jobs per iteration",
                retentionLimit,
                batchSize
            );
            long numDeletedJobsInBatch;
            long totalDeletedJobs = 0L;
            do {
                numDeletedJobsInBatch = this.persistenceService.deleteJobsCreatedBefore(
                    retentionLimit,
                    JobStatus.getActiveStatuses(),
                    batchSize
                );
                totalDeletedJobs += numDeletedJobsInBatch;
            } while (numDeletedJobsInBatch != 0);
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
    private void deleteClusters(final Instant creationThreshold) {
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
                final int batchSize = this.environment.getProperty(
                    DatabaseCleanupProperties.BATCH_SIZE_PROPERTY,
                    Integer.class,
                    this.cleanupProperties.getBatchSize()
                );

                log.info(
                    "Attempting to delete unused clusters from before {} in batches of {}",
                    creationThreshold,
                    batchSize
                );

                long deleted;
                long totalDeleted = 0L;
                do {
                    deleted = this.persistenceService.deleteUnusedClusters(
                        TO_DELETE_CLUSTER_STATUSES,
                        creationThreshold,
                        batchSize
                    );
                    totalDeleted += deleted;
                } while (deleted > 0);

                log.info(
                    "Deleted {} clusters that were in one of {} states, were created before {} and weren't "
                        + " attached to any jobs",
                    totalDeleted,
                    TO_DELETE_CLUSTER_STATUSES,
                    creationThreshold
                );
                this.numDeletedClusters.set(totalDeleted);
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
                final int batchSize = this.environment.getProperty(
                    DatabaseCleanupProperties.BATCH_SIZE_PROPERTY,
                    Integer.class,
                    this.cleanupProperties.getBatchSize()
                );
                final long rollingWindowHours = this.environment.getProperty(
                    DatabaseCleanupProperties.FileDatabaseCleanupProperties.ROLLING_WINDOW_HOURS_PROPERTY,
                    Integer.class,
                    this.cleanupProperties.getFileCleanup().getRollingWindowHours()
                );
                final long batchDaysWithin = this.environment.getProperty(
                    DatabaseCleanupProperties.FileDatabaseCleanupProperties.BATCH_DAYS_WITHIN_PROPERTY,
                    Integer.class,
                    this.cleanupProperties.getFileCleanup().getBatchDaysWithin()
                );
                log.info(
                    "Attempting to delete unused files from before {} in batches of {}",
                    creationThreshold,
                    batchSize
                );

                long totalDeleted = 0L;
                Instant upperBound = creationThreshold;
                Instant lowerBound = creationThreshold.minus(rollingWindowHours, ChronoUnit.HOURS);
                final Instant batchLowerBound = creationThreshold.minus(batchDaysWithin, ChronoUnit.DAYS);
                while (upperBound.isAfter(batchLowerBound)) {
                    totalDeleted += deleteUnusedFilesBetween(lowerBound, upperBound, batchSize);
                    upperBound = lowerBound;
                    lowerBound = lowerBound.minus(rollingWindowHours, ChronoUnit.HOURS);
                }
                // do a final deletion of everything < batchLowerBound
                totalDeleted += deleteUnusedFilesBetween(Instant.EPOCH, upperBound, batchSize);
                log.info(
                    "Deleted {} files that were unused by any resource and created before {}",
                    totalDeleted,
                    creationThreshold
                );
                this.numDeletedFiles.set(totalDeleted);
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

    private long deleteUnusedFilesBetween(final Instant lowerBound, final Instant upperBound, final int batchSize) {
        long deleted;
        long totalDeleted = 0L;
        do {
            deleted = this.persistenceService.deleteUnusedFiles(lowerBound, upperBound, batchSize);
            totalDeleted += deleted;
        } while (deleted > 0);
        return totalDeleted;
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
                final int batchSize = this.environment.getProperty(
                    DatabaseCleanupProperties.BATCH_SIZE_PROPERTY,
                    Integer.class,
                    this.cleanupProperties.getBatchSize()
                );

                log.info(
                    "Attempting to delete unused tags from before {} in batches of {}",
                    creationThreshold,
                    batchSize
                );

                long deleted;
                long totalDeleted = 0L;
                do {
                    deleted = this.persistenceService.deleteUnusedTags(creationThreshold, batchSize);
                    totalDeleted += deleted;
                } while (deleted > 0);
                log.info(
                    "Deleted {} tags that were unused by any resource and created before {}",
                    totalDeleted,
                    creationThreshold
                );
                this.numDeletedTags.set(totalDeleted);
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

    private void deactivateCommands(final Instant runtime) {
        final long startTime = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final boolean skipDeactivation = this.environment.getProperty(
                DatabaseCleanupProperties.CommandDeactivationDatabaseCleanupProperties.SKIP_PROPERTY,
                Boolean.class,
                this.cleanupProperties.getCommandDeactivation().isSkip()
            );
            if (skipDeactivation) {
                log.info("Skipping command deactivation");
                this.numDeactivatedCommands.set(0);
            } else {
                final int batchSize = this.environment.getProperty(
                    DatabaseCleanupProperties.BATCH_SIZE_PROPERTY,
                    Integer.class,
                    this.cleanupProperties.getBatchSize()
                );

                final Instant commandCreationThreshold = runtime.minus(
                    this.environment.getProperty(
                        DatabaseCleanupProperties
                            .CommandDeactivationDatabaseCleanupProperties
                            .COMMAND_CREATION_THRESHOLD_PROPERTY,
                        Integer.class,
                        this.cleanupProperties.getCommandDeactivation().getCommandCreationThreshold()
                    ),
                    ChronoUnit.DAYS
                );
                log.info(
                    "Attempting to set commands to status {} that were previously in one of {} in batches of {}",
                    CommandStatus.INACTIVE,
                    TO_DEACTIVATE_COMMAND_STATUSES,
                    batchSize
                );
                long totalDeactivatedCommands = 0;
                long batchedDeactivated;
                do {
                    batchedDeactivated = this.persistenceService.updateStatusForUnusedCommands(
                        CommandStatus.INACTIVE,
                        commandCreationThreshold,
                        TO_DEACTIVATE_COMMAND_STATUSES,
                        batchSize
                    );
                    totalDeactivatedCommands += batchedDeactivated;
                } while (batchedDeactivated > 0);
                log.info(
                    "Set {} commands to status {} that were previously in one of {}",
                    totalDeactivatedCommands,
                    CommandStatus.INACTIVE,
                    TO_DEACTIVATE_COMMAND_STATUSES
                );
                this.numDeactivatedCommands.set(totalDeactivatedCommands);
            }
        } catch (final Exception e) {
            log.error("Unable to disable commands in database", e);
            MetricsUtils.addFailureTagsWithException(tags, e);
        } finally {
            this.registry
                .timer(COMMAND_DEACTIVATION_TIMER, tags)
                .record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }

    private void deleteCommands(final Instant creationThreshold) {
        final long startTime = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final boolean skipCommands = this.environment.getProperty(
                DatabaseCleanupProperties.CommandDatabaseCleanupProperties.SKIP_PROPERTY,
                Boolean.class,
                this.cleanupProperties.getCommandCleanup().isSkip()
            );
            if (skipCommands) {
                log.info("Skipping command cleanup");
                this.numDeletedCommands.set(0);
            } else {
                final int batchSize = this.environment.getProperty(
                    DatabaseCleanupProperties.BATCH_SIZE_PROPERTY,
                    Integer.class,
                    this.cleanupProperties.getBatchSize()
                );
                log.info(
                    "Attempting to delete unused commands from before {} in batches of {}",
                    creationThreshold,
                    batchSize
                );

                long deleted;
                long totalDeleted = 0L;
                do {
                    deleted = this.persistenceService.deleteUnusedCommands(
                        TO_DELETE_COMMAND_STATUSES,
                        creationThreshold,
                        batchSize
                    );
                    totalDeleted += deleted;
                } while (deleted > 0);
                log.info(
                    "Deleted {} commands that were unused by any resource and created before {}",
                    totalDeleted,
                    creationThreshold
                );
                this.numDeletedCommands.set(totalDeleted);
            }
        } catch (final Exception e) {
            log.error("Unable to delete commands in database", e);
            MetricsUtils.addFailureTagsWithException(tags, e);
        } finally {
            this.registry
                .timer(COMMAND_DELETION_TIMER, tags)
                .record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }

    private void deleteApplications(final Instant creationThreshold) {
        final long startTime = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final boolean skipApplications = this.environment.getProperty(
                DatabaseCleanupProperties.ApplicationDatabaseCleanupProperties.SKIP_PROPERTY,
                Boolean.class,
                this.cleanupProperties.getApplicationCleanup().isSkip()
            );
            if (skipApplications) {
                log.info("Skipping application cleanup");
                this.numDeletedCommands.set(0);
            } else {
                final int batchSize = this.environment.getProperty(
                    DatabaseCleanupProperties.BATCH_SIZE_PROPERTY,
                    Integer.class,
                    this.cleanupProperties.getBatchSize()
                );
                log.info(
                    "Attempting to delete unused applications from before {} in batches of {}",
                    creationThreshold,
                    batchSize
                );

                long deleted;
                long totalDeleted = 0L;
                do {
                    deleted = this.persistenceService.deleteUnusedApplications(
                        creationThreshold,
                        batchSize
                    );
                    totalDeleted += deleted;
                } while (deleted > 0);
                log.info(
                    "Deleted {} applications that were unused by any resource and created before {}",
                    totalDeleted,
                    creationThreshold
                );
                this.numDeletedApplications.set(totalDeleted);
            }
        } catch (final Exception e) {
            log.error("Unable to delete applications in database", e);
            MetricsUtils.addFailureTagsWithException(tags, e);
        } finally {
            this.registry
                .timer(APPLICATION_DELETION_TIMER, tags)
                .record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }
}
