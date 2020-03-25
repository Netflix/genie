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
package com.netflix.genie.web.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * Properties controlling the behavior of the database cleanup leadership task.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConfigurationProperties(prefix = DatabaseCleanupProperties.PROPERTY_PREFIX)
@Getter
@Setter
@Validated
public class DatabaseCleanupProperties {

    /**
     * The property prefix for Database Cleanup related tasks.
     */
    public static final String PROPERTY_PREFIX = "genie.tasks.database-cleanup";

    /**
     * The property key for whether this feature is enabled or not.
     */
    public static final String ENABLED_PROPERTY = PROPERTY_PREFIX + ".enabled";

    /**
     * The cron expression for when the cleanup task should occur.
     */
    public static final String EXPRESSION_PROPERTY = PROPERTY_PREFIX + ".expression";

    /**
     * The number of days to retain jobs in the database.
     */
    public static final String JOB_RETENTION_PROPERTY = PROPERTY_PREFIX + ".retention";

    /**
     * The number of job records to delete from the database in a single transaction.
     * Genie will loop and perform multiple transactions until all jobs older than the retention time are deleted.
     */
    public static final String MAX_DELETED_PER_TRANSACTION_PROPERTY = PROPERTY_PREFIX + ".maxDeletedPerTransaction";

    /**
     * The page size used within each cleanup transaction to iterate through the job records.
     */
    public static final String PAGE_SIZE_PROPERTY = PROPERTY_PREFIX + ".pageSize";

    /**
     * The property key for whether this feature is enabled or not.
     */
    private boolean enabled;

    /**
     * The cron expression for when the cleanup task should occur.
     */
    @NotBlank
    private String expression = "0 0 0 * * *";

    /**
     * Properties related to cleaning up cluster records from the database.
     */
    @NotNull
    private ClusterDatabaseCleanupProperties clusterCleanup = new ClusterDatabaseCleanupProperties();

    /**
     * Properties related to cleaning up file records from the database.
     */
    @NotNull
    private FileDatabaseCleanupProperties fileCleanup = new FileDatabaseCleanupProperties();

    /**
     * Properties related to cleaning up job records from the database.
     */
    @NotNull
    private JobDatabaseCleanupProperties jobCleanup = new JobDatabaseCleanupProperties();

    /**
     * Properties related to cleaning up tag records from the database.
     */
    @NotNull
    private TagDatabaseCleanupProperties tagCleanup = new TagDatabaseCleanupProperties();


    /**
     * The number of days to retain jobs in the database.
     */
    private int retention = 90;

    /**
     * The number of job records to delete from the database in a single transaction.
     * Genie will loop and perform multiple transactions until all jobs older than the retention time are deleted.
     */
    private int maxDeletedPerTransaction = 1_000;

    /**
     * Properties related to cleaning up cluster records from the database.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Getter
    @Setter
    public static class ClusterDatabaseCleanupProperties {

        /**
         * The prefix for all properties related to cleaning up cluster records from the database.
         */
        public static final String CLUSTER_CLEANUP_PROPERTY_PREFIX
            = DatabaseCleanupProperties.PROPERTY_PREFIX + ".cluster-cleanup";

        /**
         * Skip the Clusters table when performing database cleanup.
         */
        public static final String SKIP_PROPERTY = CLUSTER_CLEANUP_PROPERTY_PREFIX + ".skip";

        /**
         * Skip the Clusters table when performing database cleanup.
         */
        private boolean skip;
    }

    /**
     * Properties related to cleaning up file records from the database.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Getter
    @Setter
    public static class FileDatabaseCleanupProperties {

        /**
         * The prefix for all properties related to cleaning up file records from the database.
         */
        public static final String FILE_CLEANUP_PROPERTY_PREFIX
            = DatabaseCleanupProperties.PROPERTY_PREFIX + ".file-cleanup";

        /**
         * Skip the Files table when performing database cleanup.
         */
        public static final String SKIP_PROPERTY = FILE_CLEANUP_PROPERTY_PREFIX + ".skip";

        /**
         * Skip the Files table when performing database cleanup.
         */
        private boolean skip;
    }

    /**
     * Properties related to cleaning up job records from the database.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Getter
    @Setter
    public static class JobDatabaseCleanupProperties {

        /**
         * The prefix for all properties related to cleaning up job records from the database.
         */
        public static final String JOB_CLEANUP_PROPERTY_PREFIX
            = DatabaseCleanupProperties.PROPERTY_PREFIX + ".job-cleanup";

        /**
         * Skip the Jobs table when performing database cleanup.
         */
        public static final String SKIP_PROPERTY = JOB_CLEANUP_PROPERTY_PREFIX + ".skip";

        /**
         * The number of days to retain jobs in the database.
         */
        public static final String JOB_RETENTION_PROPERTY = JOB_CLEANUP_PROPERTY_PREFIX + ".retention";

        /**
         * The number of job records to delete from the database in a single transaction.
         * Genie will loop and perform multiple transactions until all jobs older than the retention time are deleted.
         */
        public static final String MAX_DELETED_PER_TRANSACTION_PROPERTY
            = JOB_CLEANUP_PROPERTY_PREFIX + ".maxDeletedPerTransaction";

        /**
         * The page size used within each cleanup transaction to iterate through the job records.
         */
        public static final String PAGE_SIZE_PROPERTY = JOB_CLEANUP_PROPERTY_PREFIX + ".pageSize";

        /**
         * Skip the Jobs table when performing database cleanup.
         */
        private boolean skip;

        /**
         * The number of days to retain jobs in the database.
         */
        private int retention = 90;

        /**
         * The number of job records to delete from the database in a single transaction.
         * Genie will loop and perform multiple transactions until all jobs older than the retention time are deleted.
         */
        private int maxDeletedPerTransaction = 1_000;

        /**
         * The page size used within each cleanup transaction to iterate through the job records.
         */
        private int pageSize = 1_000;
    }

    /**
     * Properties related to cleaning up tag records from the database.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Getter
    @Setter
    public static class TagDatabaseCleanupProperties {

        /**
         * The prefix for all properties related to cleaning up tag records from the database.
         */
        public static final String TAG_CLEANUP_PROPERTY_PREFIX
            = DatabaseCleanupProperties.PROPERTY_PREFIX + ".tag-cleanup";

        /**
         * Skip the Tags table when performing database cleanup.
         */
        public static final String SKIP_PROPERTY = TAG_CLEANUP_PROPERTY_PREFIX + ".skip";

        /**
         * Skip the Tags table when performing database cleanup.
         */
        private boolean skip;
    }

    /**
     * Skip the Files table when performing database cleanup.
     */
    private boolean skipFilesCleanup;
}
