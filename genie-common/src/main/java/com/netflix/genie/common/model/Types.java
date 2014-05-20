/*
 *
 *  Copyright 2013 Netflix, Inc.
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

package com.netflix.genie.common.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Common enums used by other model objects.
 *
 * @author skrishnan
 */
public class Types {

    /**
     * Job types supported by the Execution Service.
     *
     * @author skrishnan
     */
    public enum JobType {
        /**
         * Represents a Yarn job.
         */
        YARN;

        /**
         * Parse job type.
         *
         * @param value
         *            string to parse/convert
         * @return HADOOP, HIVE, PIG if there is a match, else null
         */
        public static JobType parse(String value) {
            if (value == null) {
                return null;
            } else if (value.compareToIgnoreCase("YARN") == 0) {
                return YARN;
            } else {
                return null;
            }
        }
    }

    /**
     * Acceptable status codes for jobs.
     *
     * @author skrishnan
     */
    public enum JobStatus {
        /**
         * Job has been initialized, but not running yet.
         */
        INIT,
        /**
         * Job is now running.
         */
        RUNNING,
        /**
         * Job has finished executing, and is successful.
         */
        SUCCEEDED,
        /**
         * Job has been killed.
         */
        KILLED,
        /**
         * Job failed.
         */
        FAILED;

        /**
         * Parse job status.
         *
         * @param value
         *            string to parse/convert
         * @return INIT, RUNNING, SUCCEEDED, KILLED, FAILED if match, else null
         */
        public static JobStatus parse(String value) {
            if (value == null) {
                return null;
            } else if (value.compareToIgnoreCase("INIT") == 0) {
                return INIT;
            } else if (value.compareToIgnoreCase("RUNNING") == 0) {
                return RUNNING;
            } else if (value.compareToIgnoreCase("SUCCEEDED") == 0) {
                return SUCCEEDED;
            } else if (value.compareToIgnoreCase("KILLED") == 0) {
                return KILLED;
            } else if (value.compareToIgnoreCase("FAILED") == 0) {
                return FAILED;
            } else {
                return null;
            }
        }
    }

    /**
     * The status type for a cluster.
     *
     * @author skrishnan
     */
    public enum ClusterStatus {
        /**
         * Cluster is UP, and accepting jobs.
         */
        UP,
        /**
         * Cluster may be running, but not accepting job submissions.
         */
        OUT_OF_SERVICE,
        /**
         * Cluster is no-longer running, and is terminated.
         */
        TERMINATED;

        /**
         * Parse cluster status.
         *
         * @param value
         *            string to parse/convert into cluster status
         * @return UP, OUT_OF_SERVICE, TERMINATED if match, else return null
         */
        public static ClusterStatus parse(String value) {
            if (value == null) {
                return null;
            } else if (value.compareToIgnoreCase("UP") == 0) {
                return UP;
            } else if (value.compareToIgnoreCase("OUT_OF_SERVICE") == 0) {
                return OUT_OF_SERVICE;
            } else if (value.compareToIgnoreCase("TERMINATED") == 0) {
                return TERMINATED;
            } else {
                return null;
            }
        }
    }

    /**
     * The status type for  command and application configs.
     *
     * @author skrishnan
     */
    public enum ConfigStatus {
        /**
         * Configuration is active, and in-use.
         */
        ACTIVE,
        /**
         * Configuration is deprecated, and will be made inactive in the future.
         */
        DEPRECATED,
        /**
         * Cluster is inactive, and not in-use.
         */
        INACTIVE;

        /**
         * Parse config status.
         *
         * @param value
         *            string to parse/convert into config status
         * @return ACTIVE, DEPRECATED, INACTIVE if match, else return null
         */
        public static ConfigStatus parse(String value) {
            if (value == null) {
                return null;
            } else if (value.compareToIgnoreCase("ACTIVE") == 0) {
                return ACTIVE;
            } else if (value.compareToIgnoreCase("DEPRECATED") == 0) {
                return DEPRECATED;
            } else if (value.compareToIgnoreCase("INACTIVE") == 0) {
                return INACTIVE;
            } else {
                return null;
            }
        }
    }

    /**
     * Possible status values for jobs, and associated messages.
     *
     * @author skrishnan
     */
    public enum SubprocessStatus {
        /**
         * Job was run, but interrupted.
         */
        JOB_INTERRUPTED(-1),
        /**
         * Job ran successfully.
         */
        SUCCESS(0),
        /**
         * Job failed to create job results directory.
         */
        MKDIR_RESULTS_FAILURE(201),
        /**
         * Job failed to create conf directory.
         */
        MKDIR_CONF_FAILURE(202),
        /**
         * Job failed while copying user dependencies from S3.
         */
        USER_S3_COPY_FAILURE(203),
        /**
         * Job failed to copy Hadoop conf files from S3.
         */
        HADOOP_CONF_S3_COPY_FAILURE(204),
        /**
         * Job failed to copy Hive conf files from S3.
         */
        HIVE_CONF_S3_COPY_FAILURE(205),
        /**
         * Job failure during run.
         */
        JOB_COMMAND_FAILURE(206),
        /**
         * Job failed to create Hive log dir.
         */
        MKDIR_HIVE_CLIENT_LOG_FAILURE(207),
        /**
         * Job failed to create Pig log dir.
         */
        MKDIR_PIG_CLIENT_LOG_FAILURE(208),
        /**
         * Job failed to copy pig conf files from S3.
         */
        PIG_CONF_S3_COPY_FAILURE(209),
        /**
         * Job succeeded, but failed to archive logs to S3.
         */
        S3_ARCHIVE_FAILURE(210),
        /**
         * Job was killed.
         */
        JOB_KILLED(211),
        /**
         * Job was a zombie, hence marked as failed.
         */
        ZOMBIE_JOB(212);

        // A map of all status-es and their corresponding messages
        private static final Map<Integer, String> STATUS_MAP = new HashMap<Integer, String>();
        static {
            // assign error codes
            STATUS_MAP.put(JOB_INTERRUPTED.code(), "Job execution interrupted");
            STATUS_MAP.put(SUCCESS.code(), "Success");
            STATUS_MAP.put(MKDIR_RESULTS_FAILURE.code(),
                    "Failed to create job results dir");
            STATUS_MAP.put(MKDIR_CONF_FAILURE.code(),
                    "Failed to create job conf dir");
            STATUS_MAP.put(USER_S3_COPY_FAILURE.code(),
                    "Failed copying user files from S3");
            STATUS_MAP.put(HADOOP_CONF_S3_COPY_FAILURE.code(),
                    "Failed copying hadoop conf files from S3");
            STATUS_MAP.put(HIVE_CONF_S3_COPY_FAILURE.code(),
                    "Failed copying hive conf files from S3");
            STATUS_MAP.put(PIG_CONF_S3_COPY_FAILURE.code(),
                    "Failed copying pig conf files from S3");
            STATUS_MAP.put(JOB_COMMAND_FAILURE.code(),
                    "Job failed with non-zero exit code");
            STATUS_MAP.put(MKDIR_HIVE_CLIENT_LOG_FAILURE.code(),
                    "Failed to create dir for hive client log");
            STATUS_MAP.put(MKDIR_PIG_CLIENT_LOG_FAILURE.code(),
                    "Failed to create dir for pig client log");
            STATUS_MAP.put(S3_ARCHIVE_FAILURE.code(),
                    "Failed to archive job logs to S3");
            STATUS_MAP.put(JOB_KILLED.code(), "Job killed after it exceeded system limits");
            STATUS_MAP.put(ZOMBIE_JOB.code(), "Job has been marked as a zombie");
        }

        private final int exitCode;

        /**
         * Constructor.
         *
         * @param exitCode the exit code to initialize with.
         */
        SubprocessStatus(int exitCode) {
            this.exitCode = exitCode;
        }

        /**
         * Return the status code for the job.
         *
         * @return status for the job
         */
        public int code() {
            return exitCode;
        }

        /**
         * Return the message associated with each status code.
         *
         * @param code
         *            status code to get the message for
         * @return message for status code
         */
        public static String message(int code) {
            return STATUS_MAP.get(code);
        }
    }
}
