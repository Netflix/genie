/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.common.internal.util;

import com.netflix.genie.common.exceptions.GeniePreconditionException;

/**
 * Possible status values for jobs, and associated messages.
 *
 * @author skrishnan
 * @author tgianos
 */
public enum ProcessStatus {

    /**
     * Job was run, but interrupted.
     */
    JOB_INTERRUPTED(-1, "Job execution interrupted."),
    /**
     * Job ran successfully.
     */
    SUCCESS(0, "Success."),
    /**
     * Job failed to create job results directory.
     */
    MKDIR_JAR_FAILURE(201, "Failed to create job jar dir."),
    /**
     * Job failed to create conf directory.
     */
    MKDIR_CONF_FAILURE(202, "Failed to create job conf dir."),
    /**
     * Job failed while copying user dependencies from S3.
     */
    HADOOP_LOCAL_CONF_COPY_FAILURE(203,
        "Failed copying Hadoop files from local conf dir to current job conf dir."),
    /**
     * Job failed to copy Hadoop conf files from S3.
     */
    UPDATE_CORE_SITE_XML_FAILURE(204,
        "Failed updating core-site.xml to add certain parameters."),
    /**
     * Job failed to copy Hive conf files from S3.
     */
    ENV_VARIABLES_SOURCE_AND_SETUP_FAILURE(205, "Failed while sourcing resource envProperty files."),
    /**
     * Job failure during run.
     */
    CLUSTER_CONF_FILES_COPY_FAILURE(206, "Failed copying cluster conf files from S3"),
    /**
     * Job failed to create Hive log dir.
     */
    COMMAND_CONF_FILES_COPY_FAILURE(207, "Failed copying command conf files from S3"),
    /**
     * Job failed to create Pig log dir.
     */
    APPLICATION_CONF_FILES_COPY_FAILURE(208, "Failed copying application conf files from S3"),
    /**
     * Job failed to copy pig conf files from S3.
     */
    APPLICATION_JAR_FILES_COPY_FAILURE(209, "Failed copying application jar files from S3"),
    /**
     * Job succeeded, but failed to archive logs to S3.
     */
    JOB_DEPENDENCIES_COPY_FAILURE(210, "Job failed copying dependent files."),
    /**
     * Job was killed.
     */
    JOB_KILLED(211, "Job killed after it exceeded system limits"),
    /**
     * Job was a zombie, hence marked as failed.
     */
    ZOMBIE_JOB(212, "Job has been marked as a zombie"),
    /**
     * Command Run Failure.
     */
    COMMAND_RUN_FAILURE(213, "Command failed with non-zero exit code.");

    private final int exitCode;
    private final String message;

    /**
     * Constructor.
     *
     * @param exitCode the exit getExitCode to initialize with.
     * @param message  The getMessage associated with this exit getExitCode.
     */
    ProcessStatus(final int exitCode, final String message) {
        this.exitCode = exitCode;
        this.message = message;
    }

    /**
     * Return the status getExitCode for the job.
     *
     * @return status for the job
     */
    public int getExitCode() {
        return this.exitCode;
    }

    /**
     * Return the getMessage associated with each status getExitCode.
     *
     * @return getMessage for status getExitCode
     */
    public String getMessage() {
        return this.message;
    }

    /**
     * Try to create a ProcessStatus from an exit code,
     * if one doesn't exist an exception will be thrown.
     *
     * @param exitCode The exit code to attempt to parse.
     * @return The corresponding ProcessStatus if one exists.
     * @throws GeniePreconditionException If the code isn't available.
     */
    public static ProcessStatus parse(final int exitCode) throws GeniePreconditionException {
        for (final ProcessStatus status : ProcessStatus.values()) {
            if (exitCode == status.exitCode) {
                return status;
            }
        }
        // If we got to here no status exists for the error code. Throw exception.
        throw new GeniePreconditionException(
            "No ProcessStatus found for code " + exitCode);
    }
}
