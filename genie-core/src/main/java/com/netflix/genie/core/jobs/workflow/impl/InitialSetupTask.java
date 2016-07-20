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
package com.netflix.genie.core.jobs.workflow.impl;

import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the workflow task for handling Applications that a job needs.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class InitialSetupTask extends GenieBaseTask {

    private Timer timer;

    /**
     * Constructor.
     *
     * @param registry The metrics registry to use
     */
    public InitialSetupTask(@NotNull final Registry registry) {
        this.timer = registry.timer("genie.jobs.tasks.initialSetupTask.timer");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(@NotNull final Map<String, Object> context) throws GenieException, IOException {
        final long start = System.nanoTime();
        try {
            log.debug("Executing Initial setup Task in the workflow.");

            final String lineSeparator = System.lineSeparator();
            final JobExecutionEnvironment jobExecEnv
                = (JobExecutionEnvironment) context.get(JobConstants.JOB_EXECUTION_ENV_KEY);
            final String jobWorkingDirectory = jobExecEnv.getJobWorkingDir().getCanonicalPath();
            final Writer writer = (Writer) context.get(JobConstants.WRITER_KEY);

            // create top level directory structure for the job

            // Genie Directory {basedir/genie}
            this.createDirectory(jobWorkingDirectory + JobConstants.FILE_PATH_DELIMITER + JobConstants.GENIE_PATH_VAR);

            // Genie Logs directory {basedir/genie/logs}
            this.createDirectory(jobWorkingDirectory
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.GENIE_PATH_VAR
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.LOGS_PATH_VAR);

            // Genie applications directory {basedir/genie/applications}
            this.createDirectory(jobWorkingDirectory
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.GENIE_PATH_VAR
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.APPLICATION_PATH_VAR);

            // Genie command directory {basedir/genie/command}
            this.createDirectory(jobWorkingDirectory
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.GENIE_PATH_VAR
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.COMMAND_PATH_VAR);

            // Genie cluster directory {basedir/genie/cluster}
            this.createDirectory(jobWorkingDirectory
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.GENIE_PATH_VAR
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.CLUSTER_PATH_VAR);

            // set the env variables in the launcher script

            // set environment variable for the job directory
            writer.write(JobConstants.EXPORT
                + JobConstants.GENIE_JOB_DIR_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + jobWorkingDirectory
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + lineSeparator);

            // Append new line
            writer.write(lineSeparator);

            // create environment variable for the application directory
            writer.write(JobConstants.EXPORT
                + JobConstants.GENIE_APPLICATION_DIR_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + "${"
                + JobConstants.GENIE_JOB_DIR_ENV_VAR
                + "}"
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.GENIE_PATH_VAR
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.APPLICATION_PATH_VAR
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + lineSeparator);

            // Append new line
            writer.write(lineSeparator);

            // create environment variables for the command
            final Command command = jobExecEnv.getCommand();

            writer.write(JobConstants.EXPORT
                + JobConstants.GENIE_COMMAND_DIR_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + "${"
                + JobConstants.GENIE_JOB_DIR_ENV_VAR
                + "}"
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.GENIE_PATH_VAR
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.COMMAND_PATH_VAR
                + JobConstants.FILE_PATH_DELIMITER
                + command.getId()
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + lineSeparator);

            // Append new line
            writer.write(lineSeparator);

            writer.write(
                JobConstants.EXPORT
                    + JobConstants.GENIE_COMMAND_ID_ENV_VAR
                    + JobConstants.EQUALS_SYMBOL
                    + JobConstants.DOUBLE_QUOTE_SYMBOL
                    + command.getId()
                    + JobConstants.DOUBLE_QUOTE_SYMBOL
                    + lineSeparator
            );

            // Append new line
            writer.write(System.lineSeparator());

            writer.write(
                JobConstants.EXPORT
                    + JobConstants.GENIE_COMMAND_NAME_ENV_VAR
                    + JobConstants.EQUALS_SYMBOL
                    + JobConstants.DOUBLE_QUOTE_SYMBOL
                    + command.getName()
                    + JobConstants.DOUBLE_QUOTE_SYMBOL
                    + lineSeparator
            );

            // Append new line
            writer.write(System.lineSeparator());

            // create environment variables for the cluster
            final Cluster cluster = jobExecEnv.getCluster();

            writer.write(JobConstants.EXPORT
                + JobConstants.GENIE_CLUSTER_DIR_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + "${"
                + JobConstants.GENIE_JOB_DIR_ENV_VAR
                + "}"
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.GENIE_PATH_VAR
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.CLUSTER_PATH_VAR
                + JobConstants.FILE_PATH_DELIMITER
                + cluster.getId()
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + System.lineSeparator());

            // Append new line
            writer.write(System.lineSeparator());

            writer.write(
                JobConstants.EXPORT
                    + JobConstants.GENIE_CLUSTER_ID_ENV_VAR
                    + JobConstants.EQUALS_SYMBOL
                    + JobConstants.DOUBLE_QUOTE_SYMBOL
                    + cluster.getId()
                    + JobConstants.DOUBLE_QUOTE_SYMBOL
                    + lineSeparator
            );

            // Append new line
            writer.write(System.lineSeparator());

            writer.write(
                JobConstants.EXPORT
                    + JobConstants.GENIE_CLUSTER_NAME_ENV_VAR
                    + JobConstants.EQUALS_SYMBOL
                    + JobConstants.DOUBLE_QUOTE_SYMBOL
                    + cluster.getName()
                    + JobConstants.DOUBLE_QUOTE_SYMBOL
                    + lineSeparator
            );

            // Append new line
            writer.write(System.lineSeparator());

            // create environment variable for the job id
            writer.write(JobConstants.EXPORT
                + JobConstants.GENIE_JOB_ID_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + jobExecEnv.getJobRequest().getId()
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + System.lineSeparator());

            // Append new line
            writer.write(System.lineSeparator());

            // create environment variable for the job name
            writer.write(JobConstants.EXPORT
                + JobConstants.GENIE_JOB_NAME_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + jobExecEnv.getJobRequest().getName()
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + System.lineSeparator());

            // Append new line
            writer.write(System.lineSeparator());
        } finally {
            final long finish = System.nanoTime();
            this.timer.record(finish - start, TimeUnit.NANOSECONDS);
        }
    }
}
