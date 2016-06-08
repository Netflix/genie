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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.services.AttachmentService;
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;

/**
 * Implementation of the workflow task for processing job information for genie mode.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class JobTask extends GenieBaseTask {

    private final AttachmentService attachmentService;

    /**
     * Constructor.
     *
     * @param attachmentService An implementation of the Attachment Service
     * @throws GenieException If there is any problem.
     */
    public JobTask(
        final AttachmentService attachmentService
        ) throws GenieException {
        this.attachmentService = attachmentService;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Map<String, Object> context
    ) throws GenieException, IOException {
        log.debug("Execution Job Task in the workflow.");

        final GenieFileTransferService fts =
            (GenieFileTransferService) context.get(JobConstants.FILE_TRANSFER_SERVICE_KEY);
        final JobExecutionEnvironment jobExecEnv =
            (JobExecutionEnvironment) context.get(JobConstants.JOB_EXECUTION_ENV_KEY);
        final String jobWorkingDirectory = jobExecEnv.getJobWorkingDir().getCanonicalPath();
        final Writer writer = (Writer) context.get(JobConstants.WRITER_KEY);

        final String jobSetupFile = jobExecEnv.getJobRequest().getSetupFile();

        if (jobSetupFile != null && StringUtils.isNotBlank(jobSetupFile)) {
            final String localPath =
                jobWorkingDirectory
                    + JobConstants.FILE_PATH_DELIMITER
                    + jobSetupFile.substring(jobSetupFile.lastIndexOf(JobConstants.FILE_PATH_DELIMITER) + 1);

            fts.getFile(jobSetupFile, localPath);

            writer.write("# Sourcing setup file specified in job request" + System.lineSeparator());
            writer.write(
                JobConstants.SOURCE
                    + localPath.replace(jobWorkingDirectory, "${" + JobConstants.GENIE_JOB_DIR_ENV_VAR + "}")
                    + System.lineSeparator());

            // Append new line
            writer.write(System.lineSeparator());
        }

        // Iterate over and get all dependencies
        for (final String dependencyFile: jobExecEnv.getJobRequest().getDependencies()) {
            final String localPath = jobWorkingDirectory
                + JobConstants.FILE_PATH_DELIMITER
                + dependencyFile.substring(dependencyFile.lastIndexOf(JobConstants.FILE_PATH_DELIMITER) + 1);

            fts.getFile(dependencyFile, localPath);
        }

        // Copy down the attachments if any to the current working directory
        this.attachmentService.copy(
            jobExecEnv.getJobRequest().getId(),
            jobExecEnv.getJobWorkingDir());

        // Print out the current Envrionment to a env file before running the command.
        writer.write("# Dump the environment to a env.log file" + System.lineSeparator());
        writer.write("env > " + "${"
            + JobConstants.GENIE_JOB_DIR_ENV_VAR + "}"
            + JobConstants.GENIE_ENV_PATH
            + System.lineSeparator());

        // Append new line
        writer.write(System.lineSeparator());

        writer.write("# Kick off the command in background mode and wait for it using its pid"
            + System.lineSeparator());

        writer.write(
            jobExecEnv.getCommand().getExecutable()
                + JobConstants.WHITE_SPACE
                + jobExecEnv.getJobRequest().getCommandArgs()
                + JobConstants.STDOUT_REDIRECT
                + JobConstants.STDOUT_LOG_FILE_NAME
                + JobConstants.STDERR_REDIRECT
                + JobConstants.STDERR_LOG_FILE_NAME
                + " &"
                + System.lineSeparator()
        );

        // Wait for the above process started in background mode. Wait lets us get interrupted by kill signals.
        writer.write("wait $!" + System.lineSeparator());

        // Append new line
        writer.write(System.lineSeparator());

        // capture exit code and write to genie.done file
        writer.write("# Write the return code from the command in the done file." + System.lineSeparator());
        writer.write(JobConstants.GENIE_DONE_FILE_CONTENT_PREFIX
            + JobConstants.GENIE_DONE_FILE_NAME
            + System.lineSeparator());
    }
}
