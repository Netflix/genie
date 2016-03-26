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
import com.netflix.genie.core.services.AttachmentService;
import com.netflix.genie.core.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
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
    ) throws GenieException {
        log.debug("Execution Job Task in the workflow.");
        super.executeTask(context);

        final String jobSetupFile = jobExecEnv.getJobRequest().getSetupFile();

        if (jobSetupFile != null && StringUtils.isNotBlank(jobSetupFile)) {
            final String localPath =
                this.jobWorkingDirectory
                    + JobConstants.FILE_PATH_DELIMITER
                    + jobSetupFile.substring(jobSetupFile.lastIndexOf(JobConstants.FILE_PATH_DELIMITER) + 1);

            this.fts.getFile(jobSetupFile, localPath);

            Utils.appendToWriter(writer, "# Sourcing setup file specified in job request");
            Utils.appendToWriter(writer,
                JobConstants.SOURCE
                    + localPath.replace(this.jobWorkingDirectory, "${" + JobConstants.GENIE_JOB_DIR_ENV_VAR + "}")
                    + JobConstants.SEMICOLON_SYMBOL);

            // Append new line
            Utils.appendToWriter(writer, " ");
        }

        // Iterate over and get all dependencies
        for (final String dependencyFile: jobExecEnv.getJobRequest().getDependencies()) {
            final String localPath = this.jobWorkingDirectory
                + JobConstants.FILE_PATH_DELIMITER
                + dependencyFile.substring(dependencyFile.lastIndexOf(JobConstants.FILE_PATH_DELIMITER) + 1);

            this.fts.getFile(dependencyFile, localPath);
        }

        // Copy down the attachments if any to the current working directory
        this.attachmentService.copy(
            jobExecEnv.getJobRequest().getId(),
            jobExecEnv.getJobWorkingDir());

        // Append new line
        Utils.appendToWriter(writer, " ");

        Utils.appendToWriter(writer, "# Kick off the command in background mode and wait for it using its pid");

        Utils.appendToWriter(
            writer,
            jobExecEnv.getCommand().getExecutable()
                + JobConstants.WHITE_SPACE
                + jobExecEnv.getJobRequest().getCommandArgs()
                + JobConstants.STDOUT_REDIRECT
                + JobConstants.STDOUT_LOG_FILE_NAME
                + JobConstants.STDERR_REDIRECT
                + JobConstants.STDERR_LOG_FILE_NAME
                + " &"
        );

        // Wait for the above process started in background mode. Wait lets us get interrupted by kill signals.
        Utils.appendToWriter(writer, "wait $!");

        // Append new line
        Utils.appendToWriter(writer, " ");

        // capture exit code and write to genie.done file
        Utils.appendToWriter(writer, "# Write the return code from the command in the done file.");
        Utils.appendToWriter(writer, JobConstants.GENIE_DONE_FILE_CONTENT_PREFIX + JobConstants.GENIE_DONE_FILE_NAME);
    }
}
