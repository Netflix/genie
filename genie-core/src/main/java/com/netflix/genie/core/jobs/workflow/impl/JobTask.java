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

import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.io.Writer;
import java.util.Map;

/**
 * Implementation of the workflow task for processing job information for genie mode.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class JobTask extends GenieBaseTask implements WorkflowTask {

    private static final String JOB_PATH_VAR = "job";
    private static final String  STDERR_LOG_PATH = "./job/stderr";
    private static final String STDOUT_LOG_PATH = "./job/stdout";
    private static final String GENIE_DONE_FILE = "./genie/genie.done";

    private GenieFileTransferService fts;

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Map<String, Object> context
    ) throws GenieException {
        log.info("Execution Job Task in the workflow.");

        final JobExecutionEnvironment jobExecEnv =
            (JobExecutionEnvironment) context.get(JOB_EXECUTION_ENV_KEY);

        if (jobExecEnv == null) {
            throw new GenieServerException("Cannot run application task as jobExecutionEnvironment is null");
        }

        final String jobLauncherScriptPath = jobExecEnv.getJobWorkingDir() + "/" + GENIE_JOB_LAUNCHER_SCRIPT;
        final Writer writer = getWriter(jobLauncherScriptPath);

        this.fts = (GenieFileTransferService) context.get(FILE_TRANSFER_SERVICE_KEY);

        //TODO copy down dependencies
        final String jobSetupFile = jobExecEnv.getJobRequest().getSetupFile();

        if (jobSetupFile != null && StringUtils.isNotBlank(jobSetupFile)) {
            final String localPath = fetchFile(
                jobExecEnv.getJobWorkingDir(),
                jobExecEnv.getCommand().getId(),
                jobSetupFile,
                SETUP_FILE_PATH_PREFIX
            );

            fts.getFile(jobSetupFile, localPath);
            appendToWriter(writer, "source " + localPath + ";");
        }

        // Iterate over and get all dependencies
        for (final String dependencyFile: jobExecEnv.getJobRequest().getFileDependencies()) {
            fetchFile(
                jobExecEnv.getJobWorkingDir(),
                jobExecEnv.getJobRequest().getId(),
                dependencyFile,
                DEPENDENCY_FILE_PATH_PREFIX
            );
        }

        appendToWriter(
            writer,
            jobExecEnv.getCommand().getExecutable()
                + " "
                + jobExecEnv.getJobRequest().getCommandArgs()
                + " > "
                + STDOUT_LOG_PATH
                + " 2> "
                + STDERR_LOG_PATH
        );
        // capture exit code and write to genie.done file
        appendToWriter(writer, "printf '{\"exitCode\": \"%s\"}\\n' \"$?\" > " + GENIE_DONE_FILE);
        closeWriter(writer);
    }

    /**
     * Helper Function to fetch file to local dir.
     *
     * @param dir The directory where to copy the file
     * @param id The id to be appended to the destination path
     * @param filePath Source file path
     * @param fileType Type of file like setup, config or dependency
     * @return Local file path constructed where the file is copied to
     *
     * @throws GenieException If there is any problem
     */
    private String fetchFile(
        final String dir,
        final String id,
        final String filePath,
        final String fileType
    ) throws GenieException {
        if (filePath != null && StringUtils.isNotBlank(filePath)) {
            final String fileName = getFileNameFromPath(filePath);
            final String localPath = new StringBuilder()
                .append(dir)
                .append(FILE_PATH_DELIMITER)
                .append(JOB_PATH_VAR)
                .append(FILE_PATH_DELIMITER)
                .append(id)
                .append(FILE_PATH_DELIMITER)
                .append(fileType)
                .append(FILE_PATH_DELIMITER)
                .append(fileName)
                .toString();

            this.fts.getFile(filePath, localPath);
            return localPath;
        } else {
            throw new GenieBadRequestException("Invalid file path");
        }
    }
}
