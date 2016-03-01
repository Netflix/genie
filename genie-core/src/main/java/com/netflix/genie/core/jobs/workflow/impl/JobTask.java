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
import com.netflix.genie.common.util.Constants;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Map<String, Object> context
    ) throws GenieException {
        log.info("Execution Job Task in the workflow.");
        super.executeTask(context);

        // Open a writer to jobLauncher script
        final Writer writer = getWriter(this.jobLauncherScriptPath);

        final String jobSetupFile = jobExecEnv.getJobRequest().getSetupFile();

        if (jobSetupFile != null && StringUtils.isNotBlank(jobSetupFile)) {
            final String localPath =
                this.jobExecEnv.getJobWorkingDir()
                    + Constants.FILE_PATH_DELIMITER
                    + super.getFileNameFromPath(jobSetupFile);

            this.fts.getFile(jobSetupFile, localPath);
            appendToWriter(writer, "source " + localPath + ";");
        }

        // Iterate over and get all dependencies
        for (final String dependencyFile: jobExecEnv.getJobRequest().getFileDependencies()) {
            final String localPath = this.jobExecEnv.getJobWorkingDir()
                + Constants.FILE_PATH_DELIMITER
                + super.getFileNameFromPath(dependencyFile);
            this.fts.getFile(dependencyFile, localPath);
        }

        appendToWriter(
            writer,
            jobExecEnv.getCommand().getExecutable()
                + " "
                + jobExecEnv.getJobRequest().getCommandArgs()
                + " > "
                + Constants.STDOUT_LOG_FILE_NAME
                + " 2> "
                + Constants.STDERR_LOG_FILE_NAME
        );

        // capture exit code and write to genie.done file
        appendToWriter(writer, "printf '{\"exitCode\": \"%s\"}\\n' \"$?\" > " + Constants.GENIE_DONE_FILE_NAME);

        // close the writer
        closeWriter(writer);
    }
}
