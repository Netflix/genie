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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.io.Writer;
import java.util.Map;

/**
 * Implementation of the workflow task for processing command information.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class CommandTask extends GenieBaseTask {

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Map<String, Object> context
    ) throws GenieException {
        log.info("Executing Command Task in the workflow.");

        super.executeTask(context);

        // Open a writer to jobLauncher script
        final Writer writer = getWriter(jobLauncherScriptPath);

        // Create the directory for this command under command dir in the cwd
        createDirectory(jobExecEnv.getJobWorkingDir() + "/command/" + jobExecEnv.getCommand().getId());

        // Get the setup file if specified and add it as source command in launcher script
        final String commandSetupFile = jobExecEnv.getCommand().getSetupFile();
        if (commandSetupFile != null && StringUtils.isNotBlank(commandSetupFile)) {
            final String localPath = super.buildLocalFilePath(
                jobExecEnv.getJobWorkingDir(),
                jobExecEnv.getCommand().getId(),
                commandSetupFile,
                Constants.FileType.SETUP,
                Constants.EntityType.COMMAND
            );

            this.fts.getFile(commandSetupFile, localPath);
            appendToWriter(writer, "source " + localPath + ";");
        }

        // Iterate over and get all configuration files
        for (final String configFile: jobExecEnv.getCommand().getConfigs()) {
            final String localPath = super.buildLocalFilePath(
                jobExecEnv.getJobWorkingDir(),
                jobExecEnv.getCommand().getId(),
                configFile,
                Constants.FileType.CONFIG,
                Constants.EntityType.COMMAND
            );
            this.fts.getFile(configFile, localPath);
        }

        // close the writer
        closeWriter(writer);
    }
}
