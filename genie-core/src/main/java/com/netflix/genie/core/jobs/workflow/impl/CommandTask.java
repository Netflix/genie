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
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.jobs.workflow.Context;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.Writer;
import java.nio.file.Path;

/**
 * Implementation of the workflow task for processing command information.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class CommandTask extends GenieBaseTask implements WorkflowTask {

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Context context
    ) throws GenieException {
        log.info("Execution Command Task in the workflow.");

        final JobExecutionEnvironment jobExecEnv =
            (JobExecutionEnvironment) context.getAttribute(JOB_EXECUTION_ENV_KEY);

        if (jobExecEnv == null) {
            throw new GenieServerException("Cannot run application task as jobExecutionEnvironment is null");
        }

        final String jobLauncherScriptPath = jobExecEnv.getJobWorkingDir() + "/" + GENIE_JOB_LAUNCHER_SCRIPT;
        final Writer writer = getWriter(jobLauncherScriptPath);

        createDirectory(jobExecEnv.getJobWorkingDir() + "/command/" + jobExecEnv.getCommand().getId());
        final String commandSetupFile = jobExecEnv.getCommand().getSetupFile();

        if (commandSetupFile != null && StringUtils.isNotBlank(commandSetupFile)) {
            final Path setupFilePath = new File(commandSetupFile).toPath();
            final String setupFileLocalPath = jobExecEnv.getJobWorkingDir()
                + "/applications/"
                + jobExecEnv.getCommand().getId()
                + "/"
                + setupFilePath.getFileName();
            appendToWriter(writer, "source " + setupFileLocalPath + ";");

        }
        closeWriter(writer);
    }
}
