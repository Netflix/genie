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
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.io.Writer;
import java.util.Map;

/**
 * Implementation of the workflow task for handling Applications that a job needs.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class InitialSetupTask extends GenieBaseTask implements WorkflowTask {

    // Directory paths env variables
    private static final String GENIE_WORKING_DIR_ENV_VAR = "GENIE_WORKING_DIR";
    private static final String GENIE_JOB_DIR_ENV_VAR = "GENIE_JOB_DIR";
    private static final String GENIE_CLUSTER_DIR_ENV_VAR = "GENIE_CLUSTER_DIR";
    private static final String GENIE_COMMAND_DIR_ENV_VAR = "GENIE_COMMAND_DIR";
    private static final String GENIE_APPLICATION_DIR_ENV_VAR = "GENIE_APPLICATION_DIR";
    private static final String EXPORT = "export ";
    private static final String EQUALS = "=";

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Map<String, Object> context
    ) throws GenieException {
        log.info("Executing Initial setup Task in the workflow.");

        final JobExecutionEnvironment jobExecEnv =
            (JobExecutionEnvironment) context.get(JOB_EXECUTION_ENV_KEY);

        if (jobExecEnv == null) {
            throw new GenieServerException("Cannot run setup task as jobExecutionEnvironment is null");
        }

        // create top level directory structure for the job
        createDirectory(jobExecEnv.getJobWorkingDir());
        createDirectory(jobExecEnv.getJobWorkingDir() + FILE_PATH_DELIMITER + GENIE_PATH_VAR);
        createDirectory(jobExecEnv.getJobWorkingDir() + FILE_PATH_DELIMITER + GENIE_PATH_VAR
            + FILE_PATH_DELIMITER + LOGS_PATH_VAR);
        createDirectory(jobExecEnv.getJobWorkingDir() + FILE_PATH_DELIMITER + APPLICATION_PATH_VAR);
        createDirectory(jobExecEnv.getJobWorkingDir() + FILE_PATH_DELIMITER + COMMAND_PATH_VAR);
        createDirectory(jobExecEnv.getJobWorkingDir() + FILE_PATH_DELIMITER + CLUSTER_PATH_VAR);
        createDirectory(jobExecEnv.getJobWorkingDir() + FILE_PATH_DELIMITER + JOB_PATH_VAR);

        // set the env variables in the launcher script
        final String jobLauncherScriptPath = jobExecEnv.getJobWorkingDir() + "/" + GENIE_JOB_LAUNCHER_SCRIPT;
        final Writer writer = getWriter(jobLauncherScriptPath);

        appendToWriter(writer, EXPORT + GENIE_WORKING_DIR_ENV_VAR + EQUALS
            + jobExecEnv.getJobWorkingDir());
        appendToWriter(writer, EXPORT + GENIE_APPLICATION_DIR_ENV_VAR + EQUALS
            + jobExecEnv.getJobWorkingDir() + FILE_PATH_DELIMITER + APPLICATION_PATH_VAR);
        appendToWriter(writer, EXPORT + GENIE_COMMAND_DIR_ENV_VAR + EQUALS
            + jobExecEnv.getJobWorkingDir() + FILE_PATH_DELIMITER + COMMAND_PATH_VAR);
        appendToWriter(writer, EXPORT + GENIE_CLUSTER_DIR_ENV_VAR + EQUALS
            + jobExecEnv.getJobWorkingDir() + FILE_PATH_DELIMITER + CLUSTER_PATH_VAR);
        appendToWriter(writer, EXPORT + GENIE_JOB_DIR_ENV_VAR + EQUALS
            + jobExecEnv.getJobWorkingDir() + FILE_PATH_DELIMITER + JOB_PATH_VAR);

        closeWriter(writer);
    }
}
