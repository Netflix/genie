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
public class InitialSetupTask extends GenieBaseTask {

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Map<String, Object> context
    ) throws GenieException {
        log.info("Executing Initial setup Task in the workflow.");

        super.executeTask(context);

        // create top level directory structure for the job
        createDirectory(this.baseWorkingDirPath);

        createDirectory(this.baseWorkingDirPath + Constants.GENIE_PATH_VAR);
        createDirectory(this.baseWorkingDirPath + Constants.GENIE_PATH_VAR
            + Constants.FILE_PATH_DELIMITER + Constants.LOGS_PATH_VAR);
        createDirectory(this.baseWorkingDirPath + Constants.GENIE_PATH_VAR
            + Constants.FILE_PATH_DELIMITER
            + Constants.APPLICATION_PATH_VAR);
        createDirectory(this.baseWorkingDirPath + Constants.GENIE_PATH_VAR
            + Constants.FILE_PATH_DELIMITER
            + Constants.COMMAND_PATH_VAR);
        createDirectory(this.baseWorkingDirPath + Constants.GENIE_PATH_VAR
            + Constants.FILE_PATH_DELIMITER
            + Constants.CLUSTER_PATH_VAR);

        // set the env variables in the launcher script
        final Writer writer = getWriter(this.jobLauncherScriptPath);

        // set environment variable for the job directory
        appendToWriter(writer, Constants.EXPORT + Constants.GENIE_JOB_DIR_ENV_VAR + Constants.EQUALS_SYMBOL
            + this.baseWorkingDirPath);

        // create environment variable for the application directory
        appendToWriter(writer, Constants.EXPORT + Constants.GENIE_APPLICATION_DIR_ENV_VAR + Constants.EQUALS_SYMBOL
            + this.baseWorkingDirPath
            + Constants.FILE_PATH_DELIMITER
            + Constants.GENIE_PATH_VAR
            + Constants.FILE_PATH_DELIMITER
            + Constants.APPLICATION_PATH_VAR);

        // create environment variable for the command directory
        appendToWriter(writer, Constants.EXPORT + Constants.GENIE_COMMAND_DIR_ENV_VAR + Constants.EQUALS_SYMBOL
            + this.baseWorkingDirPath
            + Constants.FILE_PATH_DELIMITER
            + Constants.GENIE_PATH_VAR
            + Constants.FILE_PATH_DELIMITER
            + Constants.COMMAND_PATH_VAR);

        // create environment variable for the cluster directory
        appendToWriter(writer, Constants.EXPORT + Constants.GENIE_CLUSTER_DIR_ENV_VAR + Constants.EQUALS_SYMBOL
            + this.baseWorkingDirPath
            + Constants.FILE_PATH_DELIMITER
            + Constants.GENIE_PATH_VAR
            + Constants.FILE_PATH_DELIMITER
            + Constants.CLUSTER_PATH_VAR);

        // close the writer
        closeWriter(writer);
    }
}
