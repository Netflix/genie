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
import com.netflix.genie.core.util.Utils;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
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
        log.debug("Executing Initial setup Task in the workflow.");

        super.executeTask(context);

        /** create top level directory structure for the job **/

        // Genie Directory {basedir/genie}
        Utils.createDirectory(this.jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR);

        // Genie Logs directory {basedir/genie/logs}
        Utils.createDirectory(this.jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.LOGS_PATH_VAR);

        // Genie applications directory {basedir/genie/applications}
        Utils.createDirectory(this.jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.APPLICATION_PATH_VAR);

        // Genie command directory {basedir/genie/command}
        Utils.createDirectory(this.jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.COMMAND_PATH_VAR);

        // Genie cluster directory {basedir/genie/cluster}
        Utils.createDirectory(this.jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.CLUSTER_PATH_VAR);

        /** set the env variables in the launcher script **/

        // set environment variable for the job directory
        Utils.appendToWriter(writer, JobConstants.EXPORT
            + JobConstants.GENIE_JOB_DIR_ENV_VAR
            + JobConstants.EQUALS_SYMBOL
            + this.jobWorkingDirectory);

        // create environment variable for the application directory
        Utils.appendToWriter(writer, JobConstants.EXPORT
            + JobConstants.GENIE_APPLICATION_DIR_ENV_VAR
            + JobConstants.EQUALS_SYMBOL
            + this.jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.APPLICATION_PATH_VAR);

        // create environment variable for the command directory
        Utils.appendToWriter(writer, JobConstants.EXPORT
            + JobConstants.GENIE_COMMAND_DIR_ENV_VAR
            + JobConstants.EQUALS_SYMBOL
            + this.jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.COMMAND_PATH_VAR);

        // create environment variable for the cluster directory
        Utils.appendToWriter(writer, JobConstants.EXPORT
            + JobConstants.GENIE_CLUSTER_DIR_ENV_VAR
            + JobConstants.EQUALS_SYMBOL
            + this.jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.CLUSTER_PATH_VAR);
    }
}
