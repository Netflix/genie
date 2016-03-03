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
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.util.Constants;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import com.netflix.genie.core.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Map;

/**
 * An abstract class that all classes that implement a workflow task should inherit from. Provides some
 * helper methods that all classes can use.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public abstract class GenieBaseTask implements WorkflowTask {

    protected GenieFileTransferService fts;
    protected JobExecutionEnvironment jobExecEnv;
    protected String runScript;
    protected String jobWorkigDirectory;

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Map<String, Object> context
    ) throws GenieException {
        log.debug("called");

        this.jobExecEnv =
            (JobExecutionEnvironment) context.get(Constants.JOB_EXECUTION_ENV_KEY);

        if (this.jobExecEnv == null) {
            throw new GeniePreconditionException("Cannot run application task as jobExecutionEnvironment is null");
        }

        this.fts = (GenieFileTransferService) context.get(Constants.FILE_TRANSFER_SERVICE_KEY);

        try {
            this.jobWorkigDirectory = this.jobExecEnv.getJobWorkingDir().getCanonicalPath();
        } catch (IOException ioe) {
            throw new GenieServerException("Could not get base job working directory due to " + ioe);
        }

        this.runScript = this.jobWorkigDirectory
            + Constants.FILE_PATH_DELIMITER
            + Constants.GENIE_JOB_LAUNCHER_SCRIPT;
    }

    /**
     * Helper Function to fetch file to local dir.
     *
     * @param dir The directory where to copy the file
     * @param id The id to be appended to the destination path
     * @param filePath Source file path
     * @param fileType Type of file like setup, config or dependency
     * @param adminResources Entity type Application, Cluster, Command or Job
     * @return Local file path constructed where the file is copied to
     *
     * @throws GenieException If there is any problem
     */
    public String buildLocalFilePath(
        @NotBlank
        final String dir,
        @NotBlank
        final String id,
        @NotBlank
        final String filePath,
        @NotNull
        final Constants.FileType fileType,
        @NotNull
        final Constants.AdminResources adminResources
    ) throws GenieException {

        String entityPathVar = null;
        String filePathVar = null;

        switch (adminResources) {
            case APPLICATION:
                entityPathVar = Constants.APPLICATION_PATH_VAR;
                break;
            case COMMAND:
                entityPathVar = Constants.COMMAND_PATH_VAR;
                break;
            case CLUSTER:
                entityPathVar = Constants.CLUSTER_PATH_VAR;
                break;
            default:
                break;
        }

        switch (fileType) {
            case CONFIG:
                filePathVar = Constants.CONFIG_FILE_PATH_PREFIX;
                break;
            case SETUP:
                break;
            case DEPENDENCIES:
                filePathVar = Constants.DEPENDENCY_FILE_PATH_PREFIX;
                break;
            default:
                break;
        }

        if (filePath != null && StringUtils.isNotBlank(filePath)) {
            final String fileName = Utils.getFileNameFromPath(filePath);
            final StringBuilder localPath = new StringBuilder()
                .append(dir)
                .append(Constants.FILE_PATH_DELIMITER)
                .append(Constants.GENIE_PATH_VAR);

            if (entityPathVar != null) {
                localPath.append(Constants.FILE_PATH_DELIMITER)
                    .append(entityPathVar);
            }

            localPath.append(Constants.FILE_PATH_DELIMITER)
                .append(id);

            if (filePathVar != null) {
                localPath.append(Constants.FILE_PATH_DELIMITER)
                    .append(filePathVar);

            }
            localPath.append(Constants.FILE_PATH_DELIMITER).append(fileName);

            return localPath.toString();

        } else {
            throw new GenieBadRequestException("Could not construct localPath.");
        }
    }

    /**
     * Helper method to create the directory for a particular application, cluster or command in the
     * current working directory for the job.
     *
     * @param id The id of entity instance
     * @param adminResources The type of entity Application, Cluster or Command
     *
     * @throws GenieException If there is any problem
     */
    public void createEntityInstanceDirectory(
        @NotBlank
        final String id,
        @NotNull
        final Constants.AdminResources adminResources
    ) throws GenieException {
        String entityPathVar = null;

        switch (adminResources) {
            case APPLICATION:
                entityPathVar = Constants.APPLICATION_PATH_VAR;
                break;
            case COMMAND:
                entityPathVar = Constants.COMMAND_PATH_VAR;
                break;
            case CLUSTER:
                entityPathVar = Constants.CLUSTER_PATH_VAR;
                break;
            default:
                return;
        }

        Utils.createDirectory(
            this.jobWorkigDirectory
                + Constants.FILE_PATH_DELIMITER
                + Constants.GENIE_PATH_VAR
                + Constants.FILE_PATH_DELIMITER
                + entityPathVar
                + Constants.FILE_PATH_DELIMITER
                + id);
    }
}
