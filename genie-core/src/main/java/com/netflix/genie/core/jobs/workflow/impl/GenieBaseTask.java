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
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jobs.AdminResources;
import com.netflix.genie.core.jobs.FileType;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
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
    protected String jobWorkingDirectory;
    protected String genieDir;
    protected Writer writer;

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Map<String, Object> context
    ) throws GenieException, IOException {
        log.debug("called");

        this.jobExecEnv =
            (JobExecutionEnvironment) context.get(JobConstants.JOB_EXECUTION_ENV_KEY);

        if (this.jobExecEnv == null) {
            throw new GeniePreconditionException("Cannot run application task as jobExecutionEnvironment is null");
        }

        this.fts = (GenieFileTransferService) context.get(JobConstants.FILE_TRANSFER_SERVICE_KEY);

        try {
            this.jobWorkingDirectory = this.jobExecEnv.getJobWorkingDir().getCanonicalPath();
        } catch (IOException ioe) {
            throw new GenieServerException("Could not get base job working directory due to " + ioe);
        }

        this.writer = (Writer) context.get(JobConstants.WRITER_KEY);

        this.genieDir = this.jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR;
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
        final FileType fileType,
        @NotNull
        final AdminResources adminResources
    ) throws GenieException {

        String entityPathVar = null;
        String filePathVar = null;

        switch (adminResources) {
            case APPLICATION:
                entityPathVar = JobConstants.APPLICATION_PATH_VAR;
                break;
            case COMMAND:
                entityPathVar = JobConstants.COMMAND_PATH_VAR;
                break;
            case CLUSTER:
                entityPathVar = JobConstants.CLUSTER_PATH_VAR;
                break;
            default:
                break;
        }

        switch (fileType) {
            case CONFIG:
                filePathVar = JobConstants.CONFIG_FILE_PATH_PREFIX;
                break;
            case SETUP:
                break;
            case DEPENDENCIES:
                filePathVar = JobConstants.DEPENDENCY_FILE_PATH_PREFIX;
                break;
            default:
                break;
        }

        final String fileName = filePath.substring(filePath.lastIndexOf(JobConstants.FILE_PATH_DELIMITER) + 1);
        final StringBuilder localPath = new StringBuilder()
            .append(dir)
            .append(JobConstants.FILE_PATH_DELIMITER)
            .append(JobConstants.GENIE_PATH_VAR);

        localPath.append(JobConstants.FILE_PATH_DELIMITER)
                .append(entityPathVar);

        localPath.append(JobConstants.FILE_PATH_DELIMITER)
            .append(id);

        if (filePathVar != null) {
            localPath.append(JobConstants.FILE_PATH_DELIMITER)
                .append(filePathVar);

        }
        localPath.append(JobConstants.FILE_PATH_DELIMITER).append(fileName);

        return localPath.toString();


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
        final AdminResources adminResources
    ) throws GenieException {
        String entityPathVar = null;

        switch (adminResources) {
            case APPLICATION:
                entityPathVar = JobConstants.APPLICATION_PATH_VAR;
                break;
            case COMMAND:
                entityPathVar = JobConstants.COMMAND_PATH_VAR;
                break;
            case CLUSTER:
                entityPathVar = JobConstants.CLUSTER_PATH_VAR;
                break;
            // TODO The @NotNull validation will make sure we never reach default, but checkstyle forces a default.
            default:
        }

        this.createDirectory(
            this.genieDir
                + JobConstants.FILE_PATH_DELIMITER
                + entityPathVar
                + JobConstants.FILE_PATH_DELIMITER
                + id);
    }

    /**
     * Helper method to create the config directory for a particular application, cluster or command in the
     * current working directory for the job.
     *
     * @param id The id of entity instance
     * @param adminResources The type of entity Application, Cluster or Command
     *
     * @throws GenieException If there is any problem
     */
    public void createEntityInstanceConfigDirectory(
        @NotBlank
        final String id,
        @NotNull
        final AdminResources adminResources
    ) throws GenieException {
        String entityPathVar = null;

        switch (adminResources) {
            case APPLICATION:
                entityPathVar = JobConstants.APPLICATION_PATH_VAR;
                break;
            case COMMAND:
                entityPathVar = JobConstants.COMMAND_PATH_VAR;
                break;
            case CLUSTER:
                entityPathVar = JobConstants.CLUSTER_PATH_VAR;
                break;
            // TODO The @NotNull validation will make sure we never reach default, but checkstyle forces a default.
            default:
        }

        this.createDirectory(
            this.genieDir
                + JobConstants.FILE_PATH_DELIMITER
                + entityPathVar
                + JobConstants.FILE_PATH_DELIMITER
                + id
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.CONFIG_FILE_PATH_PREFIX);
    }

    /**
     * Helper method to create the dependency directory for a particular application, cluster or command in the
     * current working directory for the job.
     *
     * @param id The id of entity instance
     * @param adminResources The type of entity Application, Cluster or Command
     *
     * @throws GenieException If there is any problem
     */
    public void createEntityInstanceDependenciesDirectory(
        @NotBlank
        final String id,
        @NotNull
        final AdminResources adminResources
    ) throws GenieException {
        String entityPathVar = null;

        switch (adminResources) {
            case APPLICATION:
                entityPathVar = JobConstants.APPLICATION_PATH_VAR;
                break;
            case COMMAND:
                entityPathVar = JobConstants.COMMAND_PATH_VAR;
                break;
            case CLUSTER:
                entityPathVar = JobConstants.CLUSTER_PATH_VAR;
                break;
            // TODO The @NotNull validation will make sure we never reach default, but checkstyle forces a default.
            default:
        }

        this.createDirectory(
            this.genieDir
                + JobConstants.FILE_PATH_DELIMITER
                + entityPathVar
                + JobConstants.FILE_PATH_DELIMITER
                + id
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.DEPENDENCY_FILE_PATH_PREFIX);
    }

    /**
     * Helper method to create directories on local filesystem.
     *
     * @param dirPath The directory path to create
     * @throws GenieException If there is a problem.
     */
    protected void createDirectory(
        @NotBlank(message = "Directory path cannot be blank.")
        final String dirPath
    ) throws GenieException {
        final File dir = new File(dirPath);
        if (!dir.mkdirs()) {
            throw new GenieServerException("Could not create directory: " + dirPath);
        }
    }
}
