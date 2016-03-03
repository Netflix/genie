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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.List;
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
    protected String jobLauncherScriptPath;
    protected String baseWorkingDirPath;

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
            this.baseWorkingDirPath = this.jobExecEnv.getJobWorkingDir().getCanonicalPath();
        } catch (IOException ioe) {
            throw new GenieServerException("Could not get base job working directory due to " + ioe);
        }

        this.jobLauncherScriptPath = this.baseWorkingDirPath
            + Constants.FILE_PATH_DELIMITER
            + Constants.GENIE_JOB_LAUNCHER_SCRIPT;
    }

    /**
     * Helper method that executes a bash command.
     *
     * @param command An array consisting of the command to run
     * @param workingDirectory The working directory to set while running the command
     *
     * @throws GenieException If there is problem.
     */
    public void executeBashCommand(
        @NotNull(message = "The command to be run cannot be empty.")
        final List<String> command,
        final String workingDirectory
    ) throws GenieException {
        final ProcessBuilder pb = new ProcessBuilder(command);
        if (workingDirectory != null) {
            pb.directory(new File(workingDirectory));
        }
        try {
            final Process process = pb.start();
            final int errCode = process.waitFor();
            if (errCode != 0) {
                throw new GenieServerException("Unable to execute bash command " + String.valueOf(command));
            }
        } catch (InterruptedException | IOException ie) {
            throw new GenieServerException("Unable to execute bash command "
                + String.valueOf(command)
                + " with exception "
                +  ie.toString());
        }
    }

    /**
     * Method to create directories on local unix filesystem.
     *
     * @param dirPath The directory path to create
     * @throws GenieException If there is a problem.
     */
    public void createDirectory(
        @NotBlank(message = "Directory path cannot be blank.")
        final String dirPath
    ) throws GenieException {
        final File dir = new File(dirPath);
        if (!dir.mkdirs()) {
            throw new GenieServerException("Could not create directory: " + dirPath);
        }
    }

    /**
     * Initializes the writer to create job_launcher_script.sh.
     *
     * @param filePath The path of the file to which we need a file handle.
     * @return Return a writer object to the file specified
     *
     * @throws GenieException Throw exception in case of failure while intializing the writer
     */
    public Writer getWriter(
        @NotBlank(message = "Path of the file cannot be blank.")
        final String filePath
    ) throws GenieException {
        try {
            //fileWriter = new FileWriter(genieLauncherScript);
            return new OutputStreamWriter(new FileOutputStream(filePath, true), "UTF-8");
        } catch (IOException ioe) {
            throw new GenieServerException("Could not create a writer to file "
                + filePath + "due to exception", ioe);
        }
    }

    /**
     * Closes the stream to the writer supplied.
     *
     * @param writer The writer object to close
     *
     * @throws GenieException Throw exception in case of failure while closing the writer
     */
    public void closeWriter(
        final Writer writer
    ) throws GenieException {

        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException ioe) {
            throw new GenieServerException("Error closing file writer", ioe);
        }
    }

    /**
     * Appends content to the writer and adds a newline after.
     *
     * @param writer The writer stream to write to.
     * @param content The content to write.
     * @throws GenieException Throw exception in case of failure while writing content to writer.
     */
    protected void appendToWriter(
        @NotNull(message = "Cannot write to null writer")
        final Writer writer,
        final String content
    ) throws GenieException {

        try {
            if (content != null && StringUtils.isNotBlank(content)) {
                writer.write(content);
                writer.write("\n");
            }
        } catch (IOException ioe) {
            throw new GenieServerException("Error closing file writer", ioe);
        }
    }

    /**
     * Get the name of the file specified in the path.
     *
     * @param filePath Path of the file
     * @return The name of the file
     *
     * @throws GenieException if there is any problem
     */
    public String getFileNameFromPath(
        @NotBlank (message = "The path of the file cannot be blank.")
        final String filePath
    ) throws GenieException {
        final Path path = new File(filePath).toPath().getFileName();
        if (path != null) {
            return path.toString();
        } else {
            throw new GenieBadRequestException("Could not figure filename for path");
        }
    }

    /**
     * Helper Function to fetch file to local dir.
     *
     * @param dir The directory where to copy the file
     * @param id The id to be appended to the destination path
     * @param filePath Source file path
     * @param fileType Type of file like setup, config or dependency
     * @param entityType Entity type Application, Cluster, Command or Job
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
        final Constants.EntityType entityType
    ) throws GenieException {

        String entityPathVar = null;
        String filePathVar = null;

        switch (entityType) {
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
                //filePathVar = Constants.SETUP_FILE_PATH_PREFIX;
                break;
            case DEPENDENCIES:
                filePathVar = Constants.DEPENDENCY_FILE_PATH_PREFIX;
                break;
            default:
                break;
        }

        if (filePath != null && StringUtils.isNotBlank(filePath)) {
            final String fileName = getFileNameFromPath(filePath);
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
     * @param entityType The type of entity Application, Cluster or Command
     *
     * @throws GenieException If there is any problem
     */
    public void createEntityInstanceDirectory(
        @NotBlank
        final String id,
        @NotNull
        final Constants.EntityType entityType
    ) throws GenieException {
        String entityPathVar = null;

        switch (entityType) {
            case APPLICATION:
                entityPathVar = Constants.APPLICATION_PATH_VAR;
                break;
            case COMMAND:
                entityPathVar = Constants.COMMAND_PATH_VAR;
                break;
            case CLUSTER:
                entityPathVar = Constants.CLUSTER_PATH_VAR;
                break;
            case JOB:
                return;
            default:
                return;
        }

        createDirectory(
            this.baseWorkingDirPath
            + Constants.GENIE_PATH_VAR
                + Constants.FILE_PATH_DELIMITER
                + entityPathVar
                + Constants.FILE_PATH_DELIMITER
                + id);
    }
}
