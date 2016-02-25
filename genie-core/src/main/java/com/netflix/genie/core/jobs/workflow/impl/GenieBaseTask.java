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

import com.google.common.collect.Lists;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
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


/**
 * An abstract class that all classes that implement a workflow task should inherit from. Provides some
 * helper methods that all classes can use.
 *
 * @author amsharma
 * @since 3.0.0
 */
public abstract class GenieBaseTask {

    // TODO move to common String constants class?
    protected static final String JOB_EXECUTION_ENV_KEY = "jee";
    protected static final String GENIE_JOB_LAUNCHER_SCRIPT = "genie_job_launcher.sh";
    protected static final String FILE_TRANSFER_SERVICE_KEY = "fts";
    protected static final String SETUP_FILE_PATH_PREFIX = "setup_file";
    protected static final String DEPENDENCY_FILE_PATH_PREFIX = "dependencies";
    protected static final String CONFIG_FILE_PATH_PREFIX = "config";
    protected static final String FILE_PATH_DELIMITER = "/";
    protected static final String APPLICATION_PATH_VAR = "applications";
    protected static final String COMMAND_PATH_VAR = "commmand";
    protected static final String CLUSTER_PATH_VAR = "cluster";
    protected static final String JOB_PATH_VAR = "job";
    protected static final String GENIE_PATH_VAR = "genie";
    protected static final String LOGS_PATH_VAR = "logs";

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
        this.executeBashCommand(Lists.newArrayList("mkdir", "-p", dirPath), null);
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
}
