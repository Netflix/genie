/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.core.util;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Field;

/**
 * Class that provides some generic utility or helper functions.
 *
 * @author amsharma
 */
@Slf4j
public abstract class Utils {

    private static Executor executor = new DefaultExecutor();

    /**
     * Closes the stream to the writer supplied.
     *
     * @param writer The writer object to close
     *
     * @throws GenieException Throw exception in case of failure while closing the writer
     */
    public static void closeWriter(
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
     * Initializes the writer to create job_launcher_script.sh.
     *
     * @param filePath The path of the file to which we need a file handle.
     * @return Return a writer object to the file specified
     *
     * @throws GenieException Throw exception in case of failure while intializing the writer
     */
    public static Writer getWriter(
        @NotBlank(message = "Path of the file cannot be blank.")
        final String filePath
    ) throws GenieException {
        try {
            return new OutputStreamWriter(new FileOutputStream(filePath, true), "UTF-8");
        } catch (IOException ioe) {
            throw new GenieServerException("Could not create a writer to file "
                + filePath + "due to exception", ioe);
        }
    }

    /**
     * Appends content to the writer and adds a newline after.
     *
     * @param writer The writer stream to write to.
     * @param content The content to write.
     * @throws GenieException Throw exception in case of failure while writing content to writer.
     */
    public static void appendToWriter(
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
     * Method to create directories on local unix filesystem.
     *
     * @param dirPath The directory path to create
     * @throws GenieException If there is a problem.
     */
    public static void createDirectory(
        @NotBlank(message = "Directory path cannot be blank.")
        final String dirPath
    ) throws GenieException {
        final File dir = new File(dirPath);
        if (!dir.mkdirs()) {
            throw new GenieServerException("Could not create directory: " + dirPath);
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
    public static String getFileNameFromPath(
        @NotBlank (message = "The path of the file cannot be blank.")
        final String filePath
    ) throws GenieException {
        return new File(filePath).getName();
    }

    /**
     * Get process id for the given process.
     *
     * @param proc java process object representing the job launcher
     * @return pid for this process
     * @throws GenieException if there is an error getting the process id
     */
    public static int getProcessId(final Process proc) throws GenieException {
        log.debug("called");

        try {
            final Field f = proc.getClass().getDeclaredField(Constants.PID);
            f.setAccessible(true);
            return f.getInt(proc);
        } catch (final IllegalAccessException
            | IllegalArgumentException
            | NoSuchFieldException
            | SecurityException e) {
            final String msg = "Can't get process id for job";
            log.error(msg, e);
            throw new GenieServerException(msg, e);
        }
    }
}
