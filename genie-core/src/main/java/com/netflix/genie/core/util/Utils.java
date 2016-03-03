package com.netflix.genie.core.util;

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
import java.util.List;

/**
 * Class that provides some generic utility or helper functions.
 *
 * @author amsharma
 */
public abstract class Utils {

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
     * Helper method that executes a bash command.
     *
     * @param command An array consisting of the command to run
     * @param workingDirectory The working directory to set while running the command
     *
     * @throws GenieException If there is problem.
     */
    public static void executeBashCommand(
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
}
