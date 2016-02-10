package com.netflix.genie.core.jobs;

import com.google.common.collect.Lists;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

/**
 * An abstract class that all classes that implement a workflow task should inherit from. Provides some
 * helper methods that all classes can use.
 *
 * @author amsharma
 */
public abstract class GenieBaseTask {

    // TODO move to common String constants class?
    protected static final String JOB_EXECUTION_ENV_KEY = "jee";
    protected static final String GENIE_JOB_LAUNCHER_SCRIPT = "genie_job_launcher.sh";

    // Directory paths env variables
    private static final String GENIE_WORKING_DIR_ENV_VAR = "GENIE_WORKING_DIR";
    private static final String GENIE_JOB_DIR_ENV_VAR = "GENIE_JOB_DIR";
    private static final String GENIE_CLUSTER_DIR_ENV_VAR = "GENIE_CLUSTER_DIR";
    private static final String GENIE_COMMAND_DIR_ENV_VAR = "GENIE_COMMAND_DIR";
    private static final String GENIE_APPLICATION_DIR_ENV_VAR = "GENIE_APPLICATION_DIR";

    /**
     * Helper method that executes a bash command.
     *
     * @param command An array consisting of the command to run
     */
    protected void executeBashCommand(
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
                throw new GenieServerException("Unable to execute bash command {}" + String.valueOf(command));
            }
        } catch (InterruptedException | IOException ie) {
            throw new GenieServerException("Unable to execute bash command {} with exception {}"
                + String.valueOf(command), ie);
        }
    }

    /**
     * Method to create directories on local unix filesystem.
     *
     * @param dirPath The directory path to create
     * @throws GenieException
     */
    protected void createDirectory(
        final String dirPath
    ) throws GenieException {
        this.executeBashCommand(Lists.newArrayList("mkdir", "-p", dirPath), null);
    }

    /**
     * Initializes the writer to create job_launcher_script.sh.
     *
     * @throws GenieException Throw exception in case of failure while intializing the writer
     */
    protected Writer getWriter(
        final String filePath) throws GenieException {
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
     * @throws GenieException Throw exception in case of failure while closing the writer
     */
    protected void closeWriter(
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

    //TODO get rid of this method and abstract it out in a different service
//    /**
//     * Method to iterate over a list of fileCopyImpls to copy files.
//     *
//     * @param src  The source path to copy
//     * @param dest The destination path to copy
//     */
//    protected void copyFiles(final String src, final String dest) throws GenieException {
//        for (final FileCopyService fcs : this.fileCopyServices) {
//            if (fcs.isValid(src)) {
//                fcs.copy(src, dest);
//            } else {
//                throw new GenieServerException("Genie not equipped to copy down files of this type.");
//            }
//        }
//    }
}
