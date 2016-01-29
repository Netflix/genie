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

package com.netflix.genie.core.jobs;

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.FileCopyService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.List;
import java.util.ArrayList;
import java.net.InetAddress;
import java.lang.reflect.Field;

/**
 * Class that contains the logic to setup a genie job and run it.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class JobExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(JobExecutor.class);
    private static final String PID = "pid";
    private static final String GENIE_JOB_LAUNCHER_SCRIPT = "genie_job_launcher.sh";
    private static final String  STDERR_LOG_PATH = "./job/stderr";
    private static final String STDOUT_LOG_PATH = "./job/stdout";
    private static final String GENIE_LOG_PATH = "/genie/logs/genie.log";
    private static final String GENIE_DONE_FILE = "./genie/genie.done";


    // Directory paths env variables
    private static final String GENIE_WORKING_DIR_ENV_VAR = "GENIE_WORKING_DIR";
    private static final String GENIE_JOB_DIR_ENV_VAR = "GENIE_JOB_DIR";
    private static final String GENIE_CLUSTER_DIR_ENV_VAR = "GENIE_CLUSTER_DIR";
    private static final String GENIE_COMMAND_DIR_ENV_VAR = "GENIE_COMMAND_DIR";
    private static final String GENIE_APPLICATION_DIR_ENV_VAR = "GENIE_APPLICATION_DIR";
    private final JobExecutionEnvironment jobExecEnv;
    private List<FileCopyService> fileCopyServiceImpls;

    private String genieLauncherScript;
    private Writer fileWriter;
    private String jobWorkingDir;

    // This determines whether we just want to setup the job to run or execute it as well and archive the directory.
    private String mode;

    /**
     * Constructor Initialize the object using Job execution environment object.
     *
     * @param fileCopyServiceImpls List of implementations of the file copy interface
     * @param jobExecEnv           The job execution environment details like the job, cluster,
     *                             command and applications
     * @param mode Whether to run it in full genie mode or just local.
     *
     * @throws GenieException Exception in case of an error
     */
    public JobExecutor(
        final List<FileCopyService> fileCopyServiceImpls,
        @NotNull(message = "Cannot initialize with null JobExecEnv")
        final JobExecutionEnvironment jobExecEnv,
        final String mode
    ) throws GenieException {

        this.fileCopyServiceImpls = fileCopyServiceImpls;
        this.jobExecEnv = jobExecEnv;
        this.mode = mode;

        if (this.jobExecEnv.getJobWorkingDir() == null || StringUtils.isBlank(this.jobExecEnv.getJobWorkingDir())) {
            throw new GenieServerException("Cannot run job as working directory is not set");
        } else {
            this.jobWorkingDir = jobExecEnv.getJobWorkingDir();
        }

        // iniitalize variables
        genieLauncherScript = this.jobWorkingDir + "/" + GENIE_JOB_LAUNCHER_SCRIPT;

        // create system directories
        makeDir(jobExecEnv.getJobWorkingDir());
        makeDir(jobExecEnv.getJobWorkingDir() + "/genie");
        makeDir(jobExecEnv.getJobWorkingDir() + "/genie/logs");

        // initialize the writer to create the joblauncher script
        initializeWriter();
    }

    /**
     * Method that sets up the job directory and runs based on mode specified.
     *
     * @throws GenieException Throw exception in case of error.
     */
    public void execute() throws GenieException {
        processApplications();
        processCommand();
        processCluster();
        processJob();
        closeWriter();

        if (this.mode.equals("genie")) {
            startJob();
        }
    }


    /**
     * Method that starts the execution of the job and updates the database with its execution details.
     */
    private void startJob() throws GenieException {
        final List command = new ArrayList<>();
        command.add("bash");
        command.add(genieLauncherScript);

        final ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(new File(this.jobWorkingDir));
        pb.redirectOutput(new File(this.jobWorkingDir + GENIE_LOG_PATH));
        pb.redirectError(new File(this.jobWorkingDir + GENIE_LOG_PATH));

        try {
            final Process process = pb.start();
            final String hostname = InetAddress.getLocalHost().getHostAddress();
            final int processId = this.getProcessId(process);
            jobExecEnv.setProcessId(processId);
            jobExecEnv.setHostname(hostname);
        } catch (IOException ie) {
            throw new GenieServerException("Unable to start command " + String.valueOf(command), ie);
        }
    }

    /**
     * Method that sets up the current working directory for executing the job.
     */
    private void setupWorkingDirectory() throws GenieException {

        makeDir(jobExecEnv.getJobWorkingDir());
        makeDir(jobExecEnv.getJobWorkingDir() + "/job");
        makeDir(jobExecEnv.getJobWorkingDir() + "/cluster");
        makeDir(jobExecEnv.getJobWorkingDir() + "/command");
        makeDir(jobExecEnv.getJobWorkingDir() + "/applications");
    }

    /**
     * Process all applications content needed for the job to run.
     */
    private void processApplications() throws GenieException {

        makeDir(this.jobWorkingDir + "/applications");
        for (Application application : this.jobExecEnv.getApplications()) {
            makeDir(jobExecEnv.getJobWorkingDir() + "/applications/" + application.getId());

            final String applicationSetupFile = application.getSetupFile();

            if (applicationSetupFile != null && StringUtils.isNotBlank(applicationSetupFile)) {
                final Path setupFilePath = new File(applicationSetupFile).toPath();
                final String setupFileLocalPath = jobExecEnv.getJobWorkingDir()
                    + "/applications/"
                    + application.getId()
                    + "/"
                    + setupFilePath.getFileName();
                appendToWriter("source " + setupFileLocalPath + ";");
            }
            //TODO copy down dependencies
        }

    }

    /**
     * Process the command content needed for the job to run.
     */
    private void processCommand() throws GenieException {

        makeDir(this.jobWorkingDir + "/command");
        makeDir(jobExecEnv.getJobWorkingDir() + "/command/" + jobExecEnv.getCommand().getId());
        final String commandSetupFile = jobExecEnv.getCommand().getSetupFile();

        if (commandSetupFile != null && StringUtils.isNotBlank(commandSetupFile)) {
            final Path setupFilePath = new File(commandSetupFile).toPath();
            final String setupFileLocalPath = jobExecEnv.getJobWorkingDir()
                + "/applications/"
                + jobExecEnv.getCommand().getId()
                + "/"
                + setupFilePath.getFileName();
            appendToWriter("source " + setupFileLocalPath + ";");

        }
        //TODO copy down dependencies
    }

    /**
     * Process the cluster content needed for the job to run.
     */
    private void processCluster() throws GenieException {
        makeDir(this.jobWorkingDir + "/cluster");
        makeDir(jobExecEnv.getJobWorkingDir() + "/cluster/" + jobExecEnv.getCluster().getId());
        //TODO copy down dependencies
    }

    /**
     * Process the Job content needed for the job to run.
     */
    private void processJob() throws GenieException {
        //TODO copy down dependencies
        makeDir(jobExecEnv.getJobWorkingDir() + "/job");
        final String jobSetupFile = jobExecEnv.getJobRequest().getSetupFile();

        if (jobSetupFile != null && StringUtils.isNotBlank(jobSetupFile)) {
            final Path setupFilePath = new File(jobSetupFile).toPath();
            final String setupFileLocalPath = jobExecEnv.getJobWorkingDir()
                + "/job/"
                + setupFilePath.getFileName();
            appendToWriter("source " + setupFileLocalPath + ";");
        }

        // TODO temporary solution figure out best way to handle two modes
        if (this.mode.equals("genie")) {
            appendToWriter(
                jobExecEnv.getCommand().getExecutable()
                    + " "
                    + jobExecEnv.getJobRequest().getCommandArgs()
                    + " > "
                    + STDOUT_LOG_PATH
                    + " 2> "
                    + STDERR_LOG_PATH
            );
            // capture exit code and write to genie.done file
            appendToWriter("echo $? > " + GENIE_DONE_FILE);
        } else {
            appendToWriter(jobExecEnv.getCommand().getExecutable() + " " + jobExecEnv.getJobRequest().getCommandArgs());
        }
    }

    /**
     * Method to create directories on local unix filesystem.
     *
     * @param dirPath The directory path to create
     * @throws GenieException
     */
    private void makeDir(final String dirPath) throws GenieException {
        this.executeBashCommand(Lists.newArrayList("mkdir", "-p", dirPath), null);
    }

    /**
     * Method to iterate over a list of fileCopyImpls to copy files.
     *
     * @param src  The source path to copy
     * @param dest The destination path to copy
     */
    private void copyFiles(final String src, final String dest) throws GenieException {
        for (final FileCopyService fcs : this.fileCopyServiceImpls) {
            if (fcs.isValid(src)) {
                fcs.copy(src, dest);
            } else {
                throw new GenieServerException("Genie not equipped to copy down files of this type.");
            }
        }
    }

    /**
     * Helper method that executes a bash command.
     *
     * @param command An array consisting of the command to run
     */
    private void executeBashCommand(final List<String> command, final String workingDirectory) throws GenieException {
        final ProcessBuilder pb = new ProcessBuilder(command);
        if (workingDirectory != null) {
            pb.directory(new File(workingDirectory));
        }
        try {
            final Process process = pb.start();
            final int errCode = process.waitFor();
            if (errCode != 0) {
                throw new GenieServerException("Unable to execute bash command" + String.valueOf(command));
            }
        } catch (InterruptedException | IOException ie) {
            throw new GenieServerException("Unable to execute bash command" + String.valueOf(command), ie);
        }
    }

    /**
     * Initializes the writer to create job_launcher_script.sh.
     *
     * @throws GenieException Throw exception in case of failure while intializing the writer
     */
    private void initializeWriter() throws GenieException {
        try {
            //fileWriter = new FileWriter(genieLauncherScript);
            fileWriter = new OutputStreamWriter(new FileOutputStream(genieLauncherScript), "UTF-8");
        } catch (IOException ioe) {
            throw new GenieServerException("Could not open file to crate genie_launcher.sh", ioe);
        }
    }

    /**
     * Closes the writer to create job_launcher_script.sh.
     *
     * @throws GenieException Throw exception in case of failure while closing the writer
     */
    private void closeWriter() throws GenieException {

        try {
            if (fileWriter != null) {
                fileWriter.close();
            }
        } catch (IOException ioe) {
            throw new GenieServerException("Error closing file writer", ioe);
        }
    }

    /**
     * Appends content to the writer.
     *
     * @param content The content to write.
     * @throws GenieException Throw exception in case of failure while writing content to writer.
     */
    private void appendToWriter(final String content) throws GenieException {

        try {
            if (content != null && StringUtils.isNotBlank(content)) {
                fileWriter.write(content);
                fileWriter.write("\n");
            }
        } catch (IOException ioe) {
            throw new GenieServerException("Error closing file writer", ioe);
        }
    }

    /**
     * Get process id for the given process.
     *
     * @param proc java process object representing the job launcher
     * @return pid for this process
     * @throws GenieException if there is an error getting the process id
     */
    private int getProcessId(final Process proc) throws GenieException {
        LOG.debug("called");

        try {
            final Field f = proc.getClass().getDeclaredField(PID);
            f.setAccessible(true);
            return f.getInt(proc);
        } catch (final IllegalAccessException | IllegalArgumentException | NoSuchFieldException | SecurityException e) {
            final String msg = "Can't get process id for job";
            LOG.error(msg, e);
            throw new GenieServerException(msg, e);
        }
    }
}
