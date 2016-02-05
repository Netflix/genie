package com.netflix.genie.core.jobs;

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.FileCopyService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.List;

/**
 * A class that takes in job information and just sets it up for execution.
 *
 * @author amsharma
 */
public class JobSetupHandler implements JobHandler {


    private static final Logger LOG = LoggerFactory.getLogger(JobSetupHandler.class);
    private static final String PID = "pid";
    private static final String GENIE_JOB_LAUNCHER_SCRIPT = "genie_job_launcher.sh";

    // Directory paths env variables
    private static final String GENIE_WORKING_DIR_ENV_VAR = "GENIE_WORKING_DIR";
    private static final String GENIE_JOB_DIR_ENV_VAR = "GENIE_JOB_DIR";
    private static final String GENIE_CLUSTER_DIR_ENV_VAR = "GENIE_CLUSTER_DIR";
    private static final String GENIE_COMMAND_DIR_ENV_VAR = "GENIE_COMMAND_DIR";
    private static final String GENIE_APPLICATION_DIR_ENV_VAR = "GENIE_APPLICATION_DIR";

    protected JobExecution jobExecution;
    protected JobExecutionEnvironment jobExecEnv;
    protected List<FileCopyService> fileCopyServices;
    protected String genieLauncherScript;
    protected String jobWorkingDir;

    private Writer fileWriter;


    /**
     * Handles the job differently based on environment.
     *
     * @param fileCopyServiceImpls List of file copy interface implementations
     * @param jobExecuctionEnvironment Job Execution environment object containing everything needed to handle the job
     * @return JobExecution DTO
     * @throws GenieException if there is an error.
     */
    @Override
    public JobExecution handleJob(
        final List<FileCopyService> fileCopyServiceImpls,
        final JobExecutionEnvironment jobExecuctionEnvironment
    ) throws GenieException {


        this.fileCopyServices = fileCopyServiceImpls;
        this.jobExecEnv = jobExecuctionEnvironment;

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
        processApplications();
        processCommand();
        processCluster();
        processJob();
        closeWriter();

        return this.jobExecution;
    }

    /**
     * Method that sets up the current working directory for executing the job.
     */
    protected void setupWorkingDirectory() throws GenieException {

        makeDir(jobExecEnv.getJobWorkingDir());
        makeDir(jobExecEnv.getJobWorkingDir() + "/job");
        makeDir(jobExecEnv.getJobWorkingDir() + "/cluster");
        makeDir(jobExecEnv.getJobWorkingDir() + "/command");
        makeDir(jobExecEnv.getJobWorkingDir() + "/applications");
    }

    /**
     * Process all applications content needed for the job to run.
     */
    protected void processApplications() throws GenieException {

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
    protected void processCommand() throws GenieException {

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
    protected void processCluster() throws GenieException {
        makeDir(this.jobWorkingDir + "/cluster");
        makeDir(jobExecEnv.getJobWorkingDir() + "/cluster/" + jobExecEnv.getCluster().getId());
        //TODO copy down dependencies
    }

    /**
     * Process the Job content needed for the job to run.
     */
    protected void processJob() throws GenieException {
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

        appendToWriter(jobExecEnv.getCommand().getExecutable() + " " + jobExecEnv.getJobRequest().getCommandArgs());
    }

    /**
     * Method to create directories on local unix filesystem.
     *
     * @param dirPath The directory path to create
     * @throws GenieException
     */
    protected void makeDir(final String dirPath) throws GenieException {
        this.executeBashCommand(Lists.newArrayList("mkdir", "-p", dirPath), null);
    }

    /**
     * Method to iterate over a list of fileCopyImpls to copy files.
     *
     * @param src  The source path to copy
     * @param dest The destination path to copy
     */
    protected void copyFiles(final String src, final String dest) throws GenieException {
        for (final FileCopyService fcs : this.fileCopyServices) {
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
    protected void executeBashCommand(final List<String> command, final String workingDirectory) throws GenieException {
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
    protected void initializeWriter() throws GenieException {
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
    protected void closeWriter() throws GenieException {

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
    protected void appendToWriter(final String content) throws GenieException {

        try {
            if (content != null && StringUtils.isNotBlank(content)) {
                fileWriter.write(content);
                fileWriter.write("\n");
            }
        } catch (IOException ioe) {
            throw new GenieServerException("Error closing file writer", ioe);
        }
    }
}
