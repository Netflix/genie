package com.netflix.genie.core.jobs;

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.FileCopyService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * A class that takes in Job Information, sets up the working directory and starts the job.
 *
 * @author amsharma
 */
@Component
public class JobExecutionHandler extends JobSetupHandler {

    private static final Logger LOG = LoggerFactory.getLogger(JobExecutionHandler.class);
    private static final String PID = "pid";
    private static final String  STDERR_LOG_PATH = "./job/stderr";
    private static final String STDOUT_LOG_PATH = "./job/stdout";
    private static final String GENIE_DONE_FILE = "./genie/genie.done";
    private static final String GENIE_LOG_PATH = "/genie/logs/genie.log";

    /**
     * Handles the job differently based on environment.
     *
     * @param fileCopyServiceImpls List of file copy interface implementations
     * @param jobExecEnv           Job Execution environment object containing everything needed to handle the job
     * @return JobExecution DTO
     * @throws GenieException if there is an error.
     */
    @Override
    public JobExecution handleJob(
        final List<FileCopyService> fileCopyServiceImpls,
        final JobExecutionEnvironment jobExecEnv
    ) throws GenieException {

        super.handleJob(fileCopyServiceImpls, jobExecEnv);
        return this.jobExecution;
    }

    @Override
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
            final int processId = getProcessId(process);
            this.jobExecution = new JobExecution.Builder(hostname, processId).build();
        } catch (IOException ie) {
            throw new GenieServerException("Unable to start command " + String.valueOf(command), ie);
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
