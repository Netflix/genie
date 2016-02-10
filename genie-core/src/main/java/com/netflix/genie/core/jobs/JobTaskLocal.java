package com.netflix.genie.core.jobs;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.Writer;
import java.nio.file.Path;

/**
 * Implementation of the workflow task for processing job information in non genie-mode.
 *
 * @author amsharma
 */
@Slf4j
public class JobTaskLocal extends GenieBaseTask implements WorkflowTask {

    private static final String  STDERR_LOG_PATH = "./job/stderr";
    private static final String STDOUT_LOG_PATH = "./job/stdout";
    private static final String GENIE_DONE_FILE = "./genie/genie.done";

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Context context
    ) throws GenieException {
        log.info("Execution Job Task in the workflow.");

        final JobExecutionEnvironment jobExecEnv =
            (JobExecutionEnvironment) context.getAttribute(JOB_EXECUTION_ENV_KEY);

        if (jobExecEnv == null) {
            throw new GenieServerException("Cannot run application task as jobExecutionEnvironment is null");
        }

        final String jobLauncherScriptPath = jobExecEnv.getJobWorkingDir() + "/" + GENIE_JOB_LAUNCHER_SCRIPT;
        final Writer writer = getWriter(jobLauncherScriptPath);

        //TODO copy down dependencies
        final String jobSetupFile = jobExecEnv.getJobRequest().getSetupFile();

        if (jobSetupFile != null && StringUtils.isNotBlank(jobSetupFile)) {
            final Path setupFilePath = new File(jobSetupFile).toPath();
            final String setupFileLocalPath = jobExecEnv.getJobWorkingDir()
                + "/job/"
                + setupFilePath.getFileName();
            appendToWriter(writer, "source " + setupFileLocalPath + ";");
        }

        appendToWriter(writer, jobExecEnv.getCommand().getExecutable()
            + " "
            + jobExecEnv.getJobRequest().getCommandArgs());
        closeWriter(writer);
    }
}
