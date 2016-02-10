package com.netflix.genie.core.jobs;

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.Writer;
import java.nio.file.Path;

/**
 * Implementation of the workflow task for handling Applications that a job needs.
 *
 * @author amsharma
 */
@Slf4j
public class ApplicationTask extends GenieBaseTask implements WorkflowTask {

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Context context
    ) throws GenieException {
        log.info("Execution Application Task in the workflow.");

        final JobExecutionEnvironment jobExecEnv =
            (JobExecutionEnvironment) context.getAttribute(JOB_EXECUTION_ENV_KEY);

        if (jobExecEnv == null) {
            throw new GenieServerException("Cannot run application task as jobExecutionEnvironment is null");
        }

        final String jobLauncherScriptPath = jobExecEnv.getJobWorkingDir() + "/" + GENIE_JOB_LAUNCHER_SCRIPT;
        final Writer writer = getWriter(jobLauncherScriptPath);

        for (Application application : jobExecEnv.getApplications()) {
            createDirectory(jobExecEnv.getJobWorkingDir() + "/applications/" + application.getId());

            final String applicationSetupFile = application.getSetupFile();

            if (applicationSetupFile != null && StringUtils.isNotBlank(applicationSetupFile)) {
                final Path setupFilePath = new File(applicationSetupFile).toPath();
                final String setupFileLocalPath = jobExecEnv.getJobWorkingDir()
                    + "/applications/"
                    + application.getId()
                    + "/"
                    + setupFilePath.getFileName();
                appendToWriter(writer, "source " + setupFileLocalPath + ";");
            }
            //TODO copy down dependencies
        }

        closeWriter(writer);
    }
}
