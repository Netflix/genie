package com.netflix.genie.core.jobs;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;

/**
 * Implementation of the workflow task for handling Applications that a job needs.
 *
 * @author amsharma
 */
@Slf4j
public class IntialSetupTask extends GenieBaseTask implements WorkflowTask {

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Context context
    ) throws GenieException {
        log.info("Execution Initial setup Task in the workflow.");

        final JobExecutionEnvironment jobExecEnv =
            (JobExecutionEnvironment) context.getAttribute(JOB_EXECUTION_ENV_KEY);

        if (jobExecEnv == null) {
            throw new GenieServerException("Cannot run setup task as jobExecutionEnvironment is null");
        }

        // create top level directory structure for the job
        createDirectory(jobExecEnv.getJobWorkingDir());
        createDirectory(jobExecEnv.getJobWorkingDir() + "/genie");
        createDirectory(jobExecEnv.getJobWorkingDir() + "/genie/logs");
        createDirectory(jobExecEnv.getJobWorkingDir() + "/applications");
        createDirectory(jobExecEnv.getJobWorkingDir() + "/command");
        createDirectory(jobExecEnv.getJobWorkingDir() + "/cluster");
        createDirectory(jobExecEnv.getJobWorkingDir() + "/job");
    }
}
