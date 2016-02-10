package com.netflix.genie.core.jobs;

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.services.FileCopyService;

import java.util.List;

/**
 * Interface that takes the appropriate action given a job.
 *
 * @author amsharma
 */
public interface JobHandler {

    /**
     * Handles the job differently based on environment.
     *
     * @param fileCopyServiceImpls List of file copy interface implementations
     * @param jobExecEnv Job Execution environment object containing everything needed to handle the job
     * @return JobExecution DTO
     * @throws GenieException if there is an error.
     */
    JobExecution handleJob(
        final List<FileCopyService> fileCopyServiceImpls,
        final JobExecutionEnvironment jobExecEnv
    ) throws GenieException;
}

