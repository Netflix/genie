package com.netflix.genie.core.jobs;

import com.netflix.genie.common.dto.JobExecutionEnvironment;
import com.netflix.genie.common.dto.JobRequest;

/**
 * Contains the logic to get all the details needed to run a job.
 * Resolves the criteria provided in the job request to construct the
 * JobExecutionEnvironment dto.
 * @author amsharma
 */
public class JobEnvBuilder {

    /**
     * Method that accepts a jobRequest object and uses the criteria selected to build a
     * jobExecutionEnvironmentBuilder object.
     *
     * @param jr The job request object
     * @return jobExecutionEnvironment
     */
    public JobExecutionEnvironment build(final JobRequest jr) {
        return null;
    }
}
