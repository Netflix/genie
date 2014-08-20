/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.netflix.genie.server.jobmanager;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Job;

/**
 * Interface for the runnable which monitors a job.
 *
 * @author tgianos
 */
public interface JobMonitor extends Runnable {

    /**
     * Set the job for this to monitor.
     *
     * @param job The job to monitor. Not null.
     * @throws GenieException On any exception
     */
    void setJob(final Job job) throws GenieException;

    /**
     * Set the job manager for this monitor to use.
     *
     * @param jobManager The job manager to use. Not Null.
     * @throws GenieException on any non-runtime exception
     */
    void setJobManager(final JobManager jobManager) throws GenieException;

    /**
     * Set the process handle for this job.
     *
     * @param proc The process handle for the job. Not null.
     * @throws GenieException for any non-runtime exception
     */
    void setProcess(final Process proc) throws GenieException;

    /**
     * Set the working directory for this job.
     *
     * @param workingDir The working directory to use for this job
     */
    void setWorkingDir(final String workingDir);

}
