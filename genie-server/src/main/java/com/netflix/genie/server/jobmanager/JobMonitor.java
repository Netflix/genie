/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.netflix.genie.server.jobmanager;

import com.netflix.genie.common.exceptions.CloudServiceException;
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
     * @throws CloudServiceException
     */
    void setJob(final Job job) throws CloudServiceException;

    /**
     * Set the job manager for this monitor to use.
     *
     * @param jobManager The job manager to use. Not Null.
     * @throws CloudServiceException
     */
    void setJobManager(final JobManager jobManager) throws CloudServiceException;

    /**
     * Set the process handle for this job.
     *
     * @param proc The process handle for the job. Not null.
     * @throws CloudServiceException
     */
    void setProcess(final Process proc) throws CloudServiceException;

    /**
     * Set the working directory for this job.
     *
     * @param workingDir The working directory to use for this job
     */
    void setWorkingDir(final String workingDir);

}
