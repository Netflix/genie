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
package com.netflix.genie.core.jobmanager;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.exceptions.GenieException;

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

    /**
     * Set the amount of time, in milliseconds, to wait in between checks to the process for completion.
     *
     * @param sleepTime The time to wait in milliseconds
     * @throws GenieException if precondition of sleepTime greater than or equal to 1 isn't met.
     */
    void setThreadSleepTime(final int sleepTime) throws GenieException;
}
