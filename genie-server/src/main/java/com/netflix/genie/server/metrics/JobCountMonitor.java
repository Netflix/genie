/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.server.metrics;

import com.netflix.genie.common.exceptions.CloudServiceException;

/**
 * Monitor thread that routinely updates the statistics object.
 *
 * @author skrishnan
 * @author tgianos
 */
public interface JobCountMonitor extends Runnable {

    /**
     * Get number of jobs running on this instance.
     *
     * @return number of running jobs
     * @throws CloudServiceException if there is any error
     */
    int getNumInstanceJobs() throws CloudServiceException;

    /**
     * Get number of running jobs on this instance running for > 15 mins.
     *
     * @return number of running jobs with runtime > 15 mins
     * @throws CloudServiceException if there is any error
     */
    int getNumInstanceJobs15Mins() throws CloudServiceException;

    /**
     * Get number of running jobs with 15m < runtime < 2 hours.
     *
     * @return Number of running jobs with 15m < runtime < 2 hours
     * @throws CloudServiceException if there is any error
     */
    int getNumInstanceJobs2Hrs() throws CloudServiceException;

    /**
     * Get number of running jobs with 2h < runtime < 8 hours.
     *
     * @return Number of running jobs with 2h < runtime < 8 hours
     * @throws CloudServiceException
     */
    int getNumInstanceJobs8Hrs() throws CloudServiceException;

    /**
     * Get number of running jobs with runtime > 8h.
     *
     * @return Number of running jobs with runtime > 8h
     * @throws CloudServiceException if there is any error
     */
    int getNumInstanceJobs8HrsPlus() throws CloudServiceException;

    /**
     * Tell the monitor thread to stop running at next iteration.
     *
     * @param stop true if the thread should stop running
     */
    void setStop(final boolean stop);
}
