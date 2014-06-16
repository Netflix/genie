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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.CloudServiceException;

/**
 * Monitor thread that routinely updates the statistics object.
 *
 * @author skrishnan
 */
public class JobCountMonitor extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(JobCountMonitor.class);

    private final GenieNodeStatistics stats;
    private boolean stop;

    /**
     * Constructor.
     *
     * @param stats reference to the statistics object that must be updated
     */
    public JobCountMonitor(GenieNodeStatistics stats) {
        this.stats = stats;
        this.stop = false;
    }

    /**
     * Get number of jobs running on this instance.
     *
     * @return number of running jobs
     * @throws CloudServiceException if there is any error
     */
    public int getNumInstanceJobs() throws CloudServiceException {
        LOG.debug("called");
        return JobCountManager.getNumInstanceJobs();
    }

    /**
     * Get number of running jobs on this instance running for > 15 mins.
     *
     * @return number of running jobs with runtime > 15 mins
     * @throws CloudServiceException if there is any error
     */
    public int getNumInstanceJobs15Mins() throws CloudServiceException {
        LOG.debug("called");
        long time = System.currentTimeMillis();
        return JobCountManager.getNumInstanceJobs(time - 15 * 60 * 1000, null);
    }

    /**
     * Get number of running jobs with 15m < runtime < 2 hours.
     *
     * @return Number of running jobs with 15m < runtime < 2 hours
     * @throws CloudServiceException if there is any error
     */
    public int getNumInstanceJobs2Hrs() throws CloudServiceException {
        LOG.debug("called");
        long time = System.currentTimeMillis();
        return JobCountManager.getNumInstanceJobs(time - 2 * 60 * 60 * 1000,
                time - 15 * 60 * 1000);
    }

    /**
     * Get number of running jobs with 2h < runtime < 8 hours.
     *
     * @return Number of running jobs with 2h < runtime < 8 hours
     * @throws CloudServiceException
     */
    public int getNumInstanceJobs8Hrs() throws CloudServiceException {
        LOG.debug("called");
        long time = System.currentTimeMillis();
        return JobCountManager.getNumInstanceJobs(time - 8 * 60 * 60 * 1000,
                time - 2 * 60 * 60 * 1000);
    }

    /**
     * Get number of running jobs with runtime > 8h.
     *
     * @return Number of running jobs with runtime > 8h
     * @throws CloudServiceException if there is any error
     */
    public int getNumInstanceJobs8HrsPlus() throws CloudServiceException {
        LOG.debug("called");
        long time = System.currentTimeMillis();
        return JobCountManager.getNumInstanceJobs(null, time - 8 * 60 * 60
                * 1000);
    }

    /**
     * The main run method for this thread - it runs for ever until explicitly
     * shutdown.
     */
    @Override
    public void run() {
        while (true) {
            try {
                LOG.info("JobCountMonitor daemon waking up");
                if (stop) {
                    LOG.info("JobCountMonitor stopping as per request");
                    return;
                }

                // set the metrics - check if thread is stopped at every point
                if (!stop) {
                    stats.setGenieRunningJobs(getNumInstanceJobs());
                }

                if (!stop) {
                    stats.setGenieRunningJobs0To15m(getNumInstanceJobs15Mins());
                }

                if (!stop) {
                    stats.setGenieRunningJobs15mTo2h(getNumInstanceJobs2Hrs());
                }

                if (!stop) {
                    stats.setGenieRunningJobs2hTo8h(getNumInstanceJobs8Hrs());
                }

                if (!stop) {
                    stats.setGenieRunningJobs8hPlus(getNumInstanceJobs8HrsPlus());
                }

                // sleep for the configured timeout
                if (!stop) {
                    long sleepTime = ConfigurationManager.
                            getConfigInstance().getLong("netflix.genie.server.metrics.sleep.ms", 30000);
                    LOG.info("JobCountMonitor daemon going to sleep");
                    Thread.sleep(sleepTime);
                }
            } catch (InterruptedException e) {
                // log error and move on
                LOG.warn("Interrupted exception caught", e);
            } catch (CloudServiceException e) {
                // log error and move on
                LOG.warn("Exception while setting number of running jobs", e);
            }
        }
    }

    /**
     * Tell the monitor thread to stop running at next iteration.
     *
     * @param stop true if the thread should stop running
     */
    public void setStop(boolean stop) {
        this.stop = stop;
    }
}
