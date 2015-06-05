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
package com.netflix.genie.server.metrics.impl;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.genie.server.metrics.JobCountManager;
import com.netflix.genie.server.metrics.JobCountMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitor thread that routinely updates the statistics object.
 *
 * @author skrishnan
 */
public class JobCountMonitorImpl implements JobCountMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(JobCountMonitorImpl.class);

    private boolean stop;
    private final JobCountManager jobCountManager;
    private final GenieNodeStatistics stats;

    /**
     * Constructor.
     *
     * @param stats reference to the statistics object that must be updated
     * @param jobCountManager The job count manager
     */
    public JobCountMonitorImpl(final GenieNodeStatistics stats, final JobCountManager jobCountManager) {
        this.jobCountManager = jobCountManager;
        this.stats = stats;
        this.stop = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumInstanceJobs() throws GenieException {
        LOG.debug("called");
        return this.jobCountManager.getNumInstanceJobs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumInstanceJobs15Mins() throws GenieException {
        LOG.debug("called");
        long time = System.currentTimeMillis();
        return this.jobCountManager.getNumInstanceJobs(time - 15 * 60 * 1000, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumInstanceJobs2Hrs() throws GenieException {
        LOG.debug("called");
        long time = System.currentTimeMillis();
        return this.jobCountManager.getNumInstanceJobs(time - 2 * 60 * 60 * 1000,
                time - 15 * 60 * 1000);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumInstanceJobs8Hrs() throws GenieException {
        LOG.debug("called");
        long time = System.currentTimeMillis();
        return this.jobCountManager.getNumInstanceJobs(time - 8 * 60 * 60 * 1000,
                time - 2 * 60 * 60 * 1000);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumInstanceJobs8HrsPlus() throws GenieException {
        LOG.debug("called");
        long time = System.currentTimeMillis();
        return this.jobCountManager.getNumInstanceJobs(null, time - 8 * 60 * 60
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
                            getConfigInstance().getLong("com.netflix.genie.server.metrics.sleep.ms", 30000);
                    LOG.info("JobCountMonitor daemon going to sleep");
                    Thread.sleep(sleepTime);
                }
            } catch (final InterruptedException e) {
                // log error and move on
                LOG.warn("Interrupted exception caught", e);
            } catch (final GenieException e) {
                // log error and move on
                LOG.warn("Exception while setting number of running jobs", e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStop(boolean stop) {
        this.stop = stop;
    }
}
