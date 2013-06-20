/*
 *
 *  Copyright 2013 Netflix, Inc.
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

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;

/**
 * Singleton class that implements all servo monitoring for Genie.
 *
 * @author skrishnan
 */
public final class GenieNodeStatistics {

    private static Logger logger = LoggerFactory
            .getLogger(GenieNodeStatistics.class);

    // static instance for singleton class
    private static volatile GenieNodeStatistics instance;

    @Monitor(name = "2xx_Count", type = DataSourceType.COUNTER)
    private AtomicLong genie2xxCount = new AtomicLong(0);

    @Monitor(name = "4xx_Count", type = DataSourceType.COUNTER)
    private AtomicLong genie4xxCount = new AtomicLong(0);

    @Monitor(name = "5xx_Count", type = DataSourceType.COUNTER)
    private AtomicLong genie5xxCount = new AtomicLong(0);

    @Monitor(name = "Submit_Jobs", type = DataSourceType.COUNTER)
    private AtomicLong genieJobSubmissions = new AtomicLong(0);

    @Monitor(name = "Successful_Jobs", type = DataSourceType.COUNTER)
    private AtomicLong genieSuccessfulJobs = new AtomicLong(0);

    @Monitor(name = "Forwarded_Jobs", type = DataSourceType.COUNTER)
    private AtomicLong genieForwardedJobs = new AtomicLong(0);

    @Monitor(name = "Failed_Jobs", type = DataSourceType.COUNTER)
    private AtomicLong genieFailedJobs = new AtomicLong(0);

    @Monitor(name = "Killed_Jobs", type = DataSourceType.COUNTER)
    private AtomicLong genieKilledJobs = new AtomicLong(0);

    /**
     * Get number of jobs running on this instance.
     *
     * @return number of running jobs
     * @throws CloudServiceException
     *             if there is any error
     */
    @Monitor(name = "Running_Jobs", type = DataSourceType.GAUGE)
    public int getNumInstanceJobs() throws CloudServiceException {
        logger.debug("called");
        return JobCountManager.getNumInstanceJobs();
    }

    /**
     * Get number of running jobs on this instance running for > 15 mins.
     *
     * @return number of running jobs with runtime > 15 mins
     * @throws CloudServiceException
     *             if there is any error
     */
    @Monitor(name = "Running_Jobs_0_15m", type = DataSourceType.GAUGE)
    public int getNumInstanceJobs15Mins() throws CloudServiceException {
        logger.debug("called");
        long time = System.currentTimeMillis();
        return JobCountManager.getNumInstanceJobs(time - 15 * 60 * 1000, null);
    }

    /**
     * Get number of running jobs with 15m < runtime < 2 hours.
     *
     * @return Number of running jobs with 15m < runtime < 2 hours
     * @throws CloudServiceException
     *             if there is any error
     */
    @Monitor(name = "Running_Jobs_15m_2h", type = DataSourceType.GAUGE)
    public int getNumInstanceJobs2Hrs() throws CloudServiceException {
        logger.debug("called");
        long time = System.currentTimeMillis();
        return JobCountManager.getNumInstanceJobs(time - 2 * 60 * 60 * 1000,
                time - 15 * 60 * 1000);
    }

    /**
     * Get number of running jobs with 2h < runtime < 8 hours.
     *
     * @return Number of running jobs with 2h < runtime < 8 hours
     * @throws CloudServiceException
     *             if there is any error
     */
    @Monitor(name = "Running_Jobs_2h_8h", type = DataSourceType.GAUGE)
    public int getNumInstanceJobs8Hrs() throws CloudServiceException {
        logger.debug("called");
        long time = System.currentTimeMillis();
        return JobCountManager.getNumInstanceJobs(time - 8 * 60 * 60 * 1000,
                time - 2 * 60 * 60 * 1000);
    }

    /**
     * Get number of running jobs with runtime > 8h.
     *
     * @return Number of running jobs with runtime > 8h
     * @throws CloudServiceException
     *             if there is any error
     */
    @Monitor(name = "Running_Jobs_8h_plus", type = DataSourceType.GAUGE)
    public int getNumInstanceJobs8HrsPlus() throws CloudServiceException {
        logger.debug("called");
        long time = System.currentTimeMillis();
        return JobCountManager.getNumInstanceJobs(null, time - 8 * 60 * 60
                * 1000);
    }

    /**
     * Private constructor for singleton.
     */
    private GenieNodeStatistics() {
    }

    /**
     * Register static instance with epic.
     */
    public static void init() {
        logger.debug("called");
        logger.info("Registering Servo Monitor");
        Monitors.registerObject(getInstance());
    }

    /**
     * Create instance if needed, and return it.
     *
     * @return singleton instance
     */
    public static synchronized GenieNodeStatistics getInstance() {
        logger.info("called");
        // create an instance if it hasn't been created already
        if (instance == null) {
            instance = new GenieNodeStatistics();
        }
        return instance;
    }

    /**
     * Get count of 2xx responses.
     *
     * @return count of 2xx responses
     */
    public AtomicLong getGenie2xxCount() {
        logger.debug("called");
        return genie2xxCount;
    }

    /**
     * Increment 2xx response count atomically.
     */
    public void incrGenie2xxCount() {
        logger.debug("called");
        genie2xxCount.incrementAndGet();
    }

    /**
     * Get count of 4xx responses.
     *
     * @return count of 4xx responses
     */
    public AtomicLong getGenie4xxCount() {
        logger.debug("called");
        return genie4xxCount;
    }

    /**
     * Increment 4xx response count atomically.
     */
    public void incrGenie4xxCount() {
        logger.debug("called");
        genie4xxCount.incrementAndGet();
    }

    /**
     * Get count of 5xx responses.
     *
     * @return count of 5xx responses
     */
    public AtomicLong getGenie5xxCount() {
        logger.debug("called");
        return genie5xxCount;
    }

    /**
     * Increment 5xx response count atomically.
     */
    public void incrGenie5xxCount() {
        logger.debug("called");
        genie5xxCount.incrementAndGet();
    }

    /**
     * Get number of job submissions on this instance.
     *
     * @return number of job submissions on this instance
     */
    public AtomicLong getGenieJobSubmissions() {
        logger.debug("called");
        return genieJobSubmissions;
    }

    /**
     * Increment job submissions on this instance atomically.
     */
    public void incrGenieJobSubmissions() {
        logger.debug("called");
        genieJobSubmissions.incrementAndGet();
    }

    /**
     * Get number of successful jobs on this instance.
     *
     * @return number of successful jobs on this instance
     */
    public AtomicLong getGenieSuccessfulJobs() {
        logger.debug("called");
        return genieSuccessfulJobs;
    }

    /**
     * Increment number of successful jobs atomically.
     */
    public void incrGenieSuccessfulJobs() {
        logger.debug("called");
        genieSuccessfulJobs.incrementAndGet();
    }

    /**
     * Get number of failed jobs on this instance.
     *
     * @return number of failed jobs on this instance
     */
    public AtomicLong getGenieFailedJobs() {
        logger.debug("called");
        return genieFailedJobs;
    }

    /**
     * Increment number of failed jobs atomically.
     */
    public void incrGenieFailedJobs() {
        logger.debug("called");
        genieFailedJobs.incrementAndGet();
    }

    /**
     * Get number of forwarded jobs from this instance.
     *
     * @return number of forwarded jobs from this instance
     */
    public AtomicLong getGenieForwardedJobs() {
        logger.debug("called");
        return genieForwardedJobs;
    }

    /**
     * Increment number of forwarded jobs atomically.
     */
    public void incrGenieForwardedJobs() {
        logger.debug("called");
        genieForwardedJobs.incrementAndGet();
    }

    /**
     * Get number of killed jobs on this instance.
     *
     * @return number of killed jobs on this instance
     */
    public AtomicLong getGenieKilledJobs() {
        logger.debug("called");
        return genieKilledJobs;
    }

    /**
     * Increment number of killed jobs atomically.
     */
    public void incrGenieKilledJobs() {
        logger.debug("called");
        genieKilledJobs.incrementAndGet();
    }

    /**
     * Shut down cleanly.
     */
    public void shutdown() {
        logger.info("Shutting down Servo monitor");
        Monitors.unregisterObject(this);
    }
}
