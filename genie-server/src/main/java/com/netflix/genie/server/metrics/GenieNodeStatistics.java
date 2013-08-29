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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private JobCountMonitor jobCountMonitor;

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

    @Monitor(name = "Running_Jobs", type = DataSourceType.GAUGE)
    private AtomicInteger genieRunningJobs = new AtomicInteger(0);

    @Monitor(name = "Running_Jobs_0_15m", type = DataSourceType.GAUGE)
    private AtomicInteger genieRunningJobs0To15m = new AtomicInteger(0);

    @Monitor(name = "Running_Jobs_15m_2h", type = DataSourceType.GAUGE)
    private AtomicInteger genieRunningJobs15mTo2h = new AtomicInteger(0);

    @Monitor(name = "Running_Jobs_2h_8h", type = DataSourceType.GAUGE)
    private AtomicInteger genieRunningJobs2hTo8h = new AtomicInteger(0);

    @Monitor(name = "Running_Jobs_8h_plus", type = DataSourceType.GAUGE)
    private AtomicInteger genieRunningJobs8hPlus = new AtomicInteger(0);

    /**
     * Private constructor for singleton.
     */
    private GenieNodeStatistics() {
        jobCountMonitor = new JobCountMonitor(this);
        jobCountMonitor.setDaemon(true);
        jobCountMonitor.start();
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
     * A setter method for 2xx count.
     *
     * @param genie2xxCount long value to set
     */
    public void setGenie2xxCount(AtomicLong genie2xxCount) {
        this.genie2xxCount = genie2xxCount;
    }

    /**
     * A setter method for 4xx count.
     *
     * @param genie4xxCount long value to set
     */
    public void setGenie4xxCount(AtomicLong genie4xxCount) {
        this.genie4xxCount = genie4xxCount;
    }

    /**
     * A setter method for 5xx count.
     *
     * @param genie5xxCount long value to set
     */
    public void setGenie5xxCount(AtomicLong genie5xxCount) {
        this.genie5xxCount = genie5xxCount;
    }

    /**
     * A setter method for job submissions.
     *
     * @param genieJobSubmissions long value to set
     */
    public void setGenieJobSubmissions(AtomicLong genieJobSubmissions) {
        this.genieJobSubmissions = genieJobSubmissions;
    }

    /**
     * A setter method for successful jobs.
     *
     * @param genieSuccessfulJobs long value to set
     */
    public void setGenieSuccessfulJobs(AtomicLong genieSuccessfulJobs) {
        this.genieSuccessfulJobs = genieSuccessfulJobs;
    }

    /**
     * A setter method for forwarded jobs.
     *
     * @param genieForwardedJobs long value to set
     */
    public void setGenieForwardedJobs(AtomicLong genieForwardedJobs) {
        this.genieForwardedJobs = genieForwardedJobs;
    }

    /**
     * A setter method for failed jobs.
     *
     * @param genieFailedJobs long value to set
     */
    public void setGenieFailedJobs(AtomicLong genieFailedJobs) {
        this.genieFailedJobs = genieFailedJobs;
    }

    /**
     * A setter method for killed jobs.
     *
     * @param genieKilledJobs long value to set
     */
    public void setGenieKilledJobs(AtomicLong genieKilledJobs) {
        this.genieKilledJobs = genieKilledJobs;
    }

    /**
     * Get number of running jobs on this instance.
     *
     * @return number of running jobs on this instance
     */
    public AtomicInteger getGenieRunningJobs() {
        return genieRunningJobs;
    }

    /**
     * Set number of running jobs on this instance.
     *
     * @param genieRunningJobs number of running jobs on this instance
     */
    public void setGenieRunningJobs(int genieRunningJobs) {
        this.genieRunningJobs.set(genieRunningJobs);
    }

    /**
     * Get number of running jobs with runtime less than 15 mins.
     *
     * @return number of running jobs with runtime less than 15 mins
     */
    public AtomicInteger getGenieRunningJobs0To15m() {
        return genieRunningJobs0To15m;
    }

    /**
     * Set the number of running jobs with runtime less than 15 mins.
     *
     * @param genieRunningJobs0To15m number of running jobs with runtime less than 15 mins
     */
    public void setGenieRunningJobs0To15m(int genieRunningJobs0To15m) {
        this.genieRunningJobs0To15m.set(genieRunningJobs0To15m);
    }

    /**
     * Get the number of running jobs with runtime between 15 mins and 2 hours.
     *
     * @return number of running jobs with runtime between 15 mins and 2 hours
     */
    public AtomicInteger getGenieRunningJobs15mTo2h() {
        return genieRunningJobs15mTo2h;
    }

    /**
     * Set the number of running jobs with runtime between 15 mins and 2 hours.
     *
     * @param genieRunningJobs15mTo2h number of running jobs with runtime between
     *          15 mins and 2 hours
     */
    public void setGenieRunningJobs15mTo2h(int genieRunningJobs15mTo2h) {
        this.genieRunningJobs15mTo2h.set(genieRunningJobs15mTo2h);
    }

    /**
     * Get the number of running jobs with runtime between 2 to 8 hours.
     *
     * @return number of running jobs with runtime between 2 to 8 hours.
     */
    public AtomicInteger getGenieRunningJobs2hTo8h() {
        return genieRunningJobs2hTo8h;
    }

    /**
     * Set the number of running jobs with runtime between 2 to 8 hours.
     *
     * @param genieRunningJobs2hTo8h number of running jobs with runtime
     *          between 2 to 8 hours.
     */
    public void setGenieRunningJobs2hTo8h(int genieRunningJobs2hTo8h) {
        this.genieRunningJobs2hTo8h.set(genieRunningJobs2hTo8h);
    }

    /**
     * Get the number of running jobs with runtime greater than 8 hours.
     *
     * @return number of running jobs with runtime greater than 8 hours
     */
    public AtomicInteger getGenieRunningJobs8hPlus() {
        return genieRunningJobs8hPlus;
    }

    /**
     * Set the number of running jobs with runtime greater than 8 hours.
     *
     * @param genieRunningJobs8hPlus number of running jobs with runtime
     *          greater than 8 hours
     */
    public void setGenieRunningJobs8hPlus(int genieRunningJobs8hPlus) {
        this.genieRunningJobs8hPlus.set(genieRunningJobs8hPlus);
    }

    /**
     * Shut down cleanly.
     */
    public void shutdown() {
        logger.info("Shutting down Servo monitor");
        Monitors.unregisterObject(this);
        jobCountMonitor.setStop(true);
    }
}
