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

import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton class that implements all servo monitoring for Genie.
 *
 * @author skrishnan
 * @author tgianos
 */
public class GenieNodeStatisticsImpl implements GenieNodeStatistics {

    private static final Logger LOG = LoggerFactory.getLogger(GenieNodeStatisticsImpl.class);

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
    private final AtomicInteger genieRunningJobs = new AtomicInteger(0);

    @Monitor(name = "Running_Jobs_0_15m", type = DataSourceType.GAUGE)
    private final AtomicInteger genieRunningJobs0To15m = new AtomicInteger(0);

    @Monitor(name = "Running_Jobs_15m_2h", type = DataSourceType.GAUGE)
    private final AtomicInteger genieRunningJobs15mTo2h = new AtomicInteger(0);

    @Monitor(name = "Running_Jobs_2h_8h", type = DataSourceType.GAUGE)
    private final AtomicInteger genieRunningJobs2hTo8h = new AtomicInteger(0);

    @Monitor(name = "Running_Jobs_8h_plus", type = DataSourceType.GAUGE)
    private final AtomicInteger genieRunningJobs8hPlus = new AtomicInteger(0);

    @Monitor(name = "Successful_Email_Count", type = DataSourceType.COUNTER)
    private final AtomicLong successEmailCount = new AtomicLong(0);

    @Monitor(name = "Job_Submission_Retry_Count", type = DataSourceType.COUNTER)
    private final AtomicLong jobSubmissionRetryCount = new AtomicLong(0);

    @Monitor(name = "Failed_Email_Count", type = DataSourceType.COUNTER)
    private final AtomicLong failedEmailCount = new AtomicLong(0);

    /**
     * Initialize the object.
     */
    @PostConstruct
    public void initialize() {
        LOG.info("Registering Servo Monitor");
        //TODO: Too static no way to decouple
        Monitors.registerObject(this);
    }

    /**
     * Perform any pre destruction actions.
     */
    @PreDestroy
    public void shutdown() {
        LOG.info("Shutting down Servo monitor");
        //TODO: Too static no way to decouple
        Monitors.unregisterObject(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicLong getGenie2xxCount() {
        LOG.debug("called");
        return this.genie2xxCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrGenie2xxCount() {
        LOG.debug("called");
        this.genie2xxCount.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicLong getGenie4xxCount() {
        LOG.debug("called");
        return this.genie4xxCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrGenie4xxCount() {
        LOG.debug("called");
        this.genie4xxCount.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicLong getGenie5xxCount() {
        LOG.debug("called");
        return this.genie5xxCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrGenie5xxCount() {
        LOG.debug("called");
        this.genie5xxCount.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicLong getGenieJobSubmissions() {
        LOG.debug("called");
        return this.genieJobSubmissions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrGenieJobSubmissions() {
        LOG.debug("called");
        this.genieJobSubmissions.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicLong getSuccessfulEmailSentCount() {
        LOG.debug("called");
        return this.successEmailCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrSuccessfulEmailCount() {
        LOG.debug("called");
        this.successEmailCount.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicLong getFailedEmailSentCount() {
        LOG.debug("called");
        return this.failedEmailCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrFailedEmailCount() {
        LOG.debug("called");
        this.failedEmailCount.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicLong getGenieSuccessfulJobs() {
        LOG.debug("called");
        return this.genieSuccessfulJobs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrGenieSuccessfulJobs() {
        LOG.debug("called");
        this.genieSuccessfulJobs.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicLong getGenieFailedJobs() {
        LOG.debug("called");
        return this.genieFailedJobs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrGenieFailedJobs() {
        LOG.debug("called");
        this.genieFailedJobs.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicLong getGenieForwardedJobs() {
        LOG.debug("called");
        return this.genieForwardedJobs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrGenieForwardedJobs() {
        LOG.debug("called");
        this.genieForwardedJobs.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicLong getGenieKilledJobs() {
        LOG.debug("called");
        return this.genieKilledJobs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrGenieKilledJobs() {
        LOG.debug("called");
        this.genieKilledJobs.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenie2xxCount(final AtomicLong genie2xxCount) {
        this.genie2xxCount = genie2xxCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenie4xxCount(final AtomicLong genie4xxCount) {
        this.genie4xxCount = genie4xxCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenie5xxCount(final AtomicLong genie5xxCount) {
        this.genie5xxCount = genie5xxCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenieJobSubmissions(final AtomicLong genieJobSubmissions) {
        this.genieJobSubmissions = genieJobSubmissions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenieSuccessfulJobs(final AtomicLong genieSuccessfulJobs) {
        this.genieSuccessfulJobs = genieSuccessfulJobs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenieForwardedJobs(final AtomicLong genieForwardedJobs) {
        this.genieForwardedJobs = genieForwardedJobs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenieFailedJobs(final AtomicLong genieFailedJobs) {
        this.genieFailedJobs = genieFailedJobs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenieKilledJobs(final AtomicLong genieKilledJobs) {
        this.genieKilledJobs = genieKilledJobs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicInteger getGenieRunningJobs() {
        return this.genieRunningJobs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenieRunningJobs(final int genieRunningJobs) {
        this.genieRunningJobs.set(genieRunningJobs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicInteger getGenieRunningJobs0To15m() {
        return this.genieRunningJobs0To15m;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenieRunningJobs0To15m(final int genieRunningJobs0To15m) {
        this.genieRunningJobs0To15m.set(genieRunningJobs0To15m);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicInteger getGenieRunningJobs15mTo2h() {
        return this.genieRunningJobs15mTo2h;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenieRunningJobs15mTo2h(final int genieRunningJobs15mTo2h) {
        this.genieRunningJobs15mTo2h.set(genieRunningJobs15mTo2h);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicInteger getGenieRunningJobs2hTo8h() {
        return this.genieRunningJobs2hTo8h;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenieRunningJobs2hTo8h(final int genieRunningJobs2hTo8h) {
        this.genieRunningJobs2hTo8h.set(genieRunningJobs2hTo8h);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicInteger getGenieRunningJobs8hPlus() {
        return this.genieRunningJobs8hPlus;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGenieRunningJobs8hPlus(final int genieRunningJobs8hPlus) {
        this.genieRunningJobs8hPlus.set(genieRunningJobs8hPlus);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AtomicLong getJobSubmissionRetryCount() {
        LOG.debug("called");
        return this.jobSubmissionRetryCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrJobSubmissionRetryCount() {
        LOG.debug("called");
        this.jobSubmissionRetryCount.incrementAndGet();
    }
}
