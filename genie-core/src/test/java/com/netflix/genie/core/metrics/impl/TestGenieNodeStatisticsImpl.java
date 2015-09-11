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
package com.netflix.genie.core.metrics.impl;

import com.netflix.genie.common.exceptions.GenieException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Test case for GenieNodeStatistics.
 *
 * @author skrishnan
 * @author tgianos
 */
public class TestGenieNodeStatisticsImpl {

    private GenieNodeStatisticsImpl stats;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.stats = new GenieNodeStatisticsImpl();
    }

    /**
     * Test the initialize method.
     */
    @Test
    public void testInitialize() {
        this.stats.initialize();
    }

    /**
     * Test the shutdown method.
     */
    @Test
    public void testShutdown() {
        this.stats.shutdown();
    }

    /**
     * Test the counter that increments 200 error codes (success).
     */
    @Test
    public void test2xxCounter() {
        // incr 2xx count twice
        this.stats.setGenie2xxCount(new AtomicLong(0));
        this.stats.incrGenie2xxCount();
        this.stats.incrGenie2xxCount();
        Assert.assertEquals(2L, this.stats.getGenie2xxCount().longValue());
    }

    /**
     * Test the counter that increments 400 error codes (~bad requests).
     */
    @Test
    public void test4xxCounter() {
        // incr 4xx count 4 times
        this.stats.setGenie4xxCount(new AtomicLong(0));
        this.stats.incrGenie4xxCount();
        this.stats.incrGenie4xxCount();
        this.stats.incrGenie4xxCount();
        this.stats.incrGenie4xxCount();
        Assert.assertEquals(4L, this.stats.getGenie4xxCount().longValue());
    }

    /**
     * Test the counter that increments 500 error codes (server errors).
     */
    @Test
    public void test5xxCounter() {
        // incr 5xx count 5 times
        this.stats.setGenie5xxCount(new AtomicLong(0));
        this.stats.incrGenie5xxCount();
        this.stats.incrGenie5xxCount();
        this.stats.incrGenie5xxCount();
        this.stats.incrGenie5xxCount();
        this.stats.incrGenie5xxCount();
        Assert.assertEquals(5L, this.stats.getGenie5xxCount().longValue());
    }

    /**
     * Test the counter that increments job submissions.
     */
    @Test
    public void testJobSubCounter() {
        // incr job submissions once
        this.stats.setGenieJobSubmissions(new AtomicLong(0));
        this.stats.incrGenieJobSubmissions();
        Assert.assertEquals(1L, this.stats.getGenieJobSubmissions().longValue());
    }

    /**
     * Test the counter that increments job failures.
     */
    @Test
    public void testFailedJobCounter() {
        // incr failed jobs once
        this.stats.setGenieFailedJobs(new AtomicLong(0));
        this.stats.incrGenieFailedJobs();
        Assert.assertEquals(1L, this.stats.getGenieFailedJobs().longValue());
    }

    /**
     * Test the counter that increments job kills.
     */
    @Test
    public void testKilledJobCounter() {
        // incr killed jobs twice
        this.stats.setGenieKilledJobs(new AtomicLong(0));
        this.stats.incrGenieKilledJobs();
        this.stats.incrGenieKilledJobs();
        Assert.assertEquals(2L, this.stats.getGenieKilledJobs().longValue());
    }

    /**
     * Test the counter that increments job success.
     */
    @Test
    public void testSuccessJobCounter() {
        // incr successful jobs thrice
        this.stats.setGenieSuccessfulJobs(new AtomicLong(0));
        this.stats.incrGenieSuccessfulJobs();
        this.stats.incrGenieSuccessfulJobs();
        this.stats.incrGenieSuccessfulJobs();
        Assert.assertEquals(3L, this.stats.getGenieSuccessfulJobs().longValue());
    }

    /**
     * Test the counter that sets running job.
     *
     * @throws InterruptedException If the process is interrupted
     * @throws GenieException For any problem
     */
    @Test
    public void testRunningJobs() throws InterruptedException, GenieException {
        this.stats.setGenieRunningJobs(0);
        Assert.assertEquals(0, this.stats.getGenieRunningJobs().intValue());
    }

    /**
     * Test the counter for successful emails.
     */
    @Test
    public void testSuccessfulEmailCountCounter() {
        Assert.assertEquals(0L, this.stats.getSuccessfulEmailSentCount().get());
        this.stats.incrSuccessfulEmailCount();
        Assert.assertEquals(1L, this.stats.getSuccessfulEmailSentCount().get());
    }

    /**
     * Test the counter for failed emails.
     */
    @Test
    public void testFailedEmailCountCounter() {
        Assert.assertEquals(0L, this.stats.getFailedEmailSentCount().get());
        this.stats.incrFailedEmailCount();
        Assert.assertEquals(1L, this.stats.getFailedEmailSentCount().get());
    }

    /**
     * Test the counter for forwarded jobs.
     */
    @Test
    public void testGenieForwardedJobsCounter() {
        Assert.assertEquals(0L, this.stats.getGenieForwardedJobs().get());
        this.stats.incrGenieForwardedJobs();
        Assert.assertEquals(1L, this.stats.getGenieForwardedJobs().get());

        final long count = 584L;
        this.stats.setGenieForwardedJobs(new AtomicLong(count));
        Assert.assertEquals(count, this.stats.getGenieForwardedJobs().get());
    }

    /**
     * Test the counter for Genie running jobs 0 to 15 minutes.
     */
    @Test
    public void testGenieRunningJobs0To15mCounter() {
        Assert.assertEquals(0, this.stats.getGenieRunningJobs0To15m().get());
        final int count = 532;
        this.stats.setGenieRunningJobs0To15m(count);
        Assert.assertEquals(count, this.stats.getGenieRunningJobs0To15m().get());
    }

    /**
     * Test the counter for Genie running jobs 15 minutes to 2 hours.
     */
    @Test
    public void testGenieRunningJobs15mTo2hCounter() {
        Assert.assertEquals(0, this.stats.getGenieRunningJobs15mTo2h().get());
        final int count = 532;
        this.stats.setGenieRunningJobs15mTo2h(count);
        Assert.assertEquals(count, this.stats.getGenieRunningJobs15mTo2h().get());
    }

    /**
     * Test the counter for Genie running jobs 2 to 8 hours.
     */
    @Test
    public void testGenieRunningJobs2hTo8hCounter() {
        Assert.assertEquals(0, this.stats.getGenieRunningJobs2hTo8h().get());
        final int count = 53554;
        this.stats.setGenieRunningJobs2hTo8h(count);
        Assert.assertEquals(count, this.stats.getGenieRunningJobs2hTo8h().get());
    }

    /**
     * Test the counter for Genie running jobs 8 hours plus.
     */
    @Test
    public void testGenieRunningJobs8hPlusCounter() {
        Assert.assertEquals(0, this.stats.getGenieRunningJobs8hPlus().get());
        final int count = 5524;
        this.stats.setGenieRunningJobs8hPlus(count);
        Assert.assertEquals(count, this.stats.getGenieRunningJobs8hPlus().get());
    }

    /**
     * Test the counter for job submission retries.
     */
    @Test
    public void testJobSubmissionRetryCounter() {
        Assert.assertEquals(0L, this.stats.getJobSubmissionRetryCount().get());
        this.stats.incrJobSubmissionRetryCount();
        Assert.assertEquals(1L, this.stats.getJobSubmissionRetryCount().get());
    }
}
