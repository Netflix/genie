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
package com.netflix.genie.server.metrics.impl;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for GenieNodeStatistics.
 *
 * @author skrishnan
 * @author tgianos
 */
public class TestGenieNodeStatisticsImpl {

    private GenieNodeStatistics stats;

    /**
     * Initialize stats object before any tests are run.
     */
    @Before
    public void setup() {
        this.stats = new GenieNodeStatisticsImpl();
        this.stats.register();
    }

    /**
     * Shutdown stats object after test is done.
     */
    @After
    public void tearDown() {
        // shut down cleanly
        this.stats.shutdown();
    }

    /**
     * Test the counter that increments 200 error codes (success).
     */
    @Test
    public void test2xxCounter() {
        // incr 2xx count twice
        stats.setGenie2xxCount(new AtomicLong(0));
        stats.incrGenie2xxCount();
        stats.incrGenie2xxCount();
        System.out.println("Received: " + stats.getGenie2xxCount().longValue());
        Assert.assertEquals(2L, stats.getGenie2xxCount().longValue());
    }

    /**
     * Test the counter that increments 400 error codes (~bad requests).
     */
    @Test
    public void test4xxCounter() {
        // incr 4xx count 4 times
        stats.setGenie4xxCount(new AtomicLong(0));
        stats.incrGenie4xxCount();
        stats.incrGenie4xxCount();
        stats.incrGenie4xxCount();
        stats.incrGenie4xxCount();
        System.out.println("Received: " + stats.getGenie4xxCount().longValue());
        Assert.assertEquals(4L, stats.getGenie4xxCount().longValue());
    }

    /**
     * Test the counter that increments 500 error codes (server errors).
     */
    @Test
    public void test5xxCounter() {
        // incr 5xx count 5 times
        stats.setGenie5xxCount(new AtomicLong(0));
        stats.incrGenie5xxCount();
        stats.incrGenie5xxCount();
        stats.incrGenie5xxCount();
        stats.incrGenie5xxCount();
        stats.incrGenie5xxCount();
        System.out.println("Received: " + stats.getGenie5xxCount().longValue());
        Assert.assertEquals(5L, stats.getGenie5xxCount().longValue());
    }

    /**
     * Test the counter that increments job submissions.
     */
    @Test
    public void testJobSubCounter() {
        // incr job submissions once
        stats.setGenieJobSubmissions(new AtomicLong(0));
        stats.incrGenieJobSubmissions();
        Assert.assertEquals(1L, stats.getGenieJobSubmissions().longValue());
    }

    /**
     * Test the counter that increments job failures.
     */
    @Test
    public void testFailedJobCounter() {
        // incr failed jobs once
        stats.setGenieFailedJobs(new AtomicLong(0));
        stats.incrGenieFailedJobs();
        Assert.assertEquals(1L, stats.getGenieFailedJobs().longValue());
    }

    /**
     * Test the counter that increments job kills.
     */
    @Test
    public void testKilledJobCounter() {
        // incr killed jobs twice
        stats.setGenieKilledJobs(new AtomicLong(0));
        stats.incrGenieKilledJobs();
        stats.incrGenieKilledJobs();
        Assert.assertEquals(2L, stats.getGenieKilledJobs().longValue());
    }

    /**
     * Test the counter that increments job success.
     */
    @Test
    public void testSuccessJobCounter() {
        // incr successful jobs thrice
        stats.setGenieSuccessfulJobs(new AtomicLong(0));
        stats.incrGenieSuccessfulJobs();
        stats.incrGenieSuccessfulJobs();
        stats.incrGenieSuccessfulJobs();
        Assert.assertEquals(3L, stats.getGenieSuccessfulJobs().longValue());
    }

    /**
     * Test the counter that sets running job.
     *
     * @throws InterruptedException
     * @throws CloudServiceException
     */
    @Test
    public void testRunningJobs() throws InterruptedException, CloudServiceException {
        stats.setGenieRunningJobs(0);
        Assert.assertEquals(0, stats.getGenieRunningJobs().intValue());
    }
}
