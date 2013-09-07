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

import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.server.persistence.PersistenceManager;

/**
 * Test case for GenieNodeStatistics.
 *
 * @author skrishnan
 */
public class TestGenieNodeStatistics {

    private static GenieNodeStatistics stats;

    /**
     * Initialize stats object before any tests are run.
     */
    @BeforeClass
    public static void init() {
        stats = GenieNodeStatistics.getInstance();
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
        Assert.assertEquals(stats.getGenie2xxCount().longValue(), 2L);
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
        Assert.assertEquals(stats.getGenie4xxCount().longValue(), 4L);
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
        Assert.assertEquals(stats.getGenie5xxCount().longValue(), 5L);
    }

    /**
     * Test the counter that increments job submissions.
     */
    @Test
    public void testJobSubCounter() {
        // incr job submissions once
        stats.setGenieJobSubmissions(new AtomicLong(0));
        stats.incrGenieJobSubmissions();
        Assert.assertEquals(stats.getGenieJobSubmissions().longValue(), 1L);
    }

    /**
     * Test the counter that increments job failures.
     */
    @Test
    public void testFailedJobCounter() {
        // incr failed jobs once
        stats.setGenieFailedJobs(new AtomicLong(0));
        stats.incrGenieFailedJobs();
        Assert.assertEquals(stats.getGenieFailedJobs().longValue(), 1L);
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
        Assert.assertEquals(stats.getGenieKilledJobs().longValue(), 2L);
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
        Assert.assertEquals(stats.getGenieSuccessfulJobs().longValue(), 3L);
    }

    /**
     * Test the counter and daemon thread that sets running job.
     *
     * @throws InterruptedException
     * @throws CloudServiceException
     */
    @Test
    public void testRunningJobs() throws InterruptedException, CloudServiceException {
        // get number of running jobs before we get started
        int numRunningJobs = JobCountManager.getNumInstanceJobs();
        stats.setGenieRunningJobs(0);
        Assert.assertEquals(stats.getGenieRunningJobs().intValue(), 0);
        ConfigurationManager.getConfigInstance().setProperty("netflix.genie.server.metrics.sleep.ms",
                 new Long(2000));
        stats.setGenieRunningJobs(5);
        Assert.assertEquals(stats.getGenieRunningJobs().intValue(), 5);
        // sleep for a while - number of running jobs should be same as what we started with
        Thread.sleep(30000);
        Assert.assertEquals(numRunningJobs, stats.getGenieRunningJobs().intValue());
    }

    /**
     * Shutdown stats object after test is done.
     */
    @AfterClass
    public static void shutdown() {
        // shut down cleanly
        stats.shutdown();
        PersistenceManager.shutdown();
    }
}
