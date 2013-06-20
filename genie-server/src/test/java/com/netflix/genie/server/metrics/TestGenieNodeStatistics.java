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

import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
        stats.incrGenieJobSubmissions();
        Assert.assertEquals(stats.getGenieJobSubmissions().longValue(), 1L);
    }

    /**
     * Test the counter that increments job failures.
     */
    @Test
    public void testFailedJobCounter() {
        // incr failed jobs once
        stats.incrGenieFailedJobs();
        Assert.assertEquals(stats.getGenieFailedJobs().longValue(), 1L);
    }

    /**
     * Test the counter that increments job kills.
     */
    @Test
    public void testKilledJobCounter() {
        // incr killed jobs twice
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
        stats.incrGenieSuccessfulJobs();
        stats.incrGenieSuccessfulJobs();
        stats.incrGenieSuccessfulJobs();
        Assert.assertEquals(stats.getGenieSuccessfulJobs().longValue(), 3L);
    }

    /**
     * Shutdown stats object after test is done.
     */
    @AfterClass
    public static void shutdown() {
        // shut down cleanly
        stats.shutdown();
    }
}
