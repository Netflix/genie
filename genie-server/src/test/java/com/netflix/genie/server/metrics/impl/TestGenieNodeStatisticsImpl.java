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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test case for GenieNodeStatistics.
 *
 * @author skrishnan
 * @author tgianos
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:genie-application-test.xml")
public class TestGenieNodeStatisticsImpl {

    @Inject
    private GenieNodeStatistics stats;

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
     * @throws InterruptedException
     * @throws GenieException
     */
    @Test
    public void testRunningJobs() throws InterruptedException, GenieException {
        this.stats.setGenieRunningJobs(0);
        Assert.assertEquals(0, this.stats.getGenieRunningJobs().intValue());
    }
}
