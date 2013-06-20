/*
 *
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.netflix.genie.server.metrics;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.genie.common.model.JobInfoElement;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.util.NetUtil;

/**
 * Basic tests for the JobCountManager.
 *
 * @author skrishnan
 */
public class TestJobCountManager {

    /**
     * Initialize persistence and job count managers before test.
     */
    @BeforeClass
    public static void init() {
        PersistenceManager.init();
        JobCountManager.init();
    }

    /**
     * Test getting number of running jobs on one instance.
     * @throws Exception if there is any error during this test
     */
    @Test
    public void testNumInstanceJobs() throws Exception {

        // setup
        PersistenceManager<JobInfoElement> pm = new PersistenceManager<JobInfoElement>();
        JobInfoElement job = new JobInfoElement();
        UUID uuid = UUID.randomUUID();
        job.setJobID(uuid.toString());
        job.setJobName("My test job");
        job.setStatus("RUNNING");
        job.setHostName(NetUtil.getHostName());
        job.setStartTime(1L);
        pm.createEntity(job);

        // number of running jobs - should be > 0
        int numJobs = JobCountManager.getNumInstanceJobs();
        Assert.assertEquals(numJobs > 0, true);

        // number of running jobs between 0 and now - should be > 0
        numJobs = JobCountManager.getNumInstanceJobs(0L,
                System.currentTimeMillis());
        Assert.assertEquals(numJobs > 0, true);

        // number of running jobs between 0 and 0 - should be none
        numJobs = JobCountManager.getNumInstanceJobs(0L, 0L);
        Assert.assertEquals(numJobs == 0, true);

        // cleanup
        job.setStatus("SUCCEEDED");
        pm.updateEntity(job);
    }

    /**
     * Shut down cleanly after test.
     */
    @AfterClass
    public static void shutdown() {
        PersistenceManager.shutdown();
    }
}
