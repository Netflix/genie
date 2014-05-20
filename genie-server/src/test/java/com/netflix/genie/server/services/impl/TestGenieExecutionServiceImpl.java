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

package com.netflix.genie.server.services.impl;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.genie.common.messages.JobStatusResponse;
import com.netflix.genie.common.model.JobElement;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.services.ExecutionService;

/**
 * Test for the Genie execution service class.
 *
 * @author skrishnan
 */
public class TestGenieExecutionServiceImpl {

    private static ExecutionService xs;

    /**
     * Initialize stats object before any tests are run.
     */
    @BeforeClass
    public static void init() {
        xs = new GenieExecutionServiceImpl();
    }

    /**
     * Test whether a job kill returns immediately for a finished job.
     *
     * @throws Exception if anything went wrong with the test.
     */
    @Test
    public void testOptimizedJobKill() throws Exception {
        // add a successful job with a bogus killURI
        PersistenceManager<JobElement> pm = new PersistenceManager<JobElement>();
        JobElement job = new JobElement();
        UUID uuid = UUID.randomUUID();
        job.setJobID(uuid.toString());
        job.setKillURI("http://DOES/NOT/EXIST");
        job.setStatus("SUCCEEDED");
        pm.createEntity(job);

        // should return immediately despite bogus killURI
        JobStatusResponse status = xs.killJob(job.getJobID());
        Assert.assertEquals(status.getStatus(), "SUCCEEDED");
    }

    /**
     * Shutdown after tests are complete.
     */
    @AfterClass
    public static void shutdown() {
        PersistenceManager.shutdown();
    }
}
