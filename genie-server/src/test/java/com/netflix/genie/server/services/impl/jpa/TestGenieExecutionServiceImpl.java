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
package com.netflix.genie.server.services.impl.jpa;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.services.ExecutionService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for the Genie execution service class.
 *
 * @author skrishnan
 */
public class TestGenieExecutionServiceImpl {

    private static ExecutionService xs;
    private List<ClusterCriteria> criterias;

    /**
     * Initialize stats object before any tests are run.
     */
    @BeforeClass
    public static void init() {
        xs = new ExecutionServiceJPAImpl();
    }

    /**
     * Setup the tests.
     *
     * @throws CloudServiceException
     */
    @Before
    public void setup() throws CloudServiceException {
        final Set<String> criteriaTags = new HashSet<String>();
        criteriaTags.add("prod");
        final ClusterCriteria criteria = new ClusterCriteria(criteriaTags);
        this.criterias = new ArrayList<ClusterCriteria>();
        this.criterias.add(criteria);
    }

    /**
     * Test whether a job kill returns immediately for a finished job.
     *
     * @throws Exception if anything went wrong with the test.
     */
    @Test
    public void testOptimizedJobKill() throws Exception {
        // add a successful job with a bogus killURI
        PersistenceManager<Job> pm = new PersistenceManager<Job>();
        final Job job = new Job("someUser", "commandId", null, "someArg", this.criterias);
        job.setId(UUID.randomUUID().toString());
        job.setKillURI("http://DOES/NOT/EXIST");
        job.setStatus(JobStatus.SUCCEEDED);
        pm.createEntity(job);

        // should return immediately despite bogus killURI
        Job status = xs.killJob(job.getId());
        Assert.assertEquals(JobStatus.SUCCEEDED, status.getStatus());
    }

    /**
     * Shutdown after tests are complete.
     */
    @AfterClass
    public static void shutdown() {
        PersistenceManager.shutdown();
    }
}
