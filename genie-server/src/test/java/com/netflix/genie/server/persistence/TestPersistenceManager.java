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
package com.netflix.genie.server.persistence;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types.JobStatus;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for the persistence manager.
 *
 * @author skrishnan
 */
public class TestPersistenceManager {

    private Set<ClusterCriteria> criterias;

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
        this.criterias = new HashSet<ClusterCriteria>();
        this.criterias.add(criteria);
    }

    /**
     * Test entity create and get after create.
     *
     * @throws CloudServiceException
     */
    @Test
    public void testCreateAndGetEntity() throws CloudServiceException {
        PersistenceManager<Job> pm = new PersistenceManager<Job>();
        Job initial = new Job("someUser", "commandId", null, "someArg", this.criterias);
        final String id = UUID.randomUUID().toString();
        initial.setName("My test job");
        initial.setId(id);
        pm.createEntity(initial);
        Job result = pm.getEntity(id, Job.class);
        Assert.assertEquals(initial.getId(), result.getId());
    }

    /**
     * Test updating single entity.
     *
     * @throws CloudServiceException
     */
    @Test
    public void testUpdateEntity() throws CloudServiceException {
        PersistenceManager<Job> pm = new PersistenceManager<Job>();
        Job initial = new Job("someUser", "commandId", null, "someArg", this.criterias);
        final String id = UUID.randomUUID().toString();
        initial.setId(id);
        initial.setUser("myUserName");
        initial.setCommandArgs("commandArg");
        pm.createEntity(initial);
        initial.setJobStatus(JobStatus.FAILED);
        Job updated = pm.updateEntity(initial);
        Assert.assertEquals(JobStatus.FAILED, updated.getStatus());
    }

    /**
     * Shutdown after tests are complete.
     */
    @AfterClass
    public static void shutdown() {
        PersistenceManager.shutdown();
    }
}
