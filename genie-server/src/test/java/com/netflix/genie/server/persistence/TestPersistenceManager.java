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

package com.netflix.genie.server.persistence;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types.JobStatus;

/**
 * Test case for the persistence manager.
 *
 * @author skrishnan
 */
public class TestPersistenceManager {

    /**
     * Test entity create and get after create.
     */
    @Test
    public void testCreateAndGetEntity() {
        PersistenceManager<Job> pm = new PersistenceManager<Job>();
        Job initial = new Job();
        UUID uuid = UUID.randomUUID();
        initial.setJobName("My test job");
        initial.setId(uuid.toString());
        initial.setUserName("myUserName");
        initial.setCmdArgs("commandArg");
        pm.createEntity(initial);
        Job result = pm.getEntity(uuid.toString(),
                Job.class);
        Assert.assertEquals(initial.getId(), result.getId());
    }

    /**
     * Test updating single entity.
     */
    @Test
    public void testUpdateEntity() {
        PersistenceManager<Job> pm = new PersistenceManager<Job>();
        Job initial = new Job();
        UUID uuid = UUID.randomUUID();
        initial.setId(uuid.toString());
        initial.setUserName("myUserName");
        initial.setCmdArgs("commandArg");
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
