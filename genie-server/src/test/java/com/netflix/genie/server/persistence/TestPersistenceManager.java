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
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.genie.common.model.JobInfoElement;
import com.netflix.genie.common.model.Types.JobStatus;

/**
 * Test case for the persistence manager.
 *
 * @author skrishnan
 */
public class TestPersistenceManager {

    /**
     * Initialize manager before any tests.
     */
    @BeforeClass
    public static void init() {
        PersistenceManager.init();
    }

    /**
     * Test entity create and get after create.
     */
    @Test
    public void testCreateAndGetEntity() {
        PersistenceManager<JobInfoElement> pm = new PersistenceManager<JobInfoElement>();
        JobInfoElement initial = new JobInfoElement();
        UUID uuid = UUID.randomUUID();
        initial.setJobName("My test job");
        initial.setJobID(uuid.toString());
        pm.createEntity(initial);
        JobInfoElement result = pm.getEntity(uuid.toString(),
                JobInfoElement.class);
        Assert.assertEquals(initial.getJobID(), result.getJobID());
    }

    /**
     * Test entity deletes.
     */
    @Test
    public void testDeleteEntity() {
        PersistenceManager<JobInfoElement> pm = new PersistenceManager<JobInfoElement>();
        JobInfoElement initial = new JobInfoElement();
        UUID uuid = UUID.randomUUID();
        initial.setJobID(uuid.toString());
        pm.createEntity(initial);
        JobInfoElement deleted = pm.deleteEntity(uuid.toString(),
                JobInfoElement.class);
        Assert.assertNotNull(deleted);
    }

    /**
     * Test updating single entity.
     */
    @Test
    public void testUpdateEntity() {
        PersistenceManager<JobInfoElement> pm = new PersistenceManager<JobInfoElement>();
        JobInfoElement initial = new JobInfoElement();
        UUID uuid = UUID.randomUUID();
        initial.setJobID(uuid.toString());
        pm.createEntity(initial);
        initial.setJobStatus(JobStatus.FAILED);
        JobInfoElement updated = pm.updateEntity(initial);
        Assert.assertEquals(updated.getStatus(), "FAILED");
    }

    /**
     * Test updating multiple entities.
     *
     * @throws Exception if there is anything wrong with the test
     */
    @Test
    public void testUpdateEntities() throws Exception {
        PersistenceManager<JobInfoElement> pm = new PersistenceManager<JobInfoElement>();
        JobInfoElement one = new JobInfoElement();
        one.setJobName("UPDATE_TEST");
        one.setJobID(UUID.randomUUID().toString());
        pm.createEntity(one);
        JobInfoElement two = new JobInfoElement();
        two.setJobName("UPDATE_TEST");
        two.setJobID(UUID.randomUUID().toString());
        pm.createEntity(two);
        ClauseBuilder setCriteria = new ClauseBuilder(ClauseBuilder.COMMA);
        setCriteria.append("jobName='TEST_UPDATE'");
        setCriteria.append("jobType='HADOOP'");
        ClauseBuilder queryCriteria = new ClauseBuilder(ClauseBuilder.AND);
        queryCriteria.append("jobName='UPDATE_TEST'");
        QueryBuilder qb = new QueryBuilder().table("JobInfoElement")
                .set(setCriteria.toString()).clause(queryCriteria.toString());
        int numRows = pm.update(qb);
        System.out.println("Number of rows updated: " + numRows);
        Assert.assertEquals(numRows > 0, true);
    }

    /**
     * Test select query.
     *
     * @throws Exception if there is any error in the select
     */
    @Test
    public void testQuery() throws Exception {
        PersistenceManager<JobInfoElement> pm = new PersistenceManager<JobInfoElement>();
        JobInfoElement initial = new JobInfoElement();
        UUID uuid = UUID.randomUUID();
        initial.setJobID(uuid.toString());
        initial.setJobName("My test job");
        initial.setJobStatus(JobStatus.FAILED);
        initial.setUpdateTime(System.currentTimeMillis());
        pm.createEntity(initial);
        ClauseBuilder cb = new ClauseBuilder(ClauseBuilder.AND);
        cb.append("jobID='" + initial.getJobID() + "'");
        cb.append("status='FAILED'");
        QueryBuilder qb = new QueryBuilder().table("JobInfoElement").clause(
                cb.toString());
        Object[] results = pm.query(qb);
        Assert.assertEquals(results.length, 1);
        Assert.assertEquals(results[0] instanceof JobInfoElement, true);
    }

    /**
     * Shutdown after tests are complete.
     */
    @AfterClass
    public static void shutdown() {
        PersistenceManager.shutdown();
    }
}
