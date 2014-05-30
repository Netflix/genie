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
     * Test entity deletes.
     */
    @Test
    public void testDeleteEntity() {
        PersistenceManager<Job> pm = new PersistenceManager<Job>();
        Job initial = new Job();
        UUID uuid = UUID.randomUUID();
        initial.setId(uuid.toString());
        initial.setUserName("myUserName");
        initial.setCmdArgs("commandArg");
        pm.createEntity(initial);
        Job deleted = pm.deleteEntity(uuid.toString(),
                Job.class);
        Assert.assertNotNull(deleted);
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
     * Test updating multiple entities.
     *
     * @throws Exception if there is anything wrong with the test
     */
    @Test
    public void testUpdateEntities() throws Exception {
        PersistenceManager<Job> pm = new PersistenceManager<Job>();
        Job one = new Job();
        one.setJobName("UPDATE_TEST");
        one.setId(UUID.randomUUID().toString());
        one.setUserName("myUserName");
        one.setCmdArgs("commandArg");
        pm.createEntity(one);
        Job two = new Job();
        two.setJobName("UPDATE_TEST");
        two.setId(UUID.randomUUID().toString());
        two.setUserName("myUserName2");
        two.setCmdArgs("commandArg2");
        pm.createEntity(two);
        ClauseBuilder setCriteria = new ClauseBuilder(ClauseBuilder.COMMA);
        setCriteria.append("jobName='TEST_UPDATE'");
        ClauseBuilder queryCriteria = new ClauseBuilder(ClauseBuilder.AND);
        queryCriteria.append("jobName='UPDATE_TEST'");
        QueryBuilder qb = new QueryBuilder().table("Job")
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
        PersistenceManager<Job> pm = new PersistenceManager<Job>();
        Job initial = new Job();
        UUID uuid = UUID.randomUUID();
        initial.setId(uuid.toString());
        initial.setJobName("My test job");
        initial.setJobStatus(JobStatus.FAILED);
        initial.setUserName("myUserName");
        initial.setCmdArgs("commandArg");
        pm.createEntity(initial);
        ClauseBuilder cb = new ClauseBuilder(ClauseBuilder.AND);
        cb.append("id='" + initial.getId() + "'");
        cb.append("userName='myUserName'");
        QueryBuilder qb = new QueryBuilder().table("Job").clause(
                cb.toString());
        Object[] results = pm.query(qb);
        Assert.assertEquals(1, results.length);
        Assert.assertEquals(results[0] instanceof Job, true);
    }

    /**
     * Shutdown after tests are complete.
     */
    @AfterClass
    public static void shutdown() {
        PersistenceManager.shutdown();
    }
}
