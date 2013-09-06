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

package com.netflix.genie.server.jobmanager.impl;

import java.util.UUID;

import junit.framework.Assert;

import org.junit.Test;

import com.netflix.genie.common.model.JobInfoElement;
import com.netflix.genie.server.persistence.PersistenceManager;

/**
 * Test code for the job janitor class, which marks un-updated jobs as zombies.
 *
 * @author skrishnan
 */
public class TestJobJanitor {

    /**
     * Test whether the janitor cleans up zombie jobs correctly.
     * @throws Exception if there is any error in cleaning up.
     */
    @Test
    public void testJobJanitor() throws Exception {
        // create two old jobs
        PersistenceManager<JobInfoElement> pm = new PersistenceManager<JobInfoElement>();
        JobInfoElement one = new JobInfoElement();
        one.setJobName("UPDATE_TEST");
        one.setJobID(UUID.randomUUID().toString());
        one.setUpdateTime(0L);
        one.setStatus("RUNNING");
        pm.createEntity(one);
        JobInfoElement two = new JobInfoElement();
        two.setJobName("UPDATE_TEST");
        two.setUpdateTime(0L);
        two.setStatus("INIT");
        two.setJobID(UUID.randomUUID().toString());
        pm.createEntity(two);

        // ensure that more than two jobs have been cleaned up
        JobJanitor janitor = new JobJanitor();
        int numRows = janitor.markZombies();
        System.out.println("Number of rows marked as zombies: " + numRows);
        Assert.assertEquals(numRows >= 2, true);
    }
}
