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
package com.netflix.genie.server.jobmanager.impl;

import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.server.persistence.PersistenceManager;
import java.util.UUID;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test code for the job janitor class, which marks un-updated jobs as zombies.
 *
 * @author skrishnan
 * @author tgianos
 */
public class TestJobJanitor {

    private static final Logger LOG = LoggerFactory.getLogger(TestJobJanitor.class);

    /**
     * Test whether the janitor cleans up zombie jobs correctly.
     *
     * @throws Exception if there is any error in cleaning up.
     */
    @Test
    public void testJobJanitor() throws Exception {
        // create two old jobs
        PersistenceManager<Job> pm = new PersistenceManager<Job>();
        Job one = new Job();
        one.setId(UUID.randomUUID().toString());
        one.setJobName("UPDATE_TEST");
        one.setStatus(JobStatus.RUNNING);
        one.setUserName("someUser");
        one.setCmdArgs("someArgs");
        pm.createEntity(one);
        Job two = new Job();
        two.setId(UUID.randomUUID().toString());
        two.setJobName("UPDATE_TEST");
        two.setStatus(JobStatus.INIT);
        two.setUserName("some other user name");
        two.setCmdArgs("someArgs2");
        pm.createEntity(two);

        // ensure that more than two jobs have been cleaned up
        JobJanitor janitor = new JobJanitor();
        int numRows = janitor.markZombies();
        LOG.info("Number of rows marked as zombies: " + numRows);

        // TODO: make the test work. Need to delay time or force update time older.
//        Assert.assertEquals(numRows >= 2, true);
        // shut down cleanly
        PersistenceManager.shutdown();
    }
}
