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

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.server.jobmanager.JobJanitor;
import com.netflix.genie.server.repository.jpa.JobRepository;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

/**
 * Test code for the job janitor class, which marks un-updated jobs as zombies.
 *
 * @author skrishnan
 * @author tgianos
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:application-test.xml")
@Transactional
public class TestJobJanitorImpl {

    private static final Logger LOG = LoggerFactory.getLogger(TestJobJanitorImpl.class);

    @Inject
    private JobRepository jobRepo;

    @Inject
    private JobJanitor janitor;

    /**
     * Test whether the janitor cleans up zombie jobs correctly.
     *
     * @throws CloudServiceException
     * @throws Exception
     */
    @Test
    public void testJobJanitor() throws CloudServiceException, Exception {
        final Set<String> criteriaTags = new HashSet<String>();
        criteriaTags.add("prod");
        final ClusterCriteria criteria = new ClusterCriteria(criteriaTags);
        final List<ClusterCriteria> criterias = new ArrayList<ClusterCriteria>();
        criterias.add(criteria);
        // create two old jobs
        Job one = new Job("someUser", "commandId", null, "someArg", criterias);
        one.setId(UUID.randomUUID().toString());
        one.setName("UPDATE_TEST");
        one.setStatus(JobStatus.RUNNING);
        one = this.jobRepo.save(one);
        Job two = new Job("someUser2", null, "commandName", "someArg2", criterias);
        two.setId(UUID.randomUUID().toString());
        two.setName("UPDATE_TEST");
        two.setStatus(JobStatus.INIT);
        two = this.jobRepo.save(two);

        // ensure that more than two jobs have been cleaned up
        int numRows = this.janitor.markZombies();
        LOG.info("Number of rows marked as zombies: " + numRows);

        // TODO: make the test work. Need to delay time or force update time older.
//        Assert.assertEquals(numRows >= 2, true);
    }
}
