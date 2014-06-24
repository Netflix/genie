/*
 *
 * Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.server.metrics.impl;

import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.server.metrics.JobCountManager;
import com.netflix.genie.server.repository.JobRepository;
import com.netflix.genie.server.util.NetUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

/**
 * Basic tests for the JobCountManager.
 *
 * @author skrishnan
 * @author tgianos
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:application-test.xml")
@Transactional
public class TestJobCountManagerImpl {
    
    @Inject
    private JobRepository jobRepo;
    
    private JobCountManager manager;
    
    @Before
    public void setup() {
        this.manager = new JobCountManagerImpl();
    }

    /**
     * Test getting number of running jobs on one instance.
     *
     * @throws Exception if there is any error during this test
     */
    @Test
    public void testNumInstanceJobs() throws Exception {

        // setup
        final Set<String> criteriaTags = new HashSet<String>();
        criteriaTags.add("prod");
        final ClusterCriteria criteria = new ClusterCriteria(criteriaTags);
        final List<ClusterCriteria> criterias = new ArrayList<ClusterCriteria>();
        criterias.add(criteria);
        Job job = new Job("someUser", "commandId", null, "someArg", criterias);
        job.setId(UUID.randomUUID().toString());
        job.setName("My test job");
        job.setStatus(JobStatus.RUNNING);
        job.setHostName(NetUtil.getHostName());
        job.setStartTime(1L);
        job = this.jobRepo.save(job);

        // number of running jobs - should be > 0
        Assert.assertTrue(0 < this.manager.getNumInstanceJobs());

        // number of running jobs between 0 and now - should be > 0
        Assert.assertTrue(0 < this.manager.getNumInstanceJobs(0L, System.currentTimeMillis()));

        // number of running jobs between 0 and 0 - should be none
        Assert.assertTrue(0 == this.manager.getNumInstanceJobs(0L, 0L));
    }
}
