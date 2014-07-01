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

import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.server.metrics.JobCountManager;
import com.netflix.genie.server.repository.jpa.JobRepository;
import com.netflix.genie.server.util.NetUtil;
import java.util.List;
import javax.inject.Inject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.transaction.annotation.Transactional;

/**
 * Basic tests for the JobCountManager.
 *
 * @author skrishnan
 * @author tgianos
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:application-test.xml")
@TestExecutionListeners({
    DependencyInjectionTestExecutionListener.class,
    DirtiesContextTestExecutionListener.class,
    TransactionalTestExecutionListener.class,
    DbUnitTestExecutionListener.class
})
@Transactional
public class TestJobCountManagerImpl {

    @Inject
    private JobRepository jobRepo;

    @Inject
    private JobCountManager manager;

    /**
     * Test getting number of running jobs on one instance.
     *
     * @throws CloudServiceException if there is any error during this test
     */
    @Test
    @DatabaseSetup("testNumInstanceJobs.xml")
    public void testNumInstanceJobs() throws CloudServiceException {
        //Force the hostname of the jobs to be the machine running the build
        final List<Job> jobs = this.jobRepo.findAll();
        for (final Job job : jobs) {
            job.setHostName(NetUtil.getHostName());
        }
        this.jobRepo.flush();

        Assert.assertTrue(2 == this.manager.getNumInstanceJobs());
        Assert.assertTrue(2 == this.manager.getNumInstanceJobs(0L, System.currentTimeMillis()));
        Assert.assertTrue(1 == this.manager.getNumInstanceJobs(1404257258340L, 1404257258341L));
        Assert.assertTrue(0 == this.manager.getNumInstanceJobs(0L, 0L));
    }
}
