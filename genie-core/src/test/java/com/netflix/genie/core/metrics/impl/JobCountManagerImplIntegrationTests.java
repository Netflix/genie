/*
 *
 * Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.core.metrics.impl;

import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.netflix.genie.GenieCoreTestApplication;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.metrics.JobCountManager;
import com.netflix.genie.test.categories.IntegrationTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.transaction.annotation.Transactional;

import java.util.Calendar;

/**
 * Basic tests for the JobCountManager.
 *
 * @author tgianos
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = GenieCoreTestApplication.class)
@TestExecutionListeners({
        DependencyInjectionTestExecutionListener.class,
        DirtiesContextTestExecutionListener.class,
        TransactionalTestExecutionListener.class,
        DbUnitTestExecutionListener.class
})
public class JobCountManagerImplIntegrationTests {

    @Autowired
    private JpaJobRepository jobRepo;

    @Autowired
    private JobCountManager manager;

    @Autowired
    private String hostname;

    /**
     * Test getting number of running jobs on one instance.
     *
     * @throws GenieException For any problem if there is any error during this test
     */
    @Ignore
    @Test
    @Transactional
    @DatabaseSetup("JobCountManagerImplIntegrationTests/init.xml")
    @DatabaseTearDown("JobCountManagerImplIntegrationTests/cleanup.xml")
    public void testNumInstanceJobs() throws GenieException {
        //Force the hostname of the jobs to be the machine running the build
        //TODO: Can we do this via spring and injection of properties?
        for (final JobEntity jobEntity : this.jobRepo.findAll()) {
//            jobEntity.setHostName(hostName);
        }
        this.jobRepo.flush();

        final Calendar one = Calendar.getInstance();
        one.clear();
        one.set(2014, Calendar.JULY, 1, 16, 27, 38);

        final Calendar two = Calendar.getInstance();
        two.clear();
        two.set(2014, Calendar.JULY, 1, 16, 27, 39);

        final Calendar three = Calendar.getInstance();
        three.clear();
        three.set(2014, Calendar.JULY, 1, 16, 27, 40);

        Assert.assertEquals(2, this.manager.getNumInstanceJobs());
        Assert.assertEquals(2,
                this.manager.getNumInstanceJobs(
                        0L,
                        System.currentTimeMillis()
                )
        );
        Assert.assertEquals(1,
                this.manager.getNumInstanceJobs(
                        one.getTimeInMillis(),
                        two.getTimeInMillis()
                )
        );
        Assert.assertEquals(1,
                this.manager.getNumInstanceJobs(
                        this.hostname,
                        two.getTimeInMillis(),
                        three.getTimeInMillis()
                )
        );
        Assert.assertEquals(0, this.manager.getNumInstanceJobs(0L, 0L));
    }
}
