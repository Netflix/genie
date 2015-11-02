/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.web.tasks.impl;

import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.util.ProcessStatus;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.web.GenieWeb;
import com.netflix.genie.web.tasks.JobJanitor;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;

/**
 * Test code for the job janitor class, which marks un-updated jobs as zombies.
 *
 * @author tgianos
 */
@ActiveProfiles({"integration"})
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = GenieWeb.class)
@TestExecutionListeners({
        DependencyInjectionTestExecutionListener.class,
        DirtiesContextTestExecutionListener.class,
        TransactionalTestExecutionListener.class,
        DbUnitTestExecutionListener.class
})
@WebIntegrationTest(randomPort = true)
@Ignore
public class JobJanitorImplIntegrationTests {

    @Autowired
    private JobJanitor janitor;

    @Autowired
    private JpaJobRepository jpaJobRepository;

    /**
     * Test whether the janitor cleans up zombie jobs correctly.
     *
     * @throws Exception For any issue
     */
    @Test
    @DatabaseSetup("JobJanitorImplIntegrationTests/testMarkZombies/init.xml")
    @DatabaseTearDown("JobJanitorImplIntegrationTests/testMarkZombies/cleanup.xml")
    public void testMarkZombies() throws Exception {
        Assert.assertThat(this.jpaJobRepository.findOne("job1").getStatus(), Matchers.is(JobStatus.RUNNING));
        Assert.assertThat(this.jpaJobRepository.findOne("job2").getStatus(), Matchers.is(JobStatus.INIT));
        Assert.assertThat(this.jpaJobRepository.findOne("job3").getStatus(), Matchers.is(JobStatus.SUCCEEDED));
        this.janitor.markZombies();
        final JobEntity jobEntity1 = this.jpaJobRepository.findOne("job1");
        Assert.assertThat(jobEntity1.getStatus(), Matchers.is(JobStatus.FAILED));
        Assert.assertThat(jobEntity1.getExitCode(), Matchers.is(ProcessStatus.ZOMBIE_JOB.getExitCode()));
        Assert.assertThat(jobEntity1.getStatusMsg(), Matchers.is(ProcessStatus.ZOMBIE_JOB.getMessage()));
        final JobEntity jobEntity2 = this.jpaJobRepository.findOne("job2");
        Assert.assertThat(jobEntity2.getStatus(), Matchers.is(JobStatus.FAILED));
        Assert.assertThat(jobEntity2.getExitCode(), Matchers.is(ProcessStatus.ZOMBIE_JOB.getExitCode()));
        Assert.assertThat(jobEntity2.getStatusMsg(), Matchers.is(ProcessStatus.ZOMBIE_JOB.getMessage()));
        final JobEntity jobEntity3 = this.jpaJobRepository.findOne("job3");
        Assert.assertThat(jobEntity3.getStatus(), Matchers.is(JobStatus.SUCCEEDED));
        Assert.assertThat(jobEntity3.getExitCode(), Matchers.is(ProcessStatus.SUCCESS.getExitCode()));
    }
}
