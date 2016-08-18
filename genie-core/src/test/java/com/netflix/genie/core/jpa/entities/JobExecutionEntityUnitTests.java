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
package com.netflix.genie.core.jpa.entities;

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Date;
import java.util.UUID;

/**
 * Unit tests for the JobExecutionEntity class.
 */
@Category(UnitTest.class)
public class JobExecutionEntityUnitTests {

    private static final String ID = UUID.randomUUID().toString();
    private JobExecutionEntity entity;

    /**
     * Setup the for each test.
     *
     * @throws GenieException on error
     */
    @Before
    public void setup() throws GenieException {
        this.entity = new JobExecutionEntity();
        this.entity.setId(ID);
    }

    /**
     * Test to make sure can successfully set the host name the job is running on.
     */
    @Test
    public void canSetHostName() {
        final String hostName = UUID.randomUUID().toString();
        this.entity.setHostName(hostName);
        Assert.assertThat(this.entity.getHostName(), Matchers.is(hostName));
    }

    /**
     * Test to make sure can successfully set the process id of the job.
     */
    @Test
    public void canSetProcessId() {
        final int processId = 12309834;
        this.entity.setProcessId(processId);
        Assert.assertThat(this.entity.getProcessId().orElseGet(RandomSuppliers.INT), Matchers.is(processId));
    }

    /**
     * Make sure setting the check delay time period works properly.
     */
    @Test
    public void canSetCheckDelay() {
        Assert.assertThat(this.entity.getCheckDelay(), Matchers.nullValue());
        final long newDelay = 1803234L;
        this.entity.setCheckDelay(newDelay);
        Assert.assertThat(this.entity.getCheckDelay(), Matchers.is(newDelay));
    }

    /**
     * Test to make sure can successfully set the process id of the job.
     */
    @Test
    public void canSetExitCode() {
        final int exitCode = 80072043;
        this.entity.setExitCode(exitCode);
        Assert.assertThat(this.entity.getExitCode().orElseGet(RandomSuppliers.INT), Matchers.is(exitCode));
    }

    /**
     * Test to make sure can successfully set the job this execution is for.
     *
     * @throws GenieException On error setting id
     */
    @Test
    public void canSetJob() throws GenieException {
        final JobEntity job = new JobEntity();
        job.setId(UUID.randomUUID().toString());
        this.entity.setJob(job);
        Assert.assertThat(this.entity.getJob(), Matchers.is(job));
    }

    /**
     * Make sure setting timeout works.
     */
    @Test
    public void canSetTimeout() {
        Assert.assertTrue(this.entity.getTimeout().isPresent());
        final Date timeout = new Date();
        this.entity.setTimeout(timeout);
        Assert.assertThat(this.entity.getTimeout().orElseGet(RandomSuppliers.DATE), Matchers.is(timeout));
    }

    /**
     * Test to make sure can generate valid DTO.
     *
     * @throws GenieException On serialization issues
     */
    @Test
    public void canGetDTO() throws GenieException {
        final String hostName = UUID.randomUUID().toString();
        this.entity.setHostName(hostName);
        final int processId = 29038;
        this.entity.setProcessId(processId);
        final long checkDelay = 1890347L;
        this.entity.setCheckDelay(checkDelay);
        final int exitCode = 2084390;
        this.entity.setExitCode(exitCode);
        final Date timeout = new Date();
        this.entity.setTimeout(timeout);

        final JobExecution execution = this.entity.getDTO();
        Assert.assertThat(execution.getId().orElseGet(RandomSuppliers.STRING), Matchers.is(ID));
        Assert.assertThat(
            execution.getCreated().orElseGet(RandomSuppliers.DATE),
            Matchers.is(this.entity.getCreated())
        );
        Assert.assertThat(
            execution.getUpdated().orElseGet(RandomSuppliers.DATE),
            Matchers.is(this.entity.getUpdated())
        );
        Assert.assertThat(execution.getExitCode().orElseGet(RandomSuppliers.INT), Matchers.is(exitCode));
        Assert.assertThat(execution.getHostName(), Matchers.is(hostName));
        Assert.assertThat(execution.getProcessId().orElseGet(RandomSuppliers.INT), Matchers.is(processId));
        Assert.assertThat(execution.getCheckDelay().orElseGet(RandomSuppliers.LONG), Matchers.is(checkDelay));
        Assert.assertThat(execution.getTimeout().orElseGet(RandomSuppliers.DATE), Matchers.is(timeout));
    }
}
