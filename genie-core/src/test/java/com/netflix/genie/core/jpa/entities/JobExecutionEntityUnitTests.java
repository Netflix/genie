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
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
        final String hostname = UUID.randomUUID().toString();
        this.entity.setHostname(hostname);
        Assert.assertThat(this.entity.getHostname(), Matchers.is(hostname));
    }

    /**
     * Test to make sure can successfully set the process id of the job.
     */
    @Test
    public void canSetProcessId() {
        final int processId = 12309834;
        this.entity.setProcessId(processId);
        Assert.assertThat(this.entity.getProcessId(), Matchers.is(processId));
    }

    /**
     * Test to make sure can successfully set the process id of the job.
     */
    @Test
    public void canSetExitCode() {
        final int exitCode = 80072043;
        this.entity.setExitCode(exitCode);
        Assert.assertThat(this.entity.getExitCode(), Matchers.is(exitCode));
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
     * Test to make sure can generate valid DTO.
     *
     * @throws GenieException On serialization issues
     */
    @Test
    public void canGetDTO() throws GenieException {
        final String hostName = UUID.randomUUID().toString();
        this.entity.setHostname(hostName);
        final int processId = 29038;
        this.entity.setProcessId(processId);
        final int exitCode = 2084390;
        this.entity.setExitCode(exitCode);

        final JobExecution execution = this.entity.getDTO();
        Assert.assertThat(execution.getId(), Matchers.is(ID));
        Assert.assertThat(execution.getCreated(), Matchers.is(this.entity.getCreated()));
        Assert.assertThat(execution.getUpdated(), Matchers.is(this.entity.getUpdated()));
        Assert.assertThat(execution.getExitCode(), Matchers.is(exitCode));
        Assert.assertThat(execution.getHostname(), Matchers.is(hostName));
        Assert.assertThat(execution.getProcessId(), Matchers.is(processId));
    }
}
