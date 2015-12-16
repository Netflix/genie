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

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for the JobExecutionEntity class.
 */
@Category(UnitTest.class)
public class JobExecutionEntityUnitTests {

    private JobExecutionEntity entity;

    /**
     * Setup the for each test.
     */
    @Before
    public void setup() {
        this.entity = new JobExecutionEntity();
    }

    /**
     * Test to make sure can successfully set the host name the job is running on.
     */
    @Test
    public void canSetHostName() {
        final String hostname = UUID.randomUUID().toString();
        this.entity.setHostName(hostname);
        Assert.assertThat(this.entity.getHostName(), Matchers.is(hostname));
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
     * Test to make sure the protected methods set values correctly.
     */
    @Test
    public void canSetClusterCriteria() {
        final String clusterCriteria
            = "[\"" + UUID.randomUUID().toString() + "\",\"" + UUID.randomUUID().toString() + "\"]";
        this.entity.setClusterCriteria(clusterCriteria);
        Assert.assertThat(this.entity.getClusterCriteria(), Matchers.is(clusterCriteria));
    }

    /**
     * Test to make sure can successfully set the cluster criteria from a set.
     *
     * @throws GenieException on serialization error to JSON
     */
    @Test
    public void canSetClusterCriteriaFromSet() throws GenieException {
        Assert.assertThat(this.entity.getClusterCriteria(), Matchers.is("[]"));
        final Set<String> criteria = Sets.newHashSet("one", "two", "three");
        this.entity.setClusterCriteriaFromSet(criteria);
        Assert.assertThat(this.entity.getClusterCriteria(), Matchers.notNullValue());
        Assert.assertTrue(this.entity.getClusterCriteria().contains("one"));
        Assert.assertTrue(this.entity.getClusterCriteria().contains("two"));
        Assert.assertTrue(this.entity.getClusterCriteria().contains("three"));
        Assert.assertThat(this.entity.getClusterCriteriaAsSet(), Matchers.is(criteria));

        this.entity.setClusterCriteriaFromSet(null);
        Assert.assertThat(this.entity.getClusterCriteria(), Matchers.is("[]"));
        Assert.assertThat(this.entity.getClusterCriteriaAsSet(), Matchers.empty());
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
        this.entity.setHostName(hostName);
        final int processId = 29038;
        this.entity.setProcessId(processId);
        final int exitCode = 2084390;
        this.entity.setExitCode(exitCode);
        final Set<String> clusterCriteria = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        this.entity.setClusterCriteriaFromSet(clusterCriteria);

        final JobExecution execution = this.entity.getDTO();
        Assert.assertThat(execution.getClusterCriteria(), Matchers.is(clusterCriteria));
        Assert.assertThat(execution.getExitCode(), Matchers.is(exitCode));
        Assert.assertThat(execution.getHostName(), Matchers.is(hostName));
        Assert.assertThat(execution.getProcessId(), Matchers.is(processId));
    }
}
