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
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.validation.ConstraintViolationException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Test case for Job Status utility methods.
 *
 * @author amsharma
 * @author tgianos
 */
@Category(UnitTest.class)
public class JobEntityUnitTests extends EntityTestsBase {

    private static final String USER = "tgianos";
    private static final String NAME = "TomsJob";
    private static final String VERSION = "1.2.3";

    private JobEntity jobEntity;

    /**
     * Create a job object to be used in a bunch of tests.
     */
    @Before
    public void setup() {
        this.jobEntity = new JobEntity();
        this.jobEntity.setUser(USER);
        this.jobEntity.setName(NAME);
        this.jobEntity.setVersion(VERSION);
    }

    /**
     * Test the default constructor.
     */
    @Test
    public void testDefaultConstructor() {
        final JobEntity localJobEntity = new JobEntity();
        Assert.assertNull(localJobEntity.getId());
        Assert.assertEquals(JobEntity.DEFAULT_VERSION, localJobEntity.getVersion());
    }

    /**
     * Test the parameter based constructor.
     */
    @Test
    public void testConstructor() {
        Assert.assertNull(this.jobEntity.getId());
        Assert.assertEquals(NAME, this.jobEntity.getName());
        Assert.assertEquals(USER, this.jobEntity.getUser());
        Assert.assertEquals(VERSION, this.jobEntity.getVersion());
    }

    /**
     * Test the onCreateOrUpdateJob method which is called before saving
     * or updating.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testOnCreateOrUpdateJob() throws GeniePreconditionException {
        Assert.assertNull(this.jobEntity.getId());
        //Simulate the call stack JPA will make on persist
        this.jobEntity.onCreateBaseEntity();
        Assert.assertNotNull(this.jobEntity.getId());
        Assert.assertFalse(this.jobEntity.getTags().contains(
            CommonFields.GENIE_ID_TAG_NAMESPACE + this.jobEntity.getId())
        );
        Assert.assertFalse(this.jobEntity.getTags().contains(
            CommonFields.GENIE_NAME_TAG_NAMESPACE + this.jobEntity.getName())
        );
    }

    /**
     * Test the execution cluster name get/set.
     */
    @Test
    public void testSetGetClusterName() {
        Assert.assertNull(this.jobEntity.getClusterName());
        final String clusterName = UUID.randomUUID().toString();
        this.jobEntity.setClusterName(clusterName);
        Assert.assertEquals(clusterName, this.jobEntity.getClusterName());
    }

    /**
     * Test setter and getter for command name.
     */
    @Test
    public void testSetGetCommandName() {
        Assert.assertNull(this.jobEntity.getCommandName());
        final String commandName = UUID.randomUUID().toString();
        this.jobEntity.setCommandName(commandName);
        Assert.assertEquals(commandName, this.jobEntity.getCommandName());
    }

    /**
     * Test the setter and getter for status.
     */
    @Test
    public void testSetGetStatus() {
        Assert.assertNull(this.jobEntity.getStatus());
        this.jobEntity.setStatus(JobStatus.KILLED);
        Assert.assertEquals(JobStatus.KILLED, this.jobEntity.getStatus());
    }

    /**
     * Test the setter and getter for the status message.
     */
    @Test
    public void testSetGetStatusMsg() {
        Assert.assertNull(this.jobEntity.getStatusMsg());
        final String statusMsg = "Job is doing great";
        this.jobEntity.setStatusMsg(statusMsg);
        Assert.assertEquals(statusMsg, this.jobEntity.getStatusMsg());
    }

    /**
     * Test setter and getter for started.
     */
    @Test
    public void testSetGetStarted() {
        Assert.assertEquals(0L, this.jobEntity.getStarted().getTime());
        final Date started = new Date(123453L);
        this.jobEntity.setStarted(started);
        Assert.assertEquals(started.getTime(), this.jobEntity.getStarted().getTime());
    }

    /**
     * Test setter and getter for finished.
     */
    @Test
    public void testSetGetFinished() {
        Assert.assertEquals(0L, this.jobEntity.getFinished().getTime());
        final Date finished = new Date(123453L);
        this.jobEntity.setFinished(finished);
        Assert.assertEquals(finished.getTime(), this.jobEntity.getFinished().getTime());
    }

    /**
     * Test the setter and getter for archive location.
     */
    @Test
    public void testSetGetArchiveLocation() {
        Assert.assertNull(this.jobEntity.getArchiveLocation());
        final String archiveLocation = "s3://some/location";
        this.jobEntity.setArchiveLocation(archiveLocation);
        Assert.assertEquals(archiveLocation, this.jobEntity.getArchiveLocation());
    }

    /**
     * Tests whether a job status is updated correctly, and update time is
     * changed accordingly.
     */
    @Test
    public void testSetJobStatus() {
        final JobEntity localJobEntity = new JobEntity();
        final Date dt = new Date(0);

        // finish time is 0 on initialization
        Assert.assertTrue(dt.compareTo(localJobEntity.getFinished()) == 0);

        // start time is not zero on INIT, finish time is still 0
        localJobEntity.setJobStatus(JobStatus.INIT);
        Assert.assertNotNull(localJobEntity.getStarted());
        Assert.assertTrue(dt.compareTo(localJobEntity.getFinished()) == 0);

        // Shouldn't affect finish time
        localJobEntity.setJobStatus(JobStatus.RUNNING);
        Assert.assertTrue(dt.compareTo(localJobEntity.getFinished()) == 0);

        // finish time is non-zero on completion
        localJobEntity.setJobStatus(JobStatus.SUCCEEDED);
        Assert.assertFalse(dt.compareTo(localJobEntity.getFinished()) == 0);

        final JobEntity jobEntity2 = new JobEntity();
        // finish time is 0 on initialization
        Assert.assertTrue(dt.compareTo(jobEntity2.getFinished()) == 0);

        // start time is not zero on INIT, finish time is still 0
        final String initMessage = "We're initializing";
        jobEntity2.setJobStatus(JobStatus.INIT, initMessage);
        Assert.assertNotNull(jobEntity2.getStarted());
        Assert.assertEquals(initMessage, jobEntity2.getStatusMsg());
        Assert.assertTrue(dt.compareTo(jobEntity2.getStarted()) == -1);
        Assert.assertTrue(dt.compareTo(jobEntity2.getFinished()) == 0);

        // finish time is non-zero on completion
        final String successMessage = "Job Succeeded";
        jobEntity2.setJobStatus(com.netflix.genie.common.dto.JobStatus.SUCCEEDED, successMessage);
        Assert.assertEquals(successMessage, jobEntity2.getStatusMsg());
        Assert.assertFalse(dt.compareTo(jobEntity2.getFinished()) == 0);
    }

    /**
     * Test the setter and the getter for tags.
     */
    @Test
    public void testSetGetTags() {
        Assert.assertNotNull(this.jobEntity.getTags());
        Assert.assertTrue(this.jobEntity.getTags().isEmpty());
        final Set<String> tags = new HashSet<>();
        tags.add("someTag");
        tags.add("someOtherTag");
        this.jobEntity.setTags(tags);
        Assert.assertEquals(tags, this.jobEntity.getTags());

        this.jobEntity.setTags(null);
        Assert.assertThat(this.jobEntity.getTags(), Matchers.empty());
    }

    /**
     * Test Validate ok.
     */
    @Test
    public void testValidate() {
        this.validate(this.jobEntity);
    }

    /**
     * Test validate with exception from super class.
     *
     * @throws GenieException For any non-runtime issue
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateBadSuperClass() throws GenieException {
        this.validate(new JobEntity());
    }

    /**
     * Test to make sure can successfully set the job tags.
     */
    @Test
    public void canSetJobTags() {
        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        this.jobEntity.setJobTags(tags);
        Assert.assertThat(this.jobEntity.getJobTags(), Matchers.is(tags));
        Assert.assertThat(this.jobEntity.getTags(), Matchers.is(tags));
        Assert.assertThat(this.jobEntity.getSortedTags(), Matchers.notNullValue());

        this.jobEntity.setJobTags(null);
        Assert.assertThat(this.jobEntity.getJobTags(), Matchers.empty());
        Assert.assertThat(this.jobEntity.getTags(), Matchers.empty());
        Assert.assertThat(this.jobEntity.getSortedTags(), Matchers.nullValue());
    }

    /**
     * Test to make sure can successfully set the Job request that launched this job.
     */
    @Test
    public void canSetJobRequest() {
        final JobRequestEntity entity = new JobRequestEntity();
        this.jobEntity.setRequest(entity);
        Assert.assertThat(this.jobEntity.getRequest(), Matchers.is(entity));
    }

    /**
     * Test to make sure can successfully set the Job Execution for this job.
     */
    @Test
    public void canSetJobExecution() {
        final JobExecutionEntity entity = new JobExecutionEntity();
        this.jobEntity.setExecution(entity);
        Assert.assertThat(this.jobEntity.getExecution(), Matchers.is(entity));
    }

    /**
     * Test to make sure can successfully set the Cluster this job ran on.
     */
    @Test
    public void canSetCluster() {
        final ClusterEntity cluster = new ClusterEntity();
        final String clusterName = UUID.randomUUID().toString();
        cluster.setName(clusterName);
        Assert.assertThat(cluster.getJobs(), Matchers.empty());
        Assert.assertThat(this.jobEntity.getClusterName(), Matchers.nullValue());
        this.jobEntity.setCluster(cluster);
        Assert.assertThat(this.jobEntity.getCluster(), Matchers.is(cluster));
        Assert.assertThat(cluster.getJobs(), Matchers.contains(this.jobEntity));
        Assert.assertThat(this.jobEntity.getClusterName(), Matchers.is(clusterName));

        this.jobEntity.setCluster(null);
        Assert.assertThat(this.jobEntity.getCluster(), Matchers.nullValue());
        Assert.assertThat(cluster.getJobs(), Matchers.empty());
        Assert.assertThat(this.jobEntity.getClusterName(), Matchers.nullValue());
    }

    /**
     * Test to make sure can successfully set the Command this job ran used.
     */
    @Test
    public void canSetCommand() {
        final CommandEntity command = new CommandEntity();
        final String commandName = UUID.randomUUID().toString();
        command.setName(commandName);
        Assert.assertThat(command.getJobs(), Matchers.empty());
        Assert.assertThat(this.jobEntity.getCommandName(), Matchers.nullValue());
        this.jobEntity.setCommand(command);
        Assert.assertThat(this.jobEntity.getCommand(), Matchers.is(command));
        Assert.assertThat(command.getJobs(), Matchers.contains(this.jobEntity));
        Assert.assertThat(this.jobEntity.getCommandName(), Matchers.is(commandName));

        this.jobEntity.setCommand(null);
        Assert.assertThat(this.jobEntity.getCommand(), Matchers.nullValue());
        Assert.assertThat(command.getJobs(), Matchers.empty());
        Assert.assertThat(this.jobEntity.getCommandName(), Matchers.nullValue());
    }

    /**
     * Test to make sure can get a valid DTO from the job entity.
     *
     * @throws GenieException on error
     */
    @Test
    public void canGetDTO() throws GenieException {
        final JobEntity entity = new JobEntity();
        final String id = UUID.randomUUID().toString();
        entity.setId(id);
        final String name = UUID.randomUUID().toString();
        entity.setName(name);
        final String user = UUID.randomUUID().toString();
        entity.setUser(user);
        final String version = UUID.randomUUID().toString();
        entity.setVersion(version);
        final String clusterName = UUID.randomUUID().toString();
        entity.setClusterName(clusterName);
        final String commandName = UUID.randomUUID().toString();
        entity.setCommandName(commandName);
        final Date created = entity.getCreated();
        final Date updated = entity.getUpdated();
        final String description = UUID.randomUUID().toString();
        entity.setDescription(description);
        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        entity.setJobTags(tags);
        final String archiveLocation = UUID.randomUUID().toString();
        entity.setArchiveLocation(archiveLocation);
        final Date started = new Date();
        entity.setStarted(started);
        final Date finished = new Date();
        entity.setFinished(finished);
        entity.setStatus(JobStatus.SUCCEEDED);
        final String statusMessage = UUID.randomUUID().toString();
        entity.setStatusMsg(statusMessage);

        final Job job = entity.getDTO();
        Assert.assertThat(job.getId(), Matchers.is(id));
        Assert.assertThat(job.getName(), Matchers.is(name));
        Assert.assertThat(job.getUser(), Matchers.is(user));
        Assert.assertThat(job.getVersion(), Matchers.is(version));
        Assert.assertThat(job.getDescription(), Matchers.is(description));
        Assert.assertThat(job.getCreated(), Matchers.is(created));
        Assert.assertThat(job.getUpdated(), Matchers.is(updated));
        Assert.assertThat(job.getClusterName(), Matchers.is(clusterName));
        Assert.assertThat(job.getCommandName(), Matchers.is(commandName));
        Assert.assertThat(job.getTags(), Matchers.is(tags));
        Assert.assertThat(job.getArchiveLocation(), Matchers.is(archiveLocation));
        Assert.assertThat(job.getStarted(), Matchers.is(started));
        Assert.assertThat(job.getFinished(), Matchers.is(finished));
        Assert.assertThat(job.getStatus(), Matchers.is(JobStatus.SUCCEEDED));
        Assert.assertThat(job.getStatusMsg(), Matchers.is(statusMessage));
    }
}
