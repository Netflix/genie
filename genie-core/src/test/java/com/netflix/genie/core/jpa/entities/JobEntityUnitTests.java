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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.validation.ConstraintViolationException;
import java.util.Date;
import java.util.List;
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
            CommonFieldsEntity.GENIE_ID_TAG_NAMESPACE + this.jobEntity.getId())
        );
        Assert.assertFalse(this.jobEntity.getTags().contains(
            CommonFieldsEntity.GENIE_NAME_TAG_NAMESPACE + this.jobEntity.getName())
        );
    }

    /**
     * Test the execution cluster name get/set.
     */
    @Test
    public void testSetGetClusterName() {
        Assert.assertFalse(this.jobEntity.getClusterName().isPresent());
        final String clusterName = UUID.randomUUID().toString();
        this.jobEntity.setClusterName(clusterName);
        final String actualClusterName = this.jobEntity.getClusterName().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualClusterName, Matchers.is(clusterName));
    }

    /**
     * Test setter and getter for command name.
     */
    @Test
    public void testSetGetCommandName() {
        Assert.assertFalse(this.jobEntity.getCommandName().isPresent());
        final String commandName = UUID.randomUUID().toString();
        this.jobEntity.setCommandName(commandName);
        final String actualCommandName = this.jobEntity.getCommandName().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualCommandName, Matchers.is(commandName));
    }

    /**
     * Make sure the setter and getter for command args works properly.
     */
    @Test
    public void testSetGetCommandArgs() {
        Assert.assertNull(this.jobEntity.getCommandArgs());
        final String commandArgs = UUID.randomUUID().toString();
        this.jobEntity.setCommandArgs(commandArgs);
        Assert.assertThat(this.jobEntity.getCommandArgs(), Matchers.is(commandArgs));
    }

    /**
     * Test the setter and getter for status.
     */
    @Test
    public void testSetGetStatus() {
        Assert.assertThat(this.jobEntity.getStatus(), Matchers.is(JobStatus.INIT));
        this.jobEntity.setStatus(JobStatus.KILLED);
        Assert.assertEquals(JobStatus.KILLED, this.jobEntity.getStatus());
    }

    /**
     * Test the setter and getter for the status message.
     */
    @Test
    public void testSetGetStatusMsg() {
        Assert.assertFalse(this.jobEntity.getStatusMsg().isPresent());
        final String statusMsg = "Job is doing great";
        this.jobEntity.setStatusMsg(statusMsg);
        final String actualStatusMsg = this.jobEntity.getStatusMsg().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualStatusMsg, Matchers.is(statusMsg));
    }

    /**
     * Test setter and getter for started.
     */
    @Test
    public void testSetGetStarted() {
        Assert.assertFalse(this.jobEntity.getStarted().isPresent());
        final Date started = new Date(123453L);
        this.jobEntity.setStarted(started);
        final Date actualStarted = this.jobEntity.getStarted().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualStarted.getTime(), Matchers.is(started.getTime()));
        this.jobEntity.setStarted(null);
        Assert.assertFalse(this.jobEntity.getStarted().isPresent());
    }

    /**
     * Test setter and getter for finished.
     */
    @Test
    public void testSetGetFinished() {
        Assert.assertFalse(this.jobEntity.getFinished().isPresent());
        final Date finished = new Date(123453L);
        this.jobEntity.setFinished(finished);
        final Date actualFinished = this.jobEntity.getFinished().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualFinished.getTime(), Matchers.is(finished.getTime()));
        this.jobEntity.setFinished(null);
        Assert.assertFalse(this.jobEntity.getFinished().isPresent());
    }

    /**
     * Test the setter and getter for archive location.
     */
    @Test
    public void testSetGetArchiveLocation() {
        Assert.assertFalse(this.jobEntity.getArchiveLocation().isPresent());
        final String archiveLocation = "s3://some/location";
        this.jobEntity.setArchiveLocation(archiveLocation);
        Assert.assertEquals(
            archiveLocation,
            this.jobEntity.getArchiveLocation().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Tests whether a job status is updated correctly, and update time is
     * changed accordingly.
     */
    @Test
    public void testSetJobStatus() {
        final JobEntity localJobEntity = new JobEntity();

        // Times are null on initialization
        Assert.assertFalse(localJobEntity.getStarted().isPresent());
        Assert.assertFalse(localJobEntity.getFinished().isPresent());

        // start time is not zero on INIT, finish time is still 0
        localJobEntity.setJobStatus(JobStatus.INIT);
        Assert.assertTrue(localJobEntity.getStarted().isPresent());
        Assert.assertFalse(localJobEntity.getFinished().isPresent());
        final Date started = localJobEntity.getStarted().orElseThrow(IllegalArgumentException::new);

        // Shouldn't affect finish time
        localJobEntity.setJobStatus(JobStatus.RUNNING);
        Assert.assertThat(localJobEntity.getStarted().orElseGet(RandomSuppliers.DATE), Matchers.is(started));
        Assert.assertFalse(localJobEntity.getFinished().isPresent());

        // finish time is non-zero on completion
        localJobEntity.setJobStatus(JobStatus.SUCCEEDED);
        Assert.assertThat(localJobEntity.getStarted().orElseGet(RandomSuppliers.DATE), Matchers.is(started));
        Assert.assertTrue(localJobEntity.getFinished().isPresent());

        final JobEntity jobEntity2 = new JobEntity();
        // Times are null on initialization
        Assert.assertFalse(jobEntity2.getStarted().isPresent());
        Assert.assertFalse(jobEntity2.getFinished().isPresent());

        // start time is not zero on INIT, finish time is still 0
        final String initMessage = "We're initializing";
        jobEntity2.setJobStatus(JobStatus.INIT, initMessage);
        Assert.assertTrue(jobEntity2.getStarted().isPresent());
        Assert.assertFalse(jobEntity2.getFinished().isPresent());
        Assert.assertEquals(initMessage, jobEntity2.getStatusMsg().orElseGet(RandomSuppliers.STRING));

        // finish time is non-zero on completion
        final String successMessage = "Job Succeeded";
        jobEntity2.setJobStatus(JobStatus.SUCCEEDED, successMessage);
        Assert.assertEquals(successMessage, jobEntity2.getStatusMsg().orElseGet(RandomSuppliers.STRING));
        Assert.assertTrue(jobEntity2.getFinished().isPresent());
    }

    /**
     * Test the setter and the getter for tags.
     */
    @Test
    public void testSetGetTags() {
        Assert.assertNotNull(this.jobEntity.getTags());
        Assert.assertTrue(this.jobEntity.getTags().isEmpty());
        final Set<String> tags = Sets.newHashSet("someTag", "someOtherTag");
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
        this.jobEntity.setTags(tags);
        Assert.assertThat(this.jobEntity.getTags(), Matchers.is(tags));

        this.jobEntity.setTags(null);
        Assert.assertThat(this.jobEntity.getTags(), Matchers.empty());
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
        entity.setJob(this.jobEntity);
        Assert.assertThat(this.jobEntity, Matchers.is(entity.getJob()));
    }

    /**
     * Test to make sure can successfully set the Cluster this job ran on.
     */
    @Test
    public void canSetCluster() {
        final ClusterEntity cluster = new ClusterEntity();
        final String clusterName = UUID.randomUUID().toString();
        cluster.setName(clusterName);
        Assert.assertFalse(this.jobEntity.getClusterName().isPresent());
        this.jobEntity.setCluster(cluster);
        Assert.assertThat(this.jobEntity.getCluster(), Matchers.is(cluster));
        final String actualClusterName = this.jobEntity.getClusterName().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualClusterName, Matchers.is(clusterName));

        this.jobEntity.setCluster(null);
        Assert.assertThat(this.jobEntity.getCluster(), Matchers.nullValue());
        Assert.assertFalse(this.jobEntity.getClusterName().isPresent());
    }

    /**
     * Test to make sure can successfully set the Command this job ran used.
     */
    @Test
    public void canSetCommand() {
        final CommandEntity command = new CommandEntity();
        final String commandName = UUID.randomUUID().toString();
        command.setName(commandName);
        Assert.assertFalse(this.jobEntity.getCommandName().isPresent());
        this.jobEntity.setCommand(command);
        Assert.assertThat(this.jobEntity.getCommand(), Matchers.is(command));
        final String actualCommandName = this.jobEntity.getCommandName().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualCommandName, Matchers.is(commandName));

        this.jobEntity.setCommand(null);
        Assert.assertThat(this.jobEntity.getCommand(), Matchers.nullValue());
        Assert.assertFalse(this.jobEntity.getCommandName().isPresent());
    }

    /**
     * Test the application set and get methods.
     *
     * @throws GenieException on any issue
     */
    @Test
    public void canSetApplications() throws GenieException {
        final ApplicationEntity application1 = new ApplicationEntity();
        application1.setId(UUID.randomUUID().toString());
        final ApplicationEntity application2 = new ApplicationEntity();
        application2.setId(UUID.randomUUID().toString());
        final ApplicationEntity application3 = new ApplicationEntity();
        application3.setId(UUID.randomUUID().toString());
        final List<ApplicationEntity> applications = Lists.newArrayList(application1, application2, application3);

        Assert.assertThat(this.jobEntity.getApplications(), Matchers.empty());
        this.jobEntity.setApplications(applications);
        Assert.assertThat(this.jobEntity.getApplications(), Matchers.is(applications));
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
        entity.setTags(tags);
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
        Assert.assertThat(job.getId().orElseGet(RandomSuppliers.STRING), Matchers.is(id));
        Assert.assertThat(job.getName(), Matchers.is(name));
        Assert.assertThat(job.getUser(), Matchers.is(user));
        Assert.assertThat(job.getVersion(), Matchers.is(version));
        Assert.assertThat(job.getDescription().orElseGet(RandomSuppliers.STRING), Matchers.is(description));
        Assert.assertThat(job.getCreated().orElseGet(RandomSuppliers.DATE), Matchers.is(created));
        Assert.assertThat(job.getUpdated().orElseGet(RandomSuppliers.DATE), Matchers.is(updated));
        Assert.assertThat(job.getClusterName().orElseGet(RandomSuppliers.STRING), Matchers.is(clusterName));
        Assert.assertThat(job.getCommandName().orElseGet(RandomSuppliers.STRING), Matchers.is(commandName));
        Assert.assertThat(job.getTags(), Matchers.is(tags));
        Assert.assertThat(job.getArchiveLocation().orElseGet(RandomSuppliers.STRING), Matchers.is(archiveLocation));
        Assert.assertThat(job.getStarted().orElseGet(RandomSuppliers.DATE), Matchers.is(started));
        Assert.assertThat(job.getFinished().orElseGet(RandomSuppliers.DATE), Matchers.is(finished));
        Assert.assertThat(job.getStatus(), Matchers.is(JobStatus.SUCCEEDED));
        Assert.assertThat(job.getStatusMsg().orElseGet(RandomSuppliers.STRING), Matchers.is(statusMessage));
    }
}
