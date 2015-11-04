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

import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
public class JobEntityTests extends EntityTestsBase {

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
        this.jobEntity.onCreateAuditable();
        Assert.assertNotNull(this.jobEntity.getId());
        Assert.assertFalse(this.jobEntity.getTags().contains(
                CommonFields.GENIE_ID_TAG_NAMESPACE + this.jobEntity.getId())
        );
        Assert.assertFalse(this.jobEntity.getTags().contains(
                CommonFields.GENIE_NAME_TAG_NAMESPACE + this.jobEntity.getName())
        );
    }

    /**
     * Test the execution cluster id get/set.
     */
    @Test
    public void testSetGetClusterId() {
        Assert.assertNull(this.jobEntity.getClusterId());
        final String executionClusterId = UUID.randomUUID().toString();
        this.jobEntity.setClusterId(executionClusterId);
        Assert.assertEquals(executionClusterId, this.jobEntity.getClusterId());
    }

    /**
     * Test setter and getter for command id.
     */
    @Test
    public void testSetGetCommandId() {
        Assert.assertNull(this.jobEntity.getCommandId());
        final String commandId = UUID.randomUUID().toString();
        this.jobEntity.setCommandId(commandId);
        Assert.assertEquals(commandId, this.jobEntity.getCommandId());
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
     * Test the setter and getter for the kill uri.
     */
    @Test
    public void testSetGetKillURI() {
        Assert.assertNull(this.jobEntity.getKillURI());
        final String killURI = "http://localhost:7001";
        this.jobEntity.setKillURI(killURI);
        Assert.assertEquals(killURI, this.jobEntity.getKillURI());
    }

    /**
     * Test the setter and getter for the output uri.
     */
    @Test
    public void testSetGetOutputURI() {
        Assert.assertNull(this.jobEntity.getOutputURI());
        final String outputURI = "http://localhost:7001";
        this.jobEntity.setOutputURI(outputURI);
        Assert.assertEquals(outputURI, this.jobEntity.getOutputURI());
    }

    /**
     * Test the setter and getter for the exit code.
     */
    @Test
    public void testSetGetExitCode() {
        Assert.assertEquals(-1, this.jobEntity.getExitCode());
        final int exitCode = 0;
        this.jobEntity.setExitCode(exitCode);
        Assert.assertEquals(exitCode, this.jobEntity.getExitCode());
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
}
