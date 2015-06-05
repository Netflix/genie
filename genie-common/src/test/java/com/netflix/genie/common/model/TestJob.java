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
package com.netflix.genie.common.model;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.validation.ConstraintViolationException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Test case for Job Status utility methods.
 *
 * @author amsharma
 * @author tgianos
 */
public class TestJob extends TestEntityBase {

    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    private static final String USER = "tgianos";
    private static final String NAME = "TomsJob";
    private static final String VERSION = "1.2.3";
    private static final String COMMAND_ARGS = "-d something -f somethingElse";
    private static final String COMMAND_CRITERIA_1 = "hive";
    private static final String COMMAND_CRITERIA_2 = "prod";
    private static final String CLUSTER_CRITERIA_1 = "prod";
    private static final String CLUSTER_CRITERIA_2 = "yarn";
    private static final String CLUSTER_CRITERIA_3 = "test";
    private static final String CLUSTER_CRITERIA_4 = "yarn";

    private static final String EXPECTED_CLUSTER_CRITERIAS_STRING =
            CLUSTER_CRITERIA_2
                    + Job.CRITERIA_DELIMITER
                    + CLUSTER_CRITERIA_1
                    + Job.CRITERIA_SET_DELIMITER
                    + CLUSTER_CRITERIA_3
                    + Job.CRITERIA_DELIMITER
                    + CLUSTER_CRITERIA_4;

    private static final String EXPECTED_COMMAND_CRITERIA_STRING =
            COMMAND_CRITERIA_1 + Job.CRITERIA_DELIMITER + COMMAND_CRITERIA_2;


    private static final Set<String> COMMAND_CRITERIA
            = new HashSet<>();
    private static final List<ClusterCriteria> CLUSTER_CRITERIAS
            = new ArrayList<>();

    private Job job;

    /**
     * Setup some variables for tests.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @BeforeClass
    public static void setupTestJobClass() throws GeniePreconditionException {
        COMMAND_CRITERIA.add(COMMAND_CRITERIA_1);
        COMMAND_CRITERIA.add(COMMAND_CRITERIA_2);

        final ClusterCriteria one = new ClusterCriteria();
        final Set<String> tagsOne = new HashSet<>();
        tagsOne.add(CLUSTER_CRITERIA_1);
        tagsOne.add(CLUSTER_CRITERIA_2);
        one.setTags(tagsOne);
        CLUSTER_CRITERIAS.add(one);

        final ClusterCriteria two = new ClusterCriteria();
        final Set<String> tagsTwo = new HashSet<>();
        tagsTwo.add(CLUSTER_CRITERIA_3);
        tagsTwo.add(CLUSTER_CRITERIA_4);
        two.setTags(tagsTwo);
        CLUSTER_CRITERIAS.add(two);
    }

    /**
     * Create a job object to be used in a bunch of tests.
     */
    @Before
    public void setup() {
        this.job = new Job(
                USER,
                NAME,
                VERSION,
                COMMAND_ARGS,
                COMMAND_CRITERIA,
                CLUSTER_CRITERIAS
        );
    }

    /**
     * Test the default constructor.
     */
    @Test
    public void testDefaultConstructor() {
        final Job localJob = new Job();
        Assert.assertNull(localJob.getId());
        Assert.assertEquals(Job.DEFAULT_VERSION, localJob.getVersion());
    }

    /**
     * Test the parameter based constructor.
     */
    @Test
    public void testConstructor() {
        Assert.assertNull(this.job.getId());
        Assert.assertEquals(NAME, this.job.getName());
        Assert.assertEquals(USER, this.job.getUser());
        Assert.assertEquals(VERSION, this.job.getVersion());
        Assert.assertEquals(COMMAND_ARGS, this.job.getCommandArgs());
        Assert.assertEquals(COMMAND_CRITERIA, this.job.getCommandCriteria());
        Assert.assertEquals(CLUSTER_CRITERIAS, this.job.getClusterCriterias());
    }

    /**
     * Test the parameter based constructor.
     */
    @Test
    public void testConstructorWithBlankVersion() {
        final Job localJob = new Job(
                USER,
                NAME,
                null,
                COMMAND_ARGS,
                COMMAND_CRITERIA,
                CLUSTER_CRITERIAS
        );
        Assert.assertNull(localJob.getId());
        Assert.assertEquals(NAME, localJob.getName());
        Assert.assertEquals(USER, localJob.getUser());
        Assert.assertEquals(Job.DEFAULT_VERSION, localJob.getVersion());
        Assert.assertEquals(COMMAND_ARGS, localJob.getCommandArgs());
        Assert.assertEquals(COMMAND_CRITERIA, localJob.getCommandCriteria());
        Assert.assertEquals(CLUSTER_CRITERIAS, localJob.getClusterCriterias());
    }

    /**
     * Test the onCreateOrUpdateJob method which is called before saving
     * or updating.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testOnCreateOrUpdateJob() throws GeniePreconditionException {
        Assert.assertNull(this.job.getId());
        Assert.assertNull(this.job.getClusterCriteriasString());
        Assert.assertNull(this.job.getCommandCriteriaString());
        //Simulate the call stack JPA will make on persist
        this.job.onCreateAuditable();
        this.job.onCreateOrUpdateJob();
        Assert.assertNotNull(this.job.getId());
        Assert.assertNotNull(this.job.getClusterCriteriasString());
        Assert.assertNotNull(this.job.getCommandCriteriaString());
        Assert.assertFalse(this.job.getTags().contains(
                CommonEntityFields.GENIE_ID_TAG_NAMESPACE + this.job.getId()));
        Assert.assertFalse(this.job.getTags().contains(
                CommonEntityFields.GENIE_NAME_TAG_NAMESPACE + this.job.getName()));
    }

    /**
     * Just test to make sure it doesn't try to do something weird
     * with tags.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testOnCreateOrUpdateJobWithNotNullTags() throws GeniePreconditionException {
        Assert.assertNull(this.job.getTags());
        this.job.onCreateAuditable();
        this.job.onCreateOrUpdateJob();
        Assert.assertNotNull(this.job.getTags());
        Assert.assertNotNull(this.job.getId());
        Assert.assertNotNull(this.job.getClusterCriteriasString());
        Assert.assertNotNull(this.job.getCommandCriteriaString());
        Assert.assertFalse(this.job.getTags().contains(CommonEntityFields.GENIE_ID_TAG_NAMESPACE + this.job.getId()));
        Assert.assertFalse(this.job.getTags().contains(CommonEntityFields.GENIE_NAME_TAG_NAMESPACE
                + this.job.getName()));
    }

    /**
     * Test to make sure the sets are recreated onLoad.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testOnLoadJob() throws GeniePreconditionException {
        this.job.onCreateOrUpdateJob();
        final String clusterCriteriasString = this.job.getClusterCriteriasString();
        final String commandCriteriaString = this.job.getCommandCriteriaString();
        final Job job2 = new Job();
        job2.setClusterCriteriasString(clusterCriteriasString);
        job2.setCommandCriteriaString(commandCriteriaString);
        job2.onLoadJob();
        Assert.assertEquals(CLUSTER_CRITERIAS.size(), job2.getClusterCriterias().size());
        Assert.assertEquals(COMMAND_CRITERIA, job2.getCommandCriteria());
    }

    /**
     * Test the description get/set.
     */
    @Test
    public void testSetGetDescription() {
        Assert.assertNull(this.job.getDescription());
        final String description = "Test description";
        this.job.setDescription(description);
        Assert.assertEquals(description, this.job.getDescription());
    }

    /**
     * Test the group get/set.
     */
    @Test
    public void testSetGetGroup() {
        Assert.assertNull(this.job.getGroup());
        final String group = "Test group";
        this.job.setGroup(group);
        Assert.assertEquals(group, this.job.getGroup());
    }

    /**
     * Test the execution cluster name get/set.
     */
    @Test
    public void testSetGetExecutionClusterName() {
        Assert.assertNull(this.job.getExecutionClusterName());
        final String executionClusterName = UUID.randomUUID().toString();
        this.job.setExecutionClusterName(executionClusterName);
        Assert.assertEquals(executionClusterName, this.job.getExecutionClusterName());
    }

    /**
     * Test the execution cluster id get/set.
     */
    @Test
    public void testSetGetExecutionClusterId() {
        Assert.assertNull(this.job.getExecutionClusterId());
        final String executionClusterId = UUID.randomUUID().toString();
        this.job.setExecutionClusterId(executionClusterId);
        Assert.assertEquals(executionClusterId, this.job.getExecutionClusterId());
    }

    /**
     * Test setting and getting the clusterCriterias.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetGetClusterCriterias() throws GeniePreconditionException {
        final Job localJob = new Job(); //Use default constructor so null
        Assert.assertNull(localJob.getClusterCriterias());
        Assert.assertNull(localJob.getClusterCriteriasString());
        localJob.setClusterCriterias(CLUSTER_CRITERIAS);
        Assert.assertEquals(CLUSTER_CRITERIAS, localJob.getClusterCriterias());
        Assert.assertNotNull(localJob.getClusterCriteriasString());
    }

    /**
     * Make sure user can't set bad cluster criterias.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetClusterCriteriasWithNull() throws GeniePreconditionException {
        this.job.setClusterCriterias(null);
    }

    /**
     * Make sure user can't set bad cluster criterias.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetClusterCriteriasWithEmpty() throws GeniePreconditionException {
        this.job.setClusterCriterias(new ArrayList<ClusterCriteria>());
    }

    /**
     * Test setting and getting the command args.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetGetCommandArgs() throws GeniePreconditionException {
        final Job localJob = new Job(); //Use default constructor so null
        Assert.assertNull(localJob.getCommandArgs());
        localJob.setCommandArgs(COMMAND_ARGS);
        Assert.assertEquals(COMMAND_ARGS, localJob.getCommandArgs());
    }

    /**
     * Make sure user can't set bad command args.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetCommandArgsNull() throws GeniePreconditionException {
        this.job.setCommandArgs(null);
    }

    /**
     * Make sure user can't set bad command args.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetCommandArgsWithEmpty() throws GeniePreconditionException {
        this.job.setCommandArgs("");
    }

    /**
     * Make sure user can't set bad command args.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetCommandArgsWithBlank() throws GeniePreconditionException {
        this.job.setCommandArgs("   ");
    }

    /**
     * Test the setter and getter for file dependencies.
     */
    @Test
    public void testSetGetFileDependencies() {
        Assert.assertNull(this.job.getFileDependencies());
        final String fileDependencies = "/some/file/dependency.sh,/another.sh";
        this.job.setFileDependencies(fileDependencies);
        Assert.assertEquals(fileDependencies, this.job.getFileDependencies());
    }

    /**
     * Test the setter and getter for attachments.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetGetAttachments() throws GeniePreconditionException {
        Assert.assertNull(this.job.getAttachments());
        final FileAttachment attachment = new FileAttachment();
        attachment.setName("/some/query.q");
        attachment.setData("select * from mytable;".getBytes(UTF8_CHARSET));
        final Set<FileAttachment> attachments = new HashSet<>();
        attachments.add(attachment);
        this.job.setAttachments(attachments);
        Assert.assertEquals(attachments, this.job.getAttachments());
    }

    /**
     * Test setting and getting whether archival is disabled.
     */
    @Test
    public void testSetGetDisableLogArchival() {
        this.job.setDisableLogArchival(true);
        Assert.assertTrue(this.job.isDisableLogArchival());
        this.job.setDisableLogArchival(false);
        Assert.assertFalse(this.job.isDisableLogArchival());
    }

    /**
     * Test the setter and getter for user email.
     */
    @Test
    public void testSetGetEmail() {
        Assert.assertNull(this.job.getEmail());
        final String email = "genie@test.com";
        this.job.setEmail(email);
        Assert.assertEquals(email, this.job.getEmail());
    }

    /**
     * Test the setter and getter for application name.
     */
    @Test
    public void testSetGetApplicationName() {
        Assert.assertNull(this.job.getApplicationName());
        final String applicationName = "Tez";
        this.job.setApplicationName(applicationName);
        Assert.assertEquals(applicationName, this.job.getApplicationName());
    }

    /**
     * Test setter and getter for application id.
     */
    @Test
    public void testSetGetApplicationId() {
        Assert.assertNull(this.job.getApplicationId());
        final String applicationId = UUID.randomUUID().toString();
        this.job.setApplicationId(applicationId);
        Assert.assertEquals(applicationId, this.job.getApplicationId());
    }

    /**
     * Test the setter and getter for command name.
     */
    @Test
    public void testSetGetCommandName() {
        Assert.assertNull(this.job.getCommandName());
        final String commandName = "Tez";
        this.job.setCommandName(commandName);
        Assert.assertEquals(commandName, this.job.getCommandName());
    }

    /**
     * Test setter and getter for command id.
     */
    @Test
    public void testSetGetCommandId() {
        Assert.assertNull(this.job.getCommandId());
        final String commandId = UUID.randomUUID().toString();
        this.job.setCommandId(commandId);
        Assert.assertEquals(commandId, this.job.getCommandId());
    }

    /**
     * Test setter and getter for the process handle.
     */
    @Test
    public void testSetGetProcessHandle() {
        Assert.assertEquals(-1, this.job.getProcessHandle());
        final int processHandle = 243098;
        this.job.setProcessHandle(processHandle);
        Assert.assertEquals(processHandle, this.job.getProcessHandle());
    }

    /**
     * Test the setter and getter for status.
     */
    @Test
    public void testSetGetStatus() {
        Assert.assertNull(this.job.getStatus());
        this.job.setStatus(JobStatus.KILLED);
        Assert.assertEquals(JobStatus.KILLED, this.job.getStatus());
    }

    /**
     * Test the setter and getter for the status message.
     */
    @Test
    public void testSetGetStatusMsg() {
        Assert.assertNull(this.job.getStatusMsg());
        final String statusMsg = "Job is doing great";
        this.job.setStatusMsg(statusMsg);
        Assert.assertEquals(statusMsg, this.job.getStatusMsg());
    }

    /**
     * Test setter and getter for started.
     */
    @Test
    public void testSetGetStarted() {
        Assert.assertEquals(0L, this.job.getStarted().getTime());
        final Date started = new Date(123453L);
        this.job.setStarted(started);
        Assert.assertEquals(started.getTime(), this.job.getStarted().getTime());
    }

    /**
     * Test setter and getter for finished.
     */
    @Test
    public void testSetGetFinished() {
        Assert.assertEquals(0L, this.job.getFinished().getTime());
        final Date finished = new Date(123453L);
        this.job.setFinished(finished);
        Assert.assertEquals(finished.getTime(), this.job.getFinished().getTime());
    }

    /**
     * Test the setter and getter for the client host.
     */
    @Test
    public void testSetGetClientHost() {
        Assert.assertNull(this.job.getClientHost());
        final String clientHost = "http://localhost:7001";
        this.job.setClientHost(clientHost);
        Assert.assertEquals(clientHost, this.job.getClientHost());
    }

    /**
     * Test the setter and getter for the host name.
     */
    @Test
    public void testSetGetHostName() {
        Assert.assertNull(this.job.getHostName());
        final String hostName = "http://localhost:7001";
        this.job.setHostName(hostName);
        Assert.assertEquals(hostName, this.job.getHostName());
    }

    /**
     * Test the setter and getter for the kill uri.
     */
    @Test
    public void testSetGetKillURI() {
        Assert.assertNull(this.job.getKillURI());
        final String killURI = "http://localhost:7001";
        this.job.setKillURI(killURI);
        Assert.assertEquals(killURI, this.job.getKillURI());
    }

    /**
     * Test the setter and getter for the output uri.
     */
    @Test
    public void testSetGetOutputURI() {
        Assert.assertNull(this.job.getOutputURI());
        final String outputURI = "http://localhost:7001";
        this.job.setOutputURI(outputURI);
        Assert.assertEquals(outputURI, this.job.getOutputURI());
    }

    /**
     * Test the setter and getter for the exit code.
     */
    @Test
    public void testSetGetExitCode() {
        Assert.assertEquals(-1, this.job.getExitCode());
        final int exitCode = 0;
        this.job.setExitCode(exitCode);
        Assert.assertEquals(exitCode, this.job.getExitCode());
    }

    /**
     * Test the setter and getter for forwarded.
     */
    @Test
    public void testSetGetForwarded() {
        this.job.setForwarded(false);
        Assert.assertFalse(this.job.isForwarded());
        this.job.setForwarded(true);
        Assert.assertTrue(this.job.isForwarded());
    }

    /**
     * Test the setter and getter for archive location.
     */
    @Test
    public void testSetGetArchiveLocation() {
        Assert.assertNull(this.job.getArchiveLocation());
        final String archiveLocation = "s3://some/location";
        this.job.setArchiveLocation(archiveLocation);
        Assert.assertEquals(archiveLocation, this.job.getArchiveLocation());
    }

    /**
     * Test the setter and getter for Cluster criterias string.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetGetClusterCriteriasString() throws GeniePreconditionException {
        Assert.assertNull(this.job.getClusterCriteriasString());
        Assert.assertEquals(2, this.job.getClusterCriterias().size());
        this.job.setClusterCriteriasString(CLUSTER_CRITERIA_1);
        Assert.assertEquals(CLUSTER_CRITERIA_1, this.job.getClusterCriteriasString());
        Assert.assertEquals(1, this.job.getClusterCriterias().size());
    }

    /**
     * Test setter and getter for command criteria.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetGetCommandCriteria() throws GeniePreconditionException {
        final Job localJob = new Job(); //so that command criteria is null
        Assert.assertNull(localJob.getCommandCriteria());
        Assert.assertNull(localJob.getCommandCriteriaString());
        localJob.setCommandCriteria(COMMAND_CRITERIA);
        Assert.assertEquals(COMMAND_CRITERIA, localJob.getCommandCriteria());
        Assert.assertNotNull(localJob.getCommandCriteriaString());
    }

    /**
     * Test setter and getter for the command criteria string.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetGetCommandCriteriaString() throws GeniePreconditionException {
        Assert.assertNull(this.job.getCommandCriteriaString());
        Assert.assertEquals(2, this.job.getCommandCriteria().size());
        final String commandCriteriaString = "newCriteria";
        this.job.setCommandCriteriaString(commandCriteriaString);
        Assert.assertEquals(commandCriteriaString, this.job.getCommandCriteriaString());
        Assert.assertEquals(1, this.job.getCommandCriteria().size());
    }

    /**
     * Tests whether a job status is updated correctly, and update time is
     * changed accordingly.
     */
    @Test
    public void testSetJobStatus() {
        final Job localJob = new Job();
        final Date dt = new Date(0);

        // finish time is 0 on initialization
        Assert.assertTrue(dt.compareTo(localJob.getFinished()) == 0);

        // start time is not zero on INIT, finish time is still 0
        localJob.setJobStatus(JobStatus.INIT);
        Assert.assertNotNull(localJob.getStarted());
        Assert.assertTrue(dt.compareTo(localJob.getFinished()) == 0);

        // finish time is non-zero on completion
        localJob.setJobStatus(JobStatus.SUCCEEDED);
        Assert.assertFalse(dt.compareTo(localJob.getFinished()) == 0);

        final Job job2 = new Job();
        // finish time is 0 on initialization
        Assert.assertTrue(dt.compareTo(job2.getFinished()) == 0);

        // start time is not zero on INIT, finish time is still 0
        final String initMessage = "We're initializing";
        job2.setJobStatus(JobStatus.INIT, initMessage);
        Assert.assertNotNull(job2.getStarted());
        Assert.assertEquals(initMessage, job2.getStatusMsg());
        Assert.assertTrue(dt.compareTo(job2.getStarted()) == -1);
        Assert.assertTrue(dt.compareTo(job2.getFinished()) == 0);

        // finish time is non-zero on completion
        final String successMessage = "Job Succeeded";
        job2.setJobStatus(JobStatus.SUCCEEDED, successMessage);
        Assert.assertEquals(successMessage, job2.getStatusMsg());
        Assert.assertFalse(dt.compareTo(job2.getFinished()) == 0);
    }

    /**
     * Test setter and getter for env property file.
     */
    @Test
    public void testSetGetEnvPropFile() {
        Assert.assertNull(this.job.getEnvPropFile());
        final String envPropFile = "/some/property/file";
        this.job.setEnvPropFile(envPropFile);
        Assert.assertEquals(envPropFile, this.job.getEnvPropFile());
    }

    /**
     * Test the setter and the getter for tags.
     */
    @Test
    public void testSetGetTags() {
        Assert.assertNull(this.job.getTags());
        final Set<String> tags = new HashSet<>();
        tags.add("someTag");
        tags.add("someOtherTag");
        this.job.setTags(tags);
        Assert.assertEquals(tags, this.job.getTags());
    }

    /**
     * Test the setter and getter for the chosen cluster criteria string.
     */
    @Test
    public void testSetGetChosenClusterCriteriaString() {
        Assert.assertNull(this.job.getChosenClusterCriteriaString());
        final String chosenClusterCriteriaString = "someChosenCriteria";
        this.job.setChosenClusterCriteriaString(chosenClusterCriteriaString);
        Assert.assertEquals(chosenClusterCriteriaString,
                this.job.getChosenClusterCriteriaString());
    }

    /**
     * Test Validate ok.
     */
    @Test
    public void testValidate() {
        this.validate(this.job);
    }

    /**
     * Test validate with exception from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateBadSuperClass() throws GenieException {
        this.validate(new Job());
    }

    /**
     * Test validate with null command criteria.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNullCommandCriteria() {
        final Job localJob = new Job(
                USER,
                NAME,
                VERSION,
                COMMAND_ARGS,
                null,
                CLUSTER_CRITERIAS
        );
        this.validate(localJob);
    }

    /**
     * Test validate with empty command criteria.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateEmptyCommandCriteria() {
        final Job localJob = new Job(
                USER,
                NAME,
                VERSION,
                COMMAND_ARGS,
                new HashSet<String>(),
                CLUSTER_CRITERIAS
        );
        this.validate(localJob);
    }

    /**
     * Test validate with null command args.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNullCommandArgs() {
        final Job localJob = new Job(
                USER,
                NAME,
                VERSION,
                null,
                COMMAND_CRITERIA,
                CLUSTER_CRITERIAS
        );
        this.validate(localJob);
    }

    /**
     * Test validate with empty command args.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateEmptyCommandArgs() {
        final Job localJob = new Job(
                USER,
                NAME,
                VERSION,
                "",
                COMMAND_CRITERIA,
                CLUSTER_CRITERIAS
        );
        this.validate(localJob);
    }

    /**
     * Test validate with blank command args.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateBlankCommandArgs() {
        final Job localJob = new Job(
                USER,
                NAME,
                VERSION,
                "  ",
                COMMAND_CRITERIA,
                CLUSTER_CRITERIAS
        );
        this.validate(localJob);
    }

    /**
     * Test validate with null Cluster criteria.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNullClusterCriteria() {
        final Job localJob = new Job(
                USER,
                NAME,
                VERSION,
                COMMAND_ARGS,
                COMMAND_CRITERIA,
                null
        );
        this.validate(localJob);
    }

    /**
     * Test validate with empty Cluster criteria.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateEmptyClusterCriteria() {
        final Job localJob = new Job(
                USER,
                NAME,
                VERSION,
                COMMAND_ARGS,
                COMMAND_CRITERIA,
                new ArrayList<ClusterCriteria>()
        );
        this.validate(localJob);
    }

    /**
     * Test the helper method to convert cluster criterias to a string.
     */
    @Test
    public void testClusterCriteriasToString() {
        Assert.assertNull(this.job.clusterCriteriasToString(null));
        Assert.assertNull(this.job.clusterCriteriasToString(
                new ArrayList<ClusterCriteria>()));

        final String criterias = this.job.clusterCriteriasToString(CLUSTER_CRITERIAS);
        Assert.assertTrue(criterias.contains(CLUSTER_CRITERIA_2));
        Assert.assertTrue(criterias.contains(CLUSTER_CRITERIA_1));
        Assert.assertTrue(criterias.contains(CLUSTER_CRITERIA_3));
        Assert.assertTrue(criterias.contains(CLUSTER_CRITERIA_4));
    }

    /**
     * Test the helper method to convert command criteria to a string.
     */
    @Test
    public void testCommandCriteriaToString() {
        Assert.assertNull(this.job.commandCriteriaToString(null));
        Assert.assertNull(this.job.commandCriteriaToString(
                new HashSet<String>()));

        Assert.assertEquals(EXPECTED_COMMAND_CRITERIA_STRING,
                this.job.commandCriteriaToString(COMMAND_CRITERIA));
    }

    /**
     * Test the helper method to convert string to command criteria.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testStringToCommandCriteria() throws GeniePreconditionException {
        Assert.assertEquals(COMMAND_CRITERIA,
                this.job.stringToCommandCriteria(
                        EXPECTED_COMMAND_CRITERIA_STRING));
    }

    /**
     * Test the helper method to convert string to command criteria.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testStringToCommandCriteriaNull() throws GeniePreconditionException {
        Assert.assertEquals(COMMAND_CRITERIA,
                this.job.stringToCommandCriteria(null));
    }

    /**
     * Test the helper method to convert string to command criteria.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testStringToCommandCriteriaEmpty() throws GeniePreconditionException {
        Assert.assertEquals(COMMAND_CRITERIA,
                this.job.stringToCommandCriteria(""));
    }

    /**
     * Test the helper method to convert string to cluster criterias.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testStringToClusterCriterias() throws GeniePreconditionException {
        final List<ClusterCriteria> criterias =
                this.job.stringToClusterCriterias(EXPECTED_CLUSTER_CRITERIAS_STRING);
        Assert.assertEquals(CLUSTER_CRITERIAS.size(), criterias.size());
        for (int i = 0; i < criterias.size(); i++) {
            Assert.assertEquals(CLUSTER_CRITERIAS.get(i).getTags(),
                    criterias.get(i).getTags());
        }
    }

    /**
     * Test the helper method to convert string to cluster criterias.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testStringToClusterCriteriasNull() throws GeniePreconditionException {
        final List<ClusterCriteria> criterias =
                this.job.stringToClusterCriterias(null);
        Assert.assertEquals(CLUSTER_CRITERIAS.size(), criterias.size());
        for (int i = 0; i < criterias.size(); i++) {
            Assert.assertEquals(CLUSTER_CRITERIAS.get(i).getTags(),
                    criterias.get(i).getTags());
        }
    }

    /**
     * Test the helper method to convert string to cluster criterias.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testStringToClusterCriteriasEmpty() throws GeniePreconditionException {
        final List<ClusterCriteria> criterias =
                this.job.stringToClusterCriterias("");
        Assert.assertEquals(CLUSTER_CRITERIAS.size(), criterias.size());
        for (int i = 0; i < criterias.size(); i++) {
            Assert.assertEquals(CLUSTER_CRITERIAS.get(i).getTags(),
                    criterias.get(i).getTags());
        }
    }
}
