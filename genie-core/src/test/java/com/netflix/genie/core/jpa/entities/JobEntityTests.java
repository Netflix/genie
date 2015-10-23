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
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
public class JobEntityTests extends EntityTestsBase {

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
                    + JobEntity.CRITERIA_DELIMITER
                    + CLUSTER_CRITERIA_1
                    + JobEntity.CRITERIA_SET_DELIMITER
                    + CLUSTER_CRITERIA_3
                    + JobEntity.CRITERIA_DELIMITER
                    + CLUSTER_CRITERIA_4;

    private static final String EXPECTED_COMMAND_CRITERIA_STRING =
            COMMAND_CRITERIA_1 + JobEntity.CRITERIA_DELIMITER + COMMAND_CRITERIA_2;


    private static final Set<String> COMMAND_CRITERIA
            = new HashSet<>();
    private static final List<com.netflix.genie.common.dto.ClusterCriteria> CLUSTER_CRITERIAS
            = new ArrayList<>();

    private JobEntity jobEntity;

    /**
     * Setup some variables for tests.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @BeforeClass
    public static void setupTestJobClass() throws GeniePreconditionException {
        COMMAND_CRITERIA.add(COMMAND_CRITERIA_1);
        COMMAND_CRITERIA.add(COMMAND_CRITERIA_2);

        final ClusterCriteria one = new ClusterCriteria(Sets.newHashSet(CLUSTER_CRITERIA_1, CLUSTER_CRITERIA_2));
        CLUSTER_CRITERIAS.add(one);

        final ClusterCriteria two = new ClusterCriteria(Sets.newHashSet(CLUSTER_CRITERIA_3, CLUSTER_CRITERIA_4));
        CLUSTER_CRITERIAS.add(two);
    }

    /**
     * Create a job object to be used in a bunch of tests.
     */
    @Before
    public void setup() {
        this.jobEntity = new JobEntity(
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
        Assert.assertEquals(COMMAND_ARGS, this.jobEntity.getCommandArgs());
        Assert.assertEquals(COMMAND_CRITERIA, this.jobEntity.getCommandCriteria());
        Assert.assertEquals(CLUSTER_CRITERIAS, this.jobEntity.getClusterCriterias());
    }

    /**
     * Test the parameter based constructor.
     */
    @Test
    public void testConstructorWithBlankVersion() {
        final JobEntity localJobEntity = new JobEntity(
                USER,
                NAME,
                null,
                COMMAND_ARGS,
                COMMAND_CRITERIA,
                CLUSTER_CRITERIAS
        );
        Assert.assertNull(localJobEntity.getId());
        Assert.assertEquals(NAME, localJobEntity.getName());
        Assert.assertEquals(USER, localJobEntity.getUser());
        Assert.assertEquals(JobEntity.DEFAULT_VERSION, localJobEntity.getVersion());
        Assert.assertEquals(COMMAND_ARGS, localJobEntity.getCommandArgs());
        Assert.assertEquals(COMMAND_CRITERIA, localJobEntity.getCommandCriteria());
        Assert.assertEquals(CLUSTER_CRITERIAS, localJobEntity.getClusterCriterias());
    }

    /**
     * Test the onCreateOrUpdateJob method which is called before saving
     * or updating.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    @Ignore
    public void testOnCreateOrUpdateJob() throws GeniePreconditionException {
        Assert.assertNull(this.jobEntity.getId());
        Assert.assertNull(this.jobEntity.getClusterCriteriasString());
        Assert.assertNull(this.jobEntity.getCommandCriteriaString());
        //Simulate the call stack JPA will make on persist
        this.jobEntity.onCreateAuditable();
        this.jobEntity.onCreateOrUpdateJob();
        Assert.assertNotNull(this.jobEntity.getId());
        Assert.assertNotNull(this.jobEntity.getClusterCriteriasString());
        Assert.assertNotNull(this.jobEntity.getCommandCriteriaString());
        Assert.assertFalse(this.jobEntity.getTags().contains(
                CommonFields.GENIE_ID_TAG_NAMESPACE + this.jobEntity.getId())
        );
        Assert.assertFalse(this.jobEntity.getTags().contains(
                CommonFields.GENIE_NAME_TAG_NAMESPACE + this.jobEntity.getName())
        );
    }

    /**
     * Just test to make sure it doesn't try to do something weird
     * with tags.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testOnCreateOrUpdateJobWithNotNullTags() throws GeniePreconditionException {
        Assert.assertNotNull(this.jobEntity.getTags());
        Assert.assertTrue(this.jobEntity.getTags().isEmpty());
        this.jobEntity.onCreateAuditable();
        this.jobEntity.onCreateOrUpdateJob();
        Assert.assertNotNull(this.jobEntity.getTags());
        Assert.assertNotNull(this.jobEntity.getId());
        Assert.assertNotNull(this.jobEntity.getClusterCriteriasString());
        Assert.assertNotNull(this.jobEntity.getCommandCriteriaString());
        Assert.assertFalse(this.jobEntity.getTags().contains(
                CommonFields.GENIE_ID_TAG_NAMESPACE + this.jobEntity.getId())
        );
        Assert.assertFalse(this.jobEntity.getTags().contains(
                CommonFields.GENIE_NAME_TAG_NAMESPACE + this.jobEntity.getName())
        );
    }

    /**
     * Test to make sure the sets are recreated onLoad.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testOnLoadJob() throws GeniePreconditionException {
        this.jobEntity.onCreateOrUpdateJob();
        final String clusterCriteriasString = this.jobEntity.getClusterCriteriasString();
        final String commandCriteriaString = this.jobEntity.getCommandCriteriaString();
        final JobEntity jobEntity2 = new JobEntity();
        jobEntity2.setClusterCriteriasString(clusterCriteriasString);
        jobEntity2.setCommandCriteriaString(commandCriteriaString);
        jobEntity2.onLoadJob();
        Assert.assertEquals(CLUSTER_CRITERIAS.size(), jobEntity2.getClusterCriterias().size());
        Assert.assertEquals(COMMAND_CRITERIA, jobEntity2.getCommandCriteria());
    }

    /**
     * Test the group get/set.
     */
    @Test
    public void testSetGetGroup() {
        Assert.assertNull(this.jobEntity.getGroup());
        final String group = "Test group";
        this.jobEntity.setGroup(group);
        Assert.assertEquals(group, this.jobEntity.getGroup());
    }

    /**
     * Test the execution cluster name get/set.
     */
    @Test
    public void testSetGetExecutionClusterName() {
        Assert.assertNull(this.jobEntity.getExecutionClusterName());
        final String executionClusterName = UUID.randomUUID().toString();
        this.jobEntity.setExecutionClusterName(executionClusterName);
        Assert.assertEquals(executionClusterName, this.jobEntity.getExecutionClusterName());
    }

    /**
     * Test the execution cluster id get/set.
     */
    @Test
    public void testSetGetExecutionClusterId() {
        Assert.assertNull(this.jobEntity.getExecutionClusterId());
        final String executionClusterId = UUID.randomUUID().toString();
        this.jobEntity.setExecutionClusterId(executionClusterId);
        Assert.assertEquals(executionClusterId, this.jobEntity.getExecutionClusterId());
    }

    /**
     * Test setting and getting the clusterCriterias.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetGetClusterCriterias() throws GeniePreconditionException {
        final JobEntity localJobEntity = new JobEntity(); //Use default constructor so null
        Assert.assertNotNull(localJobEntity.getClusterCriterias());
        Assert.assertNull(localJobEntity.getClusterCriteriasString());
        localJobEntity.setClusterCriterias(CLUSTER_CRITERIAS);
        Assert.assertEquals(CLUSTER_CRITERIAS, localJobEntity.getClusterCriterias());
        Assert.assertNotNull(localJobEntity.getClusterCriteriasString());
    }

    /**
     * Make sure user can't set bad cluster criterias.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetClusterCriteriasWithNull() throws GeniePreconditionException {
        this.jobEntity.setClusterCriterias(null);
    }

    /**
     * Make sure user can't set bad cluster criterias.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetClusterCriteriasWithEmpty() throws GeniePreconditionException {
        this.jobEntity.setClusterCriterias(new ArrayList<>());
    }

    /**
     * Test setting and getting the command args.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetGetCommandArgs() throws GeniePreconditionException {
        final JobEntity localJobEntity = new JobEntity(); //Use default constructor so null
        Assert.assertNull(localJobEntity.getCommandArgs());
        localJobEntity.setCommandArgs(COMMAND_ARGS);
        Assert.assertEquals(COMMAND_ARGS, localJobEntity.getCommandArgs());
    }

    /**
     * Make sure user can't set bad command args.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetCommandArgsNull() throws GeniePreconditionException {
        this.jobEntity.setCommandArgs(null);
    }

    /**
     * Make sure user can't set bad command args.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetCommandArgsWithEmpty() throws GeniePreconditionException {
        this.jobEntity.setCommandArgs("");
    }

    /**
     * Make sure user can't set bad command args.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetCommandArgsWithBlank() throws GeniePreconditionException {
        this.jobEntity.setCommandArgs("   ");
    }

    /**
     * Test the setter and getter for file dependencies.
     */
    @Test
    public void testSetGetFileDependencies() {
        Assert.assertNull(this.jobEntity.getFileDependencies());
        final String fileDependencies = "/some/file/dependency.sh,/another.sh";
        this.jobEntity.setFileDependencies(fileDependencies);
        Assert.assertEquals(fileDependencies, this.jobEntity.getFileDependencies());
    }

//    /**
//     * Test the setter and getter for attachments.
//     *
//     * @throws GeniePreconditionException If any precondition isn't met.
//     */
//    @Test
//    public void testSetGetAttachments() throws GeniePreconditionException {
//        Assert.assertNotNull(this.jobEntity.getAttachments());
//        final FileAttachment attachment
//                = new FileAttachment("/some/query.q", "select * from mytable;".getBytes(UTF8_CHARSET));
//        final Set<com.netflix.genie.common.dto.FileAttachment> attachments = new HashSet<>();
//        attachments.add(attachment);
//        this.jobEntity.setAttachments(attachments);
//        Assert.assertEquals(attachments, this.jobEntity.getAttachments());
//    }

    /**
     * Test setting and getting whether archival is disabled.
     */
    @Test
    public void testSetGetDisableLogArchival() {
        this.jobEntity.setDisableLogArchival(true);
        Assert.assertTrue(this.jobEntity.isDisableLogArchival());
        this.jobEntity.setDisableLogArchival(false);
        Assert.assertFalse(this.jobEntity.isDisableLogArchival());
    }

    /**
     * Test the setter and getter for user email.
     */
    @Test
    public void testSetGetEmail() {
        Assert.assertNull(this.jobEntity.getEmail());
        final String email = "genie@test.com";
        this.jobEntity.setEmail(email);
        Assert.assertEquals(email, this.jobEntity.getEmail());
    }

    /**
     * Test the setter and getter for application name.
     */
    @Test
    public void testSetGetApplicationName() {
        Assert.assertNull(this.jobEntity.getApplicationName());
        final String applicationName = "Tez";
        this.jobEntity.setApplicationName(applicationName);
        Assert.assertEquals(applicationName, this.jobEntity.getApplicationName());
    }

    /**
     * Test setter and getter for application id.
     */
    @Test
    public void testSetGetApplicationId() {
        Assert.assertNull(this.jobEntity.getApplicationId());
        final String applicationId = UUID.randomUUID().toString();
        this.jobEntity.setApplicationId(applicationId);
        Assert.assertEquals(applicationId, this.jobEntity.getApplicationId());
    }

    /**
     * Test the setter and getter for command name.
     */
    @Test
    public void testSetGetCommandName() {
        Assert.assertNull(this.jobEntity.getCommandName());
        final String commandName = "Tez";
        this.jobEntity.setCommandName(commandName);
        Assert.assertEquals(commandName, this.jobEntity.getCommandName());
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
     * Test setter and getter for the process handle.
     */
    @Test
    public void testSetGetProcessHandle() {
        Assert.assertEquals(-1, this.jobEntity.getProcessHandle());
        final int processHandle = 243098;
        this.jobEntity.setProcessHandle(processHandle);
        Assert.assertEquals(processHandle, this.jobEntity.getProcessHandle());
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
     * Test the setter and getter for the client host.
     */
    @Test
    public void testSetGetClientHost() {
        Assert.assertNull(this.jobEntity.getClientHost());
        final String clientHost = "http://localhost:7001";
        this.jobEntity.setClientHost(clientHost);
        Assert.assertEquals(clientHost, this.jobEntity.getClientHost());
    }

    /**
     * Test the setter and getter for the host name.
     */
    @Test
    public void testSetGetHostName() {
        Assert.assertNull(this.jobEntity.getHostName());
        final String hostName = "http://localhost:7001";
        this.jobEntity.setHostName(hostName);
        Assert.assertEquals(hostName, this.jobEntity.getHostName());
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
     * Test the setter and getter for forwarded.
     */
    @Test
    public void testSetGetForwarded() {
        this.jobEntity.setForwarded(false);
        Assert.assertFalse(this.jobEntity.isForwarded());
        this.jobEntity.setForwarded(true);
        Assert.assertTrue(this.jobEntity.isForwarded());
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
     * Test the setter and getter for Cluster criterias string.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetGetClusterCriteriasString() throws GeniePreconditionException {
        Assert.assertNotNull(this.jobEntity.getClusterCriteriasString());
        Assert.assertEquals(2, this.jobEntity.getClusterCriterias().size());
        this.jobEntity.setClusterCriteriasString(CLUSTER_CRITERIA_1);
        Assert.assertEquals(CLUSTER_CRITERIA_1, this.jobEntity.getClusterCriteriasString());
        Assert.assertEquals(1, this.jobEntity.getClusterCriterias().size());
    }

    /**
     * Test setter and getter for command criteria.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetGetCommandCriteria() throws GeniePreconditionException {
        final JobEntity localJobEntity = new JobEntity(); //so that command criteria is null
        Assert.assertNotNull(localJobEntity.getCommandCriteria());
        Assert.assertNull(localJobEntity.getCommandCriteriaString());
        localJobEntity.setCommandCriteria(COMMAND_CRITERIA);
        Assert.assertEquals(COMMAND_CRITERIA, localJobEntity.getCommandCriteria());
        Assert.assertNotNull(localJobEntity.getCommandCriteriaString());
    }

    /**
     * Test setter and getter for the command criteria string.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetGetCommandCriteriaString() throws GeniePreconditionException {
        Assert.assertNotNull(this.jobEntity.getCommandCriteriaString());
        Assert.assertEquals(2, this.jobEntity.getCommandCriteria().size());
        final String commandCriteriaString = "newCriteria";
        this.jobEntity.setCommandCriteriaString(commandCriteriaString);
        Assert.assertEquals(commandCriteriaString, this.jobEntity.getCommandCriteriaString());
        Assert.assertEquals(1, this.jobEntity.getCommandCriteria().size());
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
     * Test setter and getter for env property file.
     */
    @Test
    public void testSetGetEnvPropFile() {
        Assert.assertNull(this.jobEntity.getSetupFile());
        final String envPropFile = "/some/property/file";
        this.jobEntity.setSetupFile(envPropFile);
        Assert.assertEquals(envPropFile, this.jobEntity.getSetupFile());
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
     * Test the setter and getter for the chosen cluster criteria string.
     */
    @Test
    public void testSetGetChosenClusterCriteriaString() {
        Assert.assertNull(this.jobEntity.getChosenClusterCriteriaString());
        final String chosenClusterCriteriaString = "someChosenCriteria";
        this.jobEntity.setChosenClusterCriteriaString(chosenClusterCriteriaString);
        Assert.assertEquals(chosenClusterCriteriaString,
                this.jobEntity.getChosenClusterCriteriaString());
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
     * Test validate with null command criteria.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNullCommandCriteria() {
        final JobEntity localJobEntity = new JobEntity(
                USER,
                NAME,
                VERSION,
                COMMAND_ARGS,
                null,
                CLUSTER_CRITERIAS
        );
        this.validate(localJobEntity);
    }

    /**
     * Test validate with empty command criteria.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateEmptyCommandCriteria() {
        final JobEntity localJobEntity = new JobEntity(
                USER,
                NAME,
                VERSION,
                COMMAND_ARGS,
                new HashSet<>(),
                CLUSTER_CRITERIAS
        );
        this.validate(localJobEntity);
    }

    /**
     * Test validate with null command args.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNullCommandArgs() {
        final JobEntity localJobEntity = new JobEntity(
                USER,
                NAME,
                VERSION,
                null,
                COMMAND_CRITERIA,
                CLUSTER_CRITERIAS
        );
        this.validate(localJobEntity);
    }

    /**
     * Test validate with empty command args.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateEmptyCommandArgs() {
        final JobEntity localJobEntity = new JobEntity(
                USER,
                NAME,
                VERSION,
                "",
                COMMAND_CRITERIA,
                CLUSTER_CRITERIAS
        );
        this.validate(localJobEntity);
    }

    /**
     * Test validate with blank command args.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateBlankCommandArgs() {
        final JobEntity localJobEntity = new JobEntity(
                USER,
                NAME,
                VERSION,
                "  ",
                COMMAND_CRITERIA,
                CLUSTER_CRITERIAS
        );
        this.validate(localJobEntity);
    }

    /**
     * Test validate with null Cluster criteria.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNullClusterCriteria() {
        final JobEntity localJobEntity = new JobEntity(
                USER,
                NAME,
                VERSION,
                COMMAND_ARGS,
                COMMAND_CRITERIA,
                null
        );
        this.validate(localJobEntity);
    }

    /**
     * Test validate with empty Cluster criteria.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateEmptyClusterCriteria() {
        final JobEntity localJobEntity = new JobEntity(
                USER,
                NAME,
                VERSION,
                COMMAND_ARGS,
                COMMAND_CRITERIA,
                new ArrayList<>()
        );
        this.validate(localJobEntity);
    }

    /**
     * Test the helper method to convert cluster criterias to a string.
     */
    @Test
    public void testClusterCriteriasToString() {
        Assert.assertNull(this.jobEntity.clusterCriteriasToString(null));
        Assert.assertNull(this.jobEntity.clusterCriteriasToString(
                new ArrayList<>()));

        final String criterias = this.jobEntity.clusterCriteriasToString(CLUSTER_CRITERIAS);
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
        Assert.assertNull(this.jobEntity.commandCriteriaToString(null));
        Assert.assertNull(this.jobEntity.commandCriteriaToString(new HashSet<>()));

        Assert.assertEquals(EXPECTED_COMMAND_CRITERIA_STRING,
                this.jobEntity.commandCriteriaToString(COMMAND_CRITERIA));
    }

    /**
     * Test the helper method to convert string to command criteria.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testStringToCommandCriteria() throws GeniePreconditionException {
        Assert.assertEquals(COMMAND_CRITERIA,
                this.jobEntity.stringToCommandCriteria(
                        EXPECTED_COMMAND_CRITERIA_STRING));
    }

    /**
     * Test the helper method to convert string to cluster criterias.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testStringToClusterCriterias() throws GeniePreconditionException {
        final List<com.netflix.genie.common.dto.ClusterCriteria> criterias =
                this.jobEntity.stringToClusterCriterias(EXPECTED_CLUSTER_CRITERIAS_STRING);
        Assert.assertEquals(CLUSTER_CRITERIAS.size(), criterias.size());
        for (int i = 0; i < criterias.size(); i++) {
            Assert.assertEquals(CLUSTER_CRITERIAS.get(i).getTags(),
                    criterias.get(i).getTags());
        }
    }
}
