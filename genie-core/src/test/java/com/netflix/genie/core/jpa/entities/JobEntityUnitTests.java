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
import java.util.stream.Collectors;

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
        Assert.assertNotNull(localJobEntity.getUniqueId());
        Assert.assertEquals(JobEntity.DEFAULT_VERSION, localJobEntity.getVersion());
    }

    /**
     * Test the parameter based constructor.
     */
    @Test
    public void testConstructor() {
        Assert.assertNotNull(this.jobEntity.getUniqueId());
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
    public void testOnCreateJob() throws GeniePreconditionException {
        Assert.assertNull(this.jobEntity.getTagSearchString());
        this.jobEntity.onCreateJob();
        Assert.assertNull(this.jobEntity.getTagSearchString());
        final TagEntity one = new TagEntity();
        one.setTag("abc");
        final TagEntity two = new TagEntity();
        two.setTag("def");
        final TagEntity three = new TagEntity();
        three.setTag("ghi");
        this.jobEntity.setTags(Sets.newHashSet(three, two, one));
        this.jobEntity.onCreateJob();
        Assert.assertEquals("|abc||def||ghi|", this.jobEntity.getTagSearchString());
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
        Assert.assertFalse(this.jobEntity.getCommandArgs().isPresent());
        final String commandArgs = UUID.randomUUID().toString();
        this.jobEntity.setCommandArgs(commandArgs);
        Assert.assertThat(
            this.jobEntity.getCommandArgs().orElseThrow(IllegalArgumentException::new),
            Matchers.is(commandArgs)
        );
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
        final TagEntity one = new TagEntity();
        one.setTag("someTag");
        final TagEntity two = new TagEntity();
        two.setTag("someOtherTag");
        final Set<TagEntity> tags = Sets.newHashSet(one, two);
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
     * Test to make sure can successfully set the Cluster this job ran on.
     */
    @Test
    public void canSetCluster() {
        final ClusterEntity cluster = new ClusterEntity();
        final String clusterName = UUID.randomUUID().toString();
        cluster.setName(clusterName);
        Assert.assertFalse(this.jobEntity.getClusterName().isPresent());
        this.jobEntity.setCluster(cluster);
        Assert.assertThat(this.jobEntity.getCluster().orElseThrow(IllegalArgumentException::new), Matchers.is(cluster));
        final String actualClusterName = this.jobEntity.getClusterName().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualClusterName, Matchers.is(clusterName));

        this.jobEntity.setCluster(null);
        Assert.assertFalse(this.jobEntity.getCluster().isPresent());
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
        Assert.assertThat(this.jobEntity.getCommand().orElseThrow(IllegalArgumentException::new), Matchers.is(command));
        final String actualCommandName = this.jobEntity.getCommandName().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualCommandName, Matchers.is(commandName));

        this.jobEntity.setCommand(null);
        Assert.assertFalse(this.jobEntity.getCommand().isPresent());
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
        application1.setUniqueId(UUID.randomUUID().toString());
        final ApplicationEntity application2 = new ApplicationEntity();
        application2.setUniqueId(UUID.randomUUID().toString());
        final ApplicationEntity application3 = new ApplicationEntity();
        application3.setUniqueId(UUID.randomUUID().toString());
        final List<ApplicationEntity> applications = Lists.newArrayList(application1, application2, application3);

        Assert.assertThat(this.jobEntity.getApplications(), Matchers.empty());
        this.jobEntity.setApplications(applications);
        Assert.assertThat(this.jobEntity.getApplications(), Matchers.is(applications));
    }

    /**
     * Test to make sure can successfully set the host name the job is running on.
     */
    @Test
    public void canSetHostName() {
        final String hostName = UUID.randomUUID().toString();
        this.jobEntity.setHostName(hostName);
        Assert.assertThat(this.jobEntity.getHostName(), Matchers.is(hostName));
    }

    /**
     * Test to make sure can successfully set the process id of the job.
     */
    @Test
    public void canSetProcessId() {
        final int processId = 12309834;
        this.jobEntity.setProcessId(processId);
        Assert.assertThat(this.jobEntity.getProcessId().orElseGet(RandomSuppliers.INT), Matchers.is(processId));
    }

    /**
     * Make sure setting the check delay time period works properly.
     */
    @Test
    public void canSetCheckDelay() {
        Assert.assertFalse(this.jobEntity.getCheckDelay().isPresent());
        final long newDelay = 1803234L;
        this.jobEntity.setCheckDelay(newDelay);
        Assert.assertThat(
            this.jobEntity.getCheckDelay().orElseThrow(IllegalArgumentException::new),
            Matchers.is(newDelay)
        );
    }

    /**
     * Test to make sure can successfully set the process id of the job.
     */
    @Test
    public void canSetExitCode() {
        final int exitCode = 80072043;
        this.jobEntity.setExitCode(exitCode);
        Assert.assertThat(this.jobEntity.getExitCode().orElseGet(RandomSuppliers.INT), Matchers.is(exitCode));
    }

    /**
     * Make sure setting timeout works.
     */
    @Test
    public void canSetTimeout() {
        Assert.assertFalse(this.jobEntity.getTimeout().isPresent());
        final Date timeout = new Date();
        this.jobEntity.setTimeout(timeout);
        Assert.assertThat(this.jobEntity.getTimeout().orElseGet(RandomSuppliers.DATE), Matchers.is(timeout));
    }

    /**
     * Make sure setting memory used works.
     */
    @Test
    public void canSetMemoryUsed() {
        Assert.assertFalse(this.jobEntity.getMemoryUsed().isPresent());
        final int memory = 10_240;
        this.jobEntity.setMemoryUsed(memory);
        Assert.assertThat(this.jobEntity.getMemoryUsed().orElseGet(RandomSuppliers.INT), Matchers.is(memory));
    }

    /**
     * Make sure can set the client host name the request came from.
     */
    @Test
    public void canSetClientHost() {
        final String clientHost = UUID.randomUUID().toString();
        this.jobEntity.setClientHost(clientHost);
        Assert.assertThat(this.jobEntity.getClientHost().orElseGet(RandomSuppliers.STRING), Matchers.is(clientHost));
    }

    /**
     * Make sure we can set and get the user agent string.
     */
    @Test
    public void canSetUserAgent() {
        Assert.assertFalse(this.jobEntity.getUserAgent().isPresent());
        final String userAgent = UUID.randomUUID().toString();
        this.jobEntity.setUserAgent(userAgent);
        Assert.assertThat(
            this.jobEntity.getUserAgent().orElseGet(RandomSuppliers.STRING),
            Matchers.is(userAgent)
        );
    }

    /**
     * Make sure we can set and get the number of attachments.
     */
    @Test
    public void canSetNumAttachments() {
        Assert.assertFalse(this.jobEntity.getNumAttachments().isPresent());
        final int numAttachments = 380208;
        this.jobEntity.setNumAttachments(numAttachments);
        Assert.assertThat(
            this.jobEntity.getNumAttachments().orElseGet(RandomSuppliers.INT), Matchers.is(numAttachments)
        );
    }

    /**
     * Make sure we can set and get the total size of the attachments.
     */
    @Test
    public void canSetTotalSizeOfAttachments() {
        Assert.assertFalse(this.jobEntity.getTotalSizeOfAttachments().isPresent());
        final long totalSizeOfAttachments = 90832432L;
        this.jobEntity.setTotalSizeOfAttachments(totalSizeOfAttachments);
        Assert.assertThat(
            this.jobEntity.getTotalSizeOfAttachments().orElseGet(RandomSuppliers.LONG),
            Matchers.is(totalSizeOfAttachments)
        );
    }

    /**
     * Make sure we can set and get the size of the std out file.
     */
    @Test
    public void canSetStdOutSize() {
        Assert.assertFalse(this.jobEntity.getStdOutSize().isPresent());
        final long stdOutSize = 90334432L;
        this.jobEntity.setStdOutSize(stdOutSize);
        Assert.assertThat(
            this.jobEntity.getStdOutSize().orElseGet(RandomSuppliers.LONG),
            Matchers.is(stdOutSize)
        );
    }

    /**
     * Make sure we can set and get the size of the std err file.
     */
    @Test
    public void canSetStdErrSize() {
        Assert.assertFalse(this.jobEntity.getStdErrSize().isPresent());
        final long stdErrSize = 9089932L;
        this.jobEntity.setStdErrSize(stdErrSize);
        Assert.assertThat(
            this.jobEntity.getStdErrSize().orElseGet(RandomSuppliers.LONG),
            Matchers.is(stdErrSize)
        );
    }

    /**
     * Make sure can set the group for the job.
     */
    @Test
    public void canSetGroup() {
        final String group = UUID.randomUUID().toString();
        this.jobEntity.setGenieUserGroup(group);
        Assert.assertThat(this.jobEntity.getGenieUserGroup().orElseGet(RandomSuppliers.STRING), Matchers.is(group));
    }

    /**
     * Make sure can set the cluster criteria.
     */
    @Test
    public void canSetClusterCriterias() {
        final Set<TagEntity> one = Sets.newHashSet("one", "two", "three")
            .stream()
            .map(
                tag -> {
                    final TagEntity tagEntity = new TagEntity();
                    tagEntity.setTag(tag);
                    return tagEntity;
                }
            ).collect(Collectors.toSet());
        final Set<TagEntity> two = Sets.newHashSet("four", "five", "six")
            .stream()
            .map(
                tag -> {
                    final TagEntity tagEntity = new TagEntity();
                    tagEntity.setTag(tag);
                    return tagEntity;
                }
            ).collect(Collectors.toSet());
        final Set<TagEntity> three = Sets.newHashSet("seven", "eight", "nine")
            .stream()
            .map(
                tag -> {
                    final TagEntity tagEntity = new TagEntity();
                    tagEntity.setTag(tag);
                    return tagEntity;
                }
            ).collect(Collectors.toSet());

        final ClusterCriteriaEntity entity1 = new ClusterCriteriaEntity();
        entity1.setTags(one);
        final ClusterCriteriaEntity entity2 = new ClusterCriteriaEntity();
        entity2.setTags(two);
        final ClusterCriteriaEntity entity3 = new ClusterCriteriaEntity();
        entity3.setTags(three);

        final List<ClusterCriteriaEntity> clusterCriterias = Lists.newArrayList(entity1, entity2, entity3);

        this.jobEntity.setClusterCriterias(clusterCriterias);
        Assert.assertThat(this.jobEntity.getClusterCriterias(), Matchers.is(clusterCriterias));
    }

    /**
     * Make sure the setter for the jobEntity class works for JPA for null cluster criterias.
     */
    @Test
    public void canSetNullClusterCriterias() {
        this.jobEntity.setClusterCriterias(null);
        Assert.assertThat(this.jobEntity.getClusterCriterias(), Matchers.empty());
    }

    /**
     * Make sure can set the command args for the job.
     */
    @Test
    public void canSetCommandArgs() {
        final String commandArgs = UUID.randomUUID().toString();
        this.jobEntity.setCommandArgs(commandArgs);
        Assert.assertThat(this.jobEntity.getCommandArgs().orElseGet(RandomSuppliers.STRING), Matchers.is(commandArgs));
    }

    /**
     * Make sure can set the file configs.
     */
    @Test
    public void canSetConfigs() {
        final String fileConfigs = UUID.randomUUID().toString();
        final FileEntity config = new FileEntity();
        config.setFile(fileConfigs);
        final Set<FileEntity> configs = Sets.newHashSet(config);
        this.jobEntity.setConfigs(configs);
        Assert.assertThat(this.jobEntity.getConfigs(), Matchers.is(configs));
    }

    /**
     * Make sure can set the blank file configs.
     */
    @Test
    public void canSetNullConfigs() {
        this.jobEntity.setConfigs(null);
        Assert.assertThat(this.jobEntity.getConfigs(), Matchers.empty());
    }

    /**
     * Make sure can set the file dependencies.
     */
    @Test
    public void canSetDependencies() {
        final String fileConfigs = UUID.randomUUID().toString();
        final FileEntity dependency = new FileEntity();
        dependency.setFile(fileConfigs);
        final Set<FileEntity> dependencies = Sets.newHashSet(dependency);
        this.jobEntity.setDependencies(dependencies);
        Assert.assertThat(this.jobEntity.getDependencies(), Matchers.is(dependencies));
    }

    /**
     * Make sure can set the blank file dependencies.
     */
    @Test
    public void canSetNullDependencies() {
        this.jobEntity.setDependencies(null);
        Assert.assertThat(this.jobEntity.getDependencies(), Matchers.empty());
    }

    /**
     * Make sure can set whether to disable logs or not.
     */
    @Test
    public void canSetDisableLogArchival() {
        this.jobEntity.setDisableLogArchival(true);
        Assert.assertTrue(this.jobEntity.isDisableLogArchival());
    }

    /**
     * Make sure can set the email address of the user.
     */
    @Test
    public void canSetEmail() {
        final String email = UUID.randomUUID().toString();
        this.jobEntity.setEmail(email);
        Assert.assertThat(this.jobEntity.getEmail().orElseGet(RandomSuppliers.STRING), Matchers.is(email));
    }

    /**
     * Make sure can set the command criteria.
     */
    @Test
    public void canSetCommandCriteria() {
        final TagEntity one = new TagEntity();
        one.setTag(UUID.randomUUID().toString());
        final TagEntity two = new TagEntity();
        two.setTag(UUID.randomUUID().toString());
        final Set<TagEntity> commandCriteria = Sets.newHashSet(one, two);

        this.jobEntity.setCommandCriteria(commandCriteria);
        Assert.assertThat(this.jobEntity.getCommandCriteria(), Matchers.notNullValue());
        Assert.assertThat(this.jobEntity.getCommandCriteria(), Matchers.is(commandCriteria));
    }

    /**
     * Make sure the setter for the jobEntity class works for JPA for null command criteria.
     */
    @Test
    public void canSetNullCommandCriteria() {
        this.jobEntity.setCommandCriteria(null);
        Assert.assertThat(this.jobEntity.getCommandCriteria(), Matchers.empty());
    }

    /**
     * Make sure can set the setup file.
     */
    @Test
    public void canSetSetupFile() {
        final String setupFile = UUID.randomUUID().toString();
        final FileEntity setupFileEntity = new FileEntity();
        setupFileEntity.setFile(setupFile);
        this.jobEntity.setSetupFile(setupFileEntity);
        Assert.assertThat(
            this.jobEntity.getSetupFile().orElseThrow(IllegalArgumentException::new), Matchers.is(setupFileEntity)
        );
    }

    /**
     * Make sure can set the tags for the job.
     */
    @Test
    public void canSetTags() {
        final TagEntity one = new TagEntity();
        one.setTag(UUID.randomUUID().toString());
        final TagEntity two = new TagEntity();
        two.setTag(UUID.randomUUID().toString());
        final Set<TagEntity> tags = Sets.newHashSet(one, two);

        this.jobEntity.setTags(tags);
        Assert.assertThat(this.jobEntity.getTags(), Matchers.is(tags));

        this.jobEntity.setTags(null);
        Assert.assertThat(this.jobEntity.getTags(), Matchers.empty());
    }

    /**
     * Make sure can set the number of cpu's to use for the job.
     */
    @Test
    public void canSetCpu() {
        final int cpu = 16;
        this.jobEntity.setCpuRequested(cpu);
        Assert.assertThat(this.jobEntity.getCpuRequested().orElseGet(RandomSuppliers.INT), Matchers.is(cpu));
    }

    /**
     * Make sure can set the amount of memory to use for the job.
     */
    @Test
    public void canSetMemory() {
        final int memory = 2048;
        this.jobEntity.setMemoryRequested(memory);
        Assert.assertThat(this.jobEntity.getMemoryRequested().orElseGet(RandomSuppliers.INT), Matchers.is(memory));
    }

    /**
     * Make sure the jobEntity class sets the applications requested right.
     */
    @Test
    public void canSetApplicationsRequested() {
        final String application = UUID.randomUUID().toString();
        final List<String> applications = Lists.newArrayList(application);
        this.jobEntity.setApplicationsRequested(applications);
        Assert.assertThat(this.jobEntity.getApplicationsRequested(), Matchers.is(applications));
    }

    /**
     * Make sure can set the timeout date for the job.
     */
    @Test
    public void canSetTimeoutRequested() {
        Assert.assertFalse(this.jobEntity.getTimeoutRequested().isPresent());
        final int timeout = 28023423;
        this.jobEntity.setTimeoutRequested(timeout);
        Assert.assertThat(this.jobEntity.getTimeoutRequested().orElseGet(RandomSuppliers.INT), Matchers.is(timeout));
    }
}
