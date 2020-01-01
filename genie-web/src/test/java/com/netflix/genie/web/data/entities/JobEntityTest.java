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
package com.netflix.genie.web.data.entities;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.validation.ConstraintViolationException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Test case for Job Status utility methods.
 *
 * @author amsharma
 * @author tgianos
 */
class JobEntityTest extends EntityTestBase {

    private static final String USER = "tgianos";
    private static final String NAME = "TomsJob";
    private static final String VERSION = "1.2.3";

    private JobEntity jobEntity;

    /**
     * Create a job object to be used in a bunch of tests.
     */
    @BeforeEach
    void setup() {
        this.jobEntity = new JobEntity();
        this.jobEntity.setUser(USER);
        this.jobEntity.setName(NAME);
        this.jobEntity.setVersion(VERSION);
    }

    /**
     * Test the default constructor.
     */
    @Test
    void testDefaultConstructor() {
        final JobEntity localJobEntity = new JobEntity();
        Assertions.assertThat(localJobEntity.getUniqueId()).isNotBlank();
    }

    /**
     * Test the parameter based constructor.
     */
    @Test
    void testConstructor() {
        Assertions.assertThat(this.jobEntity.getUniqueId()).isNotBlank();
        Assertions.assertThat(NAME).isEqualTo(this.jobEntity.getName());
        Assertions.assertThat(USER).isEqualTo(this.jobEntity.getUser());
        Assertions.assertThat(this.jobEntity.getVersion()).isEqualTo(VERSION);
    }

    /**
     * Test the onCreateOrUpdateJob method which is called before saving
     * or updating.
     */
    @Test
    void testOnCreateJob() {
        Assertions.assertThat(this.jobEntity.getTagSearchString()).isNull();
        this.jobEntity.onCreateJob();
        Assertions.assertThat(this.jobEntity.getTagSearchString()).isNull();
        final TagEntity one = new TagEntity("abc");
        final TagEntity two = new TagEntity("def");
        final TagEntity three = new TagEntity("ghi");
        this.jobEntity.setTags(Sets.newHashSet(three, two, one));
        this.jobEntity.onCreateJob();
        Assertions.assertThat(this.jobEntity.getTagSearchString()).isEqualTo("|abc||def||ghi|");
    }

    /**
     * Test the execution cluster name get/set.
     */
    @Test
    void testSetGetClusterName() {
        Assertions.assertThat(this.jobEntity.getClusterName()).isNotPresent();
        final String clusterName = UUID.randomUUID().toString();
        this.jobEntity.setClusterName(clusterName);
        Assertions.assertThat(this.jobEntity.getClusterName()).isPresent().contains(clusterName);
    }

    /**
     * Test setter and getter for command name.
     */
    @Test
    void testSetGetCommandName() {
        Assertions.assertThat(this.jobEntity.getCommandName()).isNotPresent();
        final String commandName = UUID.randomUUID().toString();
        this.jobEntity.setCommandName(commandName);
        Assertions.assertThat(this.jobEntity.getCommandName()).isPresent().contains(commandName);
    }

    /**
     * Make sure the setter and getter for command args works properly.
     */
    @Test
    void testSetGetCommandArgs() {
        Assertions.assertThat(this.jobEntity.getCommandArgs()).isEmpty();
        this.jobEntity.setCommandArgs(null);
        Assertions.assertThat(this.jobEntity.getCommandArgs()).isEmpty();
        final List<String> commandArgs = Lists.newArrayList();
        this.jobEntity.setCommandArgs(commandArgs);
        Assertions.assertThat(this.jobEntity.getCommandArgs()).isEmpty();
        commandArgs.add(UUID.randomUUID().toString());
        this.jobEntity.setCommandArgs(commandArgs);
        Assertions.assertThat(this.jobEntity.getCommandArgs()).isEqualTo(commandArgs);
    }

    /**
     * Test the setter and getter for status.
     */
    @Test
    void testSetGetStatus() {
        Assertions.assertThat(this.jobEntity.getStatus()).isNull();
        this.jobEntity.setStatus(JobStatus.KILLED.name());
        Assertions.assertThat(this.jobEntity.getStatus()).isEqualTo(JobStatus.KILLED.name());
    }

    /**
     * Test the setter and getter for the status message.
     */
    @Test
    void testSetGetStatusMsg() {
        Assertions.assertThat(this.jobEntity.getStatusMsg()).isNotPresent();
        final String statusMsg = "Job is doing great";
        this.jobEntity.setStatusMsg(statusMsg);
        Assertions.assertThat(this.jobEntity.getStatusMsg()).isPresent().contains(statusMsg);
    }

    /**
     * Test setter and getter for started.
     */
    @Test
    void testSetGetStarted() {
        Assertions.assertThat(this.jobEntity.getStarted()).isNotPresent();
        final Instant started = Instant.ofEpochMilli(123453L);
        this.jobEntity.setStarted(started);
        Assertions.assertThat(this.jobEntity.getStarted()).isPresent().contains(started);
        this.jobEntity.setStarted(null);
        Assertions.assertThat(this.jobEntity.getStarted()).isNotPresent();
    }

    /**
     * Test setter and getter for finished.
     */
    @Test
    void testSetGetFinished() {
        Assertions.assertThat(this.jobEntity.getFinished()).isNotPresent();
        final Instant finished = Instant.ofEpochMilli(123453L);
        this.jobEntity.setFinished(finished);
        Assertions.assertThat(this.jobEntity.getFinished()).isPresent().contains(finished);
        this.jobEntity.setFinished(null);
        Assertions.assertThat(this.jobEntity.getFinished()).isNotPresent();
    }

    /**
     * Test the setter and getter for archive location.
     */
    @Test
    void testSetGetArchiveLocation() {
        Assertions.assertThat(this.jobEntity.getArchiveLocation()).isNotPresent();
        final String archiveLocation = "s3://some/location";
        this.jobEntity.setArchiveLocation(archiveLocation);
        Assertions.assertThat(this.jobEntity.getArchiveLocation()).isPresent().contains(archiveLocation);
    }

    /**
     * Tests setting and getting the (transient) notifiedJobStatus field.
     */
    @Test
    void testSetNotifiedJobStatus() {
        final JobEntity localJobEntity = new JobEntity();
        Assertions.assertThat(localJobEntity.getNotifiedJobStatus()).isNotPresent();
        localJobEntity.setNotifiedJobStatus(JobStatus.RUNNING.name());
        Assertions.assertThat(localJobEntity.getNotifiedJobStatus()).isPresent().contains(JobStatus.RUNNING.name());
    }

    /**
     * Test the setter and the getter for tags.
     */
    @Test
    void testSetGetTags() {
        Assertions.assertThat(this.jobEntity.getTags()).isEmpty();
        final TagEntity one = new TagEntity("someTag");
        final TagEntity two = new TagEntity("someOtherTag");
        final Set<TagEntity> tags = Sets.newHashSet(one, two);
        this.jobEntity.setTags(tags);
        Assertions.assertThat(this.jobEntity.getTags()).isEqualTo(tags);

        this.jobEntity.setTags(null);
        Assertions.assertThat(this.jobEntity.getTags()).isEmpty();
    }

    /**
     * Test Validate ok.
     */
    @Test
    void testValidate() {
        this.validate(this.jobEntity);
    }

    /**
     * Test validate with exception from super class.
     */
    @Test
    void testValidateBadSuperClass() {
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(new JobEntity()));
    }

    /**
     * Test to make sure can successfully set the Cluster this job ran on.
     */
    @Test
    void canSetCluster() {
        final ClusterEntity cluster = new ClusterEntity();
        final String clusterName = UUID.randomUUID().toString();
        cluster.setName(clusterName);
        Assertions.assertThat(this.jobEntity.getCluster()).isNotPresent();
        Assertions.assertThat(this.jobEntity.getClusterName()).isNotPresent();
        this.jobEntity.setCluster(cluster);
        Assertions.assertThat(this.jobEntity.getCluster()).isPresent().contains(cluster);
        Assertions.assertThat(this.jobEntity.getClusterName()).isPresent().contains(clusterName);

        this.jobEntity.setCluster(null);
        Assertions.assertThat(this.jobEntity.getCluster()).isNotPresent();
        Assertions.assertThat(this.jobEntity.getClusterName()).isNotPresent();
    }

    /**
     * Test to make sure can successfully set the Command this job ran used.
     */
    @Test
    void canSetCommand() {
        final CommandEntity command = new CommandEntity();
        final String commandName = UUID.randomUUID().toString();
        command.setName(commandName);
        Assertions.assertThat(this.jobEntity.getCommand()).isNotPresent();
        Assertions.assertThat(this.jobEntity.getCommandName()).isNotPresent();
        this.jobEntity.setCommand(command);
        Assertions.assertThat(this.jobEntity.getCommand()).isPresent().contains(command);
        Assertions.assertThat(this.jobEntity.getCommandName()).isPresent().contains(commandName);

        this.jobEntity.setCommand(null);
        Assertions.assertThat(this.jobEntity.getCommand()).isNotPresent();
        Assertions.assertThat(this.jobEntity.getCommandName()).isNotPresent();
    }

    /**
     * Test the application set and get methods.
     */
    @Test
    void canSetApplications() {
        final ApplicationEntity application1 = new ApplicationEntity();
        application1.setUniqueId(UUID.randomUUID().toString());
        final ApplicationEntity application2 = new ApplicationEntity();
        application2.setUniqueId(UUID.randomUUID().toString());
        final ApplicationEntity application3 = new ApplicationEntity();
        application3.setUniqueId(UUID.randomUUID().toString());
        final List<ApplicationEntity> applications = Lists.newArrayList(application1, application2, application3);

        Assertions.assertThat(this.jobEntity.getApplications()).isEmpty();
        this.jobEntity.setApplications(applications);
        Assertions.assertThat(this.jobEntity.getApplications()).isEqualTo(applications);
    }

    /**
     * Test to make sure can successfully set the host name the job is running on.
     */
    @Test
    void canSetAgentHostName() {
        final String hostName = UUID.randomUUID().toString();
        this.jobEntity.setAgentHostname(hostName);
        Assertions.assertThat(this.jobEntity.getAgentHostname()).isPresent().contains(hostName);
    }

    /**
     * Test to make sure can successfully set the process id of the job.
     */
    @Test
    void canSetProcessId() {
        final int processId = 12309834;
        this.jobEntity.setProcessId(processId);
        Assertions.assertThat(this.jobEntity.getProcessId()).isPresent().contains(processId);
    }

    /**
     * Make sure setting the check delay time period works properly.
     */
    @Test
    void canSetCheckDelay() {
        Assertions.assertThat(this.jobEntity.getCheckDelay()).isNotPresent();
        final long newDelay = 1803234L;
        this.jobEntity.setCheckDelay(newDelay);
        Assertions.assertThat(this.jobEntity.getCheckDelay()).isPresent().contains(newDelay);
    }

    /**
     * Test to make sure can successfully set the process id of the job.
     */
    @Test
    void canSetExitCode() {
        final int exitCode = 80072043;
        this.jobEntity.setExitCode(exitCode);
        Assertions.assertThat(this.jobEntity.getExitCode()).isPresent().contains(exitCode);
    }

    /**
     * Make sure setting memory used works.
     */
    @Test
    void canSetMemoryUsed() {
        Assertions.assertThat(this.jobEntity.getMemoryUsed()).isNotPresent();
        final int memory = 10_240;
        this.jobEntity.setMemoryUsed(memory);
        Assertions.assertThat(this.jobEntity.getMemoryUsed()).isPresent().contains(memory);
    }

    /**
     * Make sure can set the client host name the request came from.
     */
    @Test
    void canSetRequestApiClientHostname() {
        final String clientHost = UUID.randomUUID().toString();
        this.jobEntity.setRequestApiClientHostname(clientHost);
        Assertions.assertThat(this.jobEntity.getRequestApiClientHostname()).isPresent().contains(clientHost);
    }

    /**
     * Make sure we can set and get the user agent string.
     */
    @Test
    void canSetRequestApiClientUserAgent() {
        Assertions.assertThat(this.jobEntity.getRequestApiClientUserAgent()).isNotPresent();
        final String userAgent = UUID.randomUUID().toString();
        this.jobEntity.setRequestApiClientUserAgent(userAgent);
        Assertions.assertThat(this.jobEntity.getRequestApiClientUserAgent()).isPresent().contains(userAgent);
    }

    /**
     * Make sure can set the client host name the request came from.
     */
    @Test
    void canSetRequestAgentClientHostname() {
        final String clientHost = UUID.randomUUID().toString();
        this.jobEntity.setRequestAgentClientHostname(clientHost);
        Assertions.assertThat(this.jobEntity.getRequestAgentClientHostname()).isPresent().contains(clientHost);
    }

    /**
     * Make sure can set the client version the request came from.
     */
    @Test
    void canSetRequestAgentClientVersion() {
        final String version = UUID.randomUUID().toString();
        this.jobEntity.setRequestAgentClientVersion(version);
        Assertions.assertThat(this.jobEntity.getRequestAgentClientVersion()).isPresent().contains(version);
    }

    /**
     * Make sure can set the client pid the request came from.
     */
    @Test
    void canSetRequestAgentClientPid() {
        final int pid = 28_000;
        this.jobEntity.setRequestAgentClientPid(pid);
        Assertions.assertThat(this.jobEntity.getRequestAgentClientPid()).isPresent().contains(pid);
    }

    /**
     * Make sure we can set and get the number of attachments.
     */
    @Test
    void canSetNumAttachments() {
        Assertions.assertThat(this.jobEntity.getNumAttachments()).isNotPresent();
        final int numAttachments = 380208;
        this.jobEntity.setNumAttachments(numAttachments);
        Assertions.assertThat(this.jobEntity.getNumAttachments()).isPresent().contains(numAttachments);
    }

    /**
     * Make sure we can set and get the total size of the attachments.
     */
    @Test
    void canSetTotalSizeOfAttachments() {
        Assertions.assertThat(this.jobEntity.getTotalSizeOfAttachments()).isNotPresent();
        final long totalSizeOfAttachments = 90832432L;
        this.jobEntity.setTotalSizeOfAttachments(totalSizeOfAttachments);
        Assertions.assertThat(this.jobEntity.getTotalSizeOfAttachments()).isPresent().contains(totalSizeOfAttachments);
    }

    /**
     * Make sure we can set and get the size of the std out file.
     */
    @Test
    void canSetStdOutSize() {
        Assertions.assertThat(this.jobEntity.getStdOutSize()).isNotPresent();
        final long stdOutSize = 90334432L;
        this.jobEntity.setStdOutSize(stdOutSize);
        Assertions.assertThat(this.jobEntity.getStdOutSize()).isPresent().contains(stdOutSize);
    }

    /**
     * Make sure we can set and get the size of the std err file.
     */
    @Test
    void canSetStdErrSize() {
        Assertions.assertThat(this.jobEntity.getStdErrSize()).isNotPresent();
        final long stdErrSize = 9089932L;
        this.jobEntity.setStdErrSize(stdErrSize);
        Assertions.assertThat(this.jobEntity.getStdErrSize()).isPresent().contains(stdErrSize);
    }

    /**
     * Make sure can set the group for the job.
     */
    @Test
    void canSetGroup() {
        final String group = UUID.randomUUID().toString();
        this.jobEntity.setGenieUserGroup(group);
        Assertions.assertThat(this.jobEntity.getGenieUserGroup()).isPresent().contains(group);
    }

    /**
     * Make sure can set the cluster criteria.
     */
    @Test
    void canSetClusterCriteria() {
        final Set<TagEntity> one = Sets.newHashSet("one", "two", "three")
            .stream()
            .map(TagEntity::new)
            .collect(Collectors.toSet());
        final Set<TagEntity> two = Sets.newHashSet("four", "five", "six")
            .stream()
            .map(TagEntity::new)
            .collect(Collectors.toSet());
        final Set<TagEntity> three = Sets.newHashSet("seven", "eight", "nine")
            .stream()
            .map(TagEntity::new)
            .collect(Collectors.toSet());

        final CriterionEntity entity1 = new CriterionEntity(null, null, null, null, one);
        final CriterionEntity entity2 = new CriterionEntity(null, null, null, null, two);
        final CriterionEntity entity3 = new CriterionEntity(null, null, null, null, three);

        final List<CriterionEntity> clusterCriteria = Lists.newArrayList(entity1, entity2, entity3);

        this.jobEntity.setClusterCriteria(clusterCriteria);
        Assertions.assertThat(this.jobEntity.getClusterCriteria()).isEqualTo(clusterCriteria);
    }

    /**
     * Make sure the setter for the jobEntity class works for JPA for null cluster criterias.
     */
    @Test
    void canSetNullClusterCriteria() {
        this.jobEntity.setClusterCriteria(null);
        Assertions.assertThat(this.jobEntity.getClusterCriteria()).isEmpty();
    }

    /**
     * Make sure can set the file configs.
     */
    @Test
    void canSetConfigs() {
        final Set<FileEntity> configs = Sets.newHashSet(new FileEntity(UUID.randomUUID().toString()));
        this.jobEntity.setConfigs(configs);
        Assertions.assertThat(this.jobEntity.getConfigs()).isEqualTo(configs);
    }

    /**
     * Make sure can set the blank file configs.
     */
    @Test
    void canSetNullConfigs() {
        this.jobEntity.setConfigs(null);
        Assertions.assertThat(this.jobEntity.getConfigs()).isEmpty();
    }

    /**
     * Make sure can set the file dependencies.
     */
    @Test
    void canSetDependencies() {
        final Set<FileEntity> dependencies = Sets.newHashSet(new FileEntity(UUID.randomUUID().toString()));
        this.jobEntity.setDependencies(dependencies);
        Assertions.assertThat(this.jobEntity.getDependencies()).isEqualTo(dependencies);
    }

    /**
     * Make sure can set the blank file dependencies.
     */
    @Test
    void canSetNullDependencies() {
        this.jobEntity.setDependencies(null);
        Assertions.assertThat(this.jobEntity.getDependencies()).isEmpty();
    }

    /**
     * Make sure can set whether to disable logs or not.
     */
    @Test
    void canSetArchivingDisabled() {
        this.jobEntity.setArchivingDisabled(true);
        Assertions.assertThat(this.jobEntity.isArchivingDisabled()).isTrue();
    }

    /**
     * Make sure can set the email address of the user.
     */
    @Test
    void canSetEmail() {
        final String email = UUID.randomUUID().toString();
        this.jobEntity.setEmail(email);
        Assertions.assertThat(this.jobEntity.getEmail()).isPresent().contains(email);
    }

    /**
     * Make sure can set the command criteria.
     */
    @Test
    void canSetCommandCriteria() {
        final Set<TagEntity> tags = Sets.newHashSet(
            new TagEntity(UUID.randomUUID().toString()),
            new TagEntity(UUID.randomUUID().toString())
        );

        final CriterionEntity commandCriterion = new CriterionEntity(null, null, null, null, tags);
        this.jobEntity.setCommandCriterion(commandCriterion);
        Assertions.assertThat(this.jobEntity.getCommandCriterion()).isEqualTo(commandCriterion);
    }

    /**
     * Make sure can set the setup file.
     */
    @Test
    void canSetSetupFile() {
        final FileEntity setupFileEntity = new FileEntity(UUID.randomUUID().toString());
        this.jobEntity.setSetupFile(setupFileEntity);
        Assertions.assertThat(this.jobEntity.getSetupFile()).isPresent().contains(setupFileEntity);
    }

    /**
     * Make sure can set the tags for the job.
     */
    @Test
    void canSetTags() {
        final TagEntity one = new TagEntity(UUID.randomUUID().toString());
        final TagEntity two = new TagEntity(UUID.randomUUID().toString());
        final Set<TagEntity> tags = Sets.newHashSet(one, two);

        this.jobEntity.setTags(tags);
        Assertions.assertThat(this.jobEntity.getTags()).isEqualTo(tags);

        this.jobEntity.setTags(null);
        Assertions.assertThat(this.jobEntity.getTags()).isEmpty();
    }

    /**
     * Make sure can set the number of cpu's to use for the job.
     */
    @Test
    void canSetRequestedCpu() {
        final int cpu = 16;
        this.jobEntity.setRequestedCpu(cpu);
        Assertions.assertThat(this.jobEntity.getRequestedCpu()).isPresent().contains(cpu);
    }

    /**
     * Make sure can set the amount of memory to use for the job.
     */
    @Test
    void canSetRequestedMemory() {
        final int memory = 2048;
        this.jobEntity.setRequestedMemory(memory);
        Assertions.assertThat(this.jobEntity.getRequestedMemory()).isPresent().contains(memory);
    }

    /**
     * Make sure the jobEntity class sets the applications requested right.
     */
    @Test
    void canSetRequestedApplications() {
        final String application = UUID.randomUUID().toString();
        final List<String> applications = Lists.newArrayList(application);
        this.jobEntity.setRequestedApplications(applications);
        Assertions.assertThat(this.jobEntity.getRequestedApplications()).isEqualTo(applications);
    }

    /**
     * Make sure can set the timeout date for the job.
     */
    @Test
    void canSetRequestedTimeout() {
        Assertions.assertThat(this.jobEntity.getRequestedTimeout()).isNotPresent();
        final int timeout = 28023423;
        this.jobEntity.setRequestedTimeout(timeout);
        Assertions.assertThat(this.jobEntity.getRequestedTimeout()).isPresent().contains(timeout);
    }

    /**
     * Test the setter.
     */
    @Test
    void canSetRequestedEnvironmentVariables() {
        Assertions.assertThat(this.jobEntity.getRequestedEnvironmentVariables()).isEmpty();

        this.jobEntity.setRequestedEnvironmentVariables(null);
        Assertions.assertThat(this.jobEntity.getRequestedEnvironmentVariables()).isEmpty();

        final Map<String, String> variables = Maps.newHashMap();
        variables.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        this.jobEntity.setRequestedEnvironmentVariables(variables);
        Assertions.assertThat(this.jobEntity.getRequestedEnvironmentVariables()).isEqualTo(variables);

        // Make sure outside modifications of collection don't effect internal class state
        variables.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        Assertions.assertThat(this.jobEntity.getRequestedEnvironmentVariables()).isNotEqualTo(variables);

        this.jobEntity.setRequestedEnvironmentVariables(variables);
        Assertions.assertThat(this.jobEntity.getRequestedEnvironmentVariables()).isEqualTo(variables);

        // Make sure this clears variables
        this.jobEntity.setRequestedEnvironmentVariables(null);
        Assertions.assertThat(this.jobEntity.getRequestedEnvironmentVariables()).isEmpty();
    }

    /**
     * Test the setter.
     */
    @Test
    void canSetEnvironmentVariables() {
        Assertions.assertThat(this.jobEntity.getEnvironmentVariables()).isEmpty();

        this.jobEntity.setEnvironmentVariables(null);
        Assertions.assertThat(this.jobEntity.getEnvironmentVariables()).isEmpty();

        final Map<String, String> variables = Maps.newHashMap();
        variables.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        this.jobEntity.setEnvironmentVariables(variables);
        Assertions.assertThat(this.jobEntity.getEnvironmentVariables()).isEqualTo(variables);

        // Make sure outside modifications of collection don't effect internal class state
        variables.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        Assertions.assertThat(this.jobEntity.getEnvironmentVariables()).isNotEqualTo(variables);

        this.jobEntity.setEnvironmentVariables(variables);
        Assertions.assertThat(this.jobEntity.getEnvironmentVariables()).isEqualTo(variables);

        // Make sure this clears variables
        this.jobEntity.setEnvironmentVariables(null);
        Assertions.assertThat(this.jobEntity.getEnvironmentVariables()).isEmpty();
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetInteractive() {
        Assertions.assertThat(this.jobEntity.isInteractive()).isFalse();
        this.jobEntity.setInteractive(true);
        Assertions.assertThat(this.jobEntity.isInteractive()).isTrue();
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetResolved() {
        Assertions.assertThat(this.jobEntity.isResolved()).isFalse();
        this.jobEntity.setResolved(true);
        Assertions.assertThat(this.jobEntity.isResolved()).isTrue();
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetRequestedJobDirectoryLocation() {
        Assertions.assertThat(this.jobEntity.getRequestedJobDirectoryLocation()).isNotPresent();
        final String location = UUID.randomUUID().toString();
        this.jobEntity.setRequestedJobDirectoryLocation(location);
        Assertions.assertThat(this.jobEntity.getRequestedJobDirectoryLocation()).isPresent().contains(location);
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetJobDirectoryLocation() {
        Assertions.assertThat(this.jobEntity.getJobDirectoryLocation()).isNotPresent();
        final String location = UUID.randomUUID().toString();
        this.jobEntity.setJobDirectoryLocation(location);
        Assertions.assertThat(this.jobEntity.getJobDirectoryLocation()).isPresent().contains(location);
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetRequestedAgentConfigExt() {
        Assertions.assertThat(this.jobEntity.getRequestedAgentConfigExt()).isNotPresent();
        final String ext = "{\"" + UUID.randomUUID().toString() + "\": \"" + UUID.randomUUID().toString() + "\"}";
        this.jobEntity.setRequestedAgentConfigExt(ext);
        Assertions.assertThat(this.jobEntity.getRequestedAgentConfigExt()).isPresent().contains(ext);
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetRequestedAgentEnvironmentExt() {
        Assertions.assertThat(this.jobEntity.getRequestedAgentEnvironmentExt()).isNotPresent();
        final String ext = "{\"" + UUID.randomUUID().toString() + "\": \"" + UUID.randomUUID().toString() + "\"}";
        this.jobEntity.setRequestedAgentEnvironmentExt(ext);
        Assertions.assertThat(this.jobEntity.getRequestedAgentEnvironmentExt()).isPresent().contains(ext);
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetAgentVersion() {
        Assertions.assertThat(this.jobEntity.getAgentVersion()).isNotPresent();
        final String version = UUID.randomUUID().toString();
        this.jobEntity.setAgentVersion(version);
        Assertions.assertThat(this.jobEntity.getAgentVersion()).isPresent().contains(version);
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetAgentPid() {
        Assertions.assertThat(this.jobEntity.getAgentPid()).isNotPresent();
        final int pid = 31_382;
        this.jobEntity.setAgentPid(pid);
        Assertions.assertThat(this.jobEntity.getAgentPid()).isPresent().contains(pid);
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetClaimed() {
        Assertions.assertThat(this.jobEntity.isClaimed()).isFalse();
        this.jobEntity.setClaimed(true);
        Assertions.assertThat(this.jobEntity.isClaimed()).isTrue();
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetV4() {
        Assertions.assertThat(this.jobEntity.isV4()).isFalse();
        this.jobEntity.setV4(true);
        Assertions.assertThat(this.jobEntity.isV4()).isTrue();
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetRequestedArchiveLocationPrefix() {
        Assertions.assertThat(this.jobEntity.getRequestedArchiveLocationPrefix()).isNotPresent();
        final String requestedArchiveLocationPrefix = UUID.randomUUID().toString();
        this.jobEntity.setRequestedArchiveLocationPrefix(requestedArchiveLocationPrefix);
        Assertions
            .assertThat(this.jobEntity.getRequestedArchiveLocationPrefix())
            .isPresent()
            .contains(requestedArchiveLocationPrefix);
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetTimeoutUsed() {
        Assertions.assertThat(this.jobEntity.getTimeoutUsed()).isEmpty();
        this.jobEntity.setTimeoutUsed(null);
        Assertions.assertThat(this.jobEntity.getTimeoutUsed()).isEmpty();
        final int timeoutUsed = 324_323;
        this.jobEntity.setTimeoutUsed(timeoutUsed);
        Assertions.assertThat(this.jobEntity.getTimeoutUsed()).isPresent().contains(timeoutUsed);
    }

    /**
     * Test setter/getter.
     */
    @Test
    void canSetApi() {
        Assertions.assertThat(this.jobEntity.isApi()).isTrue();
        this.jobEntity.setApi(false);
        Assertions.assertThat(this.jobEntity.isApi()).isFalse();
    }

    /**
     * Test the toString method.
     */
    @Test
    void testToString() {
        Assertions.assertThat(this.jobEntity.toString()).isNotBlank();
    }
}
