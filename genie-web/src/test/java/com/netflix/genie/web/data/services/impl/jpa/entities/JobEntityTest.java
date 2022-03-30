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
package com.netflix.genie.web.data.services.impl.jpa.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.validation.ConstraintViolationException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Tests for {@link JobEntity}.
 *
 * @author amsharma
 * @author tgianos
 */
class JobEntityTest extends EntityTestBase {

    private static final String USER = "tgianos";
    private static final String NAME = "TomsJob";
    private static final String VERSION = "1.2.3";

    private JobEntity entity;

    @BeforeEach
    void setup() {
        this.entity = new JobEntity();
        this.entity.setUser(USER);
        this.entity.setName(NAME);
        this.entity.setVersion(VERSION);
    }

    @Test
    void testDefaultConstructor() {
        final JobEntity localJobEntity = new JobEntity();
        Assertions.assertThat(localJobEntity.getUniqueId()).isNotBlank();
    }

    @Test
    void testConstructor() {
        Assertions.assertThat(this.entity.getUniqueId()).isNotBlank();
        Assertions.assertThat(NAME).isEqualTo(this.entity.getName());
        Assertions.assertThat(USER).isEqualTo(this.entity.getUser());
        Assertions.assertThat(this.entity.getVersion()).isEqualTo(VERSION);
    }

    @Test
    void testOnCreateJob() {
        Assertions.assertThat(this.entity.getTagSearchString()).isNull();
        this.entity.onCreateJob();
        Assertions.assertThat(this.entity.getTagSearchString()).isNull();
        final TagEntity one = new TagEntity("abc");
        final TagEntity two = new TagEntity("def");
        final TagEntity three = new TagEntity("ghi");
        this.entity.setTags(Sets.newHashSet(three, two, one));
        this.entity.onCreateJob();
        Assertions.assertThat(this.entity.getTagSearchString()).isEqualTo("|abc||def||ghi|");
    }

    @Test
    void testSetGetClusterName() {
        this.testOptionalField(
            this.entity::getClusterName,
            this.entity::setClusterName,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void testSetGetCommandName() {
        this.testOptionalField(
            this.entity::getCommandName,
            this.entity::setCommandName,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void testSetGetCommandArgs() {
        Assertions.assertThat(this.entity.getCommandArgs()).isEmpty();
        this.entity.setCommandArgs(null);
        Assertions.assertThat(this.entity.getCommandArgs()).isEmpty();
        final List<String> commandArgs = Lists.newArrayList();
        this.entity.setCommandArgs(commandArgs);
        Assertions.assertThat(this.entity.getCommandArgs()).isEmpty();
        commandArgs.add(UUID.randomUUID().toString());
        this.entity.setCommandArgs(commandArgs);
        Assertions.assertThat(this.entity.getCommandArgs()).isEqualTo(commandArgs);
    }

    @Test
    void testSetGetStatus() {
        Assertions.assertThat(this.entity.getStatus()).isNull();
        this.entity.setStatus(JobStatus.KILLED.name());
        Assertions.assertThat(this.entity.getStatus()).isEqualTo(JobStatus.KILLED.name());
    }

    @Test
    void testSetGetStatusMsg() {
        this.testOptionalField(this.entity::getStatusMsg, this.entity::setStatusMsg, UUID.randomUUID().toString());
    }

    @Test
    void testSetGetStarted() {
        this.testOptionalField(this.entity::getStarted, this.entity::setStarted, Instant.ofEpochMilli(123453L));
    }

    @Test
    void testSetGetFinished() {
        this.testOptionalField(this.entity::getFinished, this.entity::setFinished, Instant.ofEpochMilli(123453L));
    }

    @Test
    void testSetGetArchiveLocation() {
        this.testOptionalField(
            this.entity::getArchiveLocation,
            this.entity::setArchiveLocation,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void testSetNotifiedJobStatus() {
        final JobEntity localJobEntity = new JobEntity();
        Assertions.assertThat(localJobEntity.getNotifiedJobStatus()).isNotPresent();
        localJobEntity.setNotifiedJobStatus(JobStatus.RUNNING.name());
        Assertions.assertThat(localJobEntity.getNotifiedJobStatus()).isPresent().contains(JobStatus.RUNNING.name());
    }

    @Test
    void testSetGetTags() {
        Assertions.assertThat(this.entity.getTags()).isEmpty();
        final TagEntity one = new TagEntity("someTag");
        final TagEntity two = new TagEntity("someOtherTag");
        final Set<TagEntity> tags = Sets.newHashSet(one, two);
        this.entity.setTags(tags);
        Assertions.assertThat(this.entity.getTags()).isEqualTo(tags);

        this.entity.setTags(null);
        Assertions.assertThat(this.entity.getTags()).isEmpty();
    }

    @Test
    void testValidate() {
        this.validate(this.entity);
    }

    @Test
    void testValidateBadSuperClass() {
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(new JobEntity()));
    }

    @Test
    void canSetCluster() {
        final ClusterEntity cluster = new ClusterEntity();
        final String clusterName = UUID.randomUUID().toString();
        cluster.setName(clusterName);
        Assertions.assertThat(this.entity.getCluster()).isNotPresent();
        Assertions.assertThat(this.entity.getClusterName()).isNotPresent();
        this.entity.setCluster(cluster);
        Assertions.assertThat(this.entity.getCluster()).isPresent().contains(cluster);
        Assertions.assertThat(this.entity.getClusterName()).isPresent().contains(clusterName);

        this.entity.setCluster(null);
        Assertions.assertThat(this.entity.getCluster()).isNotPresent();
        Assertions.assertThat(this.entity.getClusterName()).isNotPresent();
    }

    @Test
    void canSetCommand() {
        final CommandEntity command = new CommandEntity();
        final String commandName = UUID.randomUUID().toString();
        command.setName(commandName);
        Assertions.assertThat(this.entity.getCommand()).isNotPresent();
        Assertions.assertThat(this.entity.getCommandName()).isNotPresent();
        this.entity.setCommand(command);
        Assertions.assertThat(this.entity.getCommand()).isPresent().contains(command);
        Assertions.assertThat(this.entity.getCommandName()).isPresent().contains(commandName);

        this.entity.setCommand(null);
        Assertions.assertThat(this.entity.getCommand()).isNotPresent();
        Assertions.assertThat(this.entity.getCommandName()).isNotPresent();
    }

    @Test
    void canSetApplications() {
        final ApplicationEntity application1 = new ApplicationEntity();
        application1.setUniqueId(UUID.randomUUID().toString());
        final ApplicationEntity application2 = new ApplicationEntity();
        application2.setUniqueId(UUID.randomUUID().toString());
        final ApplicationEntity application3 = new ApplicationEntity();
        application3.setUniqueId(UUID.randomUUID().toString());
        final List<ApplicationEntity> applications = Lists.newArrayList(application1, application2, application3);

        Assertions.assertThat(this.entity.getApplications()).isEmpty();
        this.entity.setApplications(applications);
        Assertions.assertThat(this.entity.getApplications()).isEqualTo(applications);
    }

    @Test
    void canSetAgentHostName() {
        this.testOptionalField(
            this.entity::getAgentHostname,
            this.entity::setAgentHostname,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void canSetProcessId() {
        this.testOptionalField(this.entity::getProcessId, this.entity::setProcessId, 352);
    }

    @Test
    void canSetExitCode() {
        this.testOptionalField(this.entity::getExitCode, this.entity::setExitCode, 80072043);
    }

    @Test
    void canSetMemoryUsed() {
        this.testOptionalField(this.entity::getMemoryUsed, this.entity::setMemoryUsed, 10_240);
    }

    @Test
    void canSetRequestApiClientHostname() {
        this.testOptionalField(
            this.entity::getRequestApiClientHostname,
            this.entity::setRequestApiClientHostname,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void canSetRequestApiClientUserAgent() {
        this.testOptionalField(
            this.entity::getRequestApiClientUserAgent,
            this.entity::setRequestApiClientUserAgent,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void canSetRequestAgentClientHostname() {
        this.testOptionalField(
            this.entity::getRequestAgentClientHostname,
            this.entity::setRequestAgentClientHostname,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void canSetRequestAgentClientVersion() {
        this.testOptionalField(
            this.entity::getRequestAgentClientVersion,
            this.entity::setRequestAgentClientVersion,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void canSetRequestAgentClientPid() {
        this.testOptionalField(this.entity::getRequestAgentClientPid, this.entity::setRequestAgentClientPid, 28_000);
    }

    @Test
    void canSetNumAttachments() {
        this.testOptionalField(this.entity::getNumAttachments, this.entity::setNumAttachments, 380_208);
    }

    @Test
    void canSetTotalSizeOfAttachments() {
        this.testOptionalField(
            this.entity::getTotalSizeOfAttachments,
            this.entity::setTotalSizeOfAttachments,
            90832432L
        );
    }

    @Test
    void canSetStdOutSize() {
        this.testOptionalField(this.entity::getStdOutSize, this.entity::setStdOutSize, 90334432L);
    }

    @Test
    void canSetStdErrSize() {
        this.testOptionalField(this.entity::getStdErrSize, this.entity::setStdErrSize, 9089932L);
    }

    @Test
    void canSetGroup() {
        this.testOptionalField(
            this.entity::getGenieUserGroup,
            this.entity::setGenieUserGroup,
            UUID.randomUUID().toString()
        );
    }

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

        this.entity.setClusterCriteria(clusterCriteria);
        Assertions.assertThat(this.entity.getClusterCriteria()).isEqualTo(clusterCriteria);
    }

    @Test
    void canSetNullClusterCriteria() {
        this.entity.setClusterCriteria(null);
        Assertions.assertThat(this.entity.getClusterCriteria()).isEmpty();
    }

    @Test
    void canSetConfigs() {
        final Set<FileEntity> configs = Sets.newHashSet(new FileEntity(UUID.randomUUID().toString()));
        this.entity.setConfigs(configs);
        Assertions.assertThat(this.entity.getConfigs()).isEqualTo(configs);
    }

    @Test
    void canSetNullConfigs() {
        this.entity.setConfigs(null);
        Assertions.assertThat(this.entity.getConfigs()).isEmpty();
    }

    @Test
    void canSetDependencies() {
        final Set<FileEntity> dependencies = Sets.newHashSet(new FileEntity(UUID.randomUUID().toString()));
        this.entity.setDependencies(dependencies);
        Assertions.assertThat(this.entity.getDependencies()).isEqualTo(dependencies);
    }

    @Test
    void canSetNullDependencies() {
        this.entity.setDependencies(null);
        Assertions.assertThat(this.entity.getDependencies()).isEmpty();
    }

    @Test
    void canSetArchivingDisabled() {
        this.entity.setArchivingDisabled(true);
        Assertions.assertThat(this.entity.isArchivingDisabled()).isTrue();
    }

    @Test
    void canSetEmail() {
        this.testOptionalField(this.entity::getEmail, this.entity::setEmail, UUID.randomUUID().toString());
    }

    @Test
    void canSetCommandCriteria() {
        final Set<TagEntity> tags = Sets.newHashSet(
            new TagEntity(UUID.randomUUID().toString()),
            new TagEntity(UUID.randomUUID().toString())
        );

        final CriterionEntity commandCriterion = new CriterionEntity(null, null, null, null, tags);
        this.entity.setCommandCriterion(commandCriterion);
        Assertions.assertThat(this.entity.getCommandCriterion()).isEqualTo(commandCriterion);
    }

    @Test
    void canSetSetupFile() {
        final FileEntity setupFileEntity = new FileEntity(UUID.randomUUID().toString());
        this.entity.setSetupFile(setupFileEntity);
        Assertions.assertThat(this.entity.getSetupFile()).isPresent().contains(setupFileEntity);
    }

    @Test
    void canSetTags() {
        final TagEntity one = new TagEntity(UUID.randomUUID().toString());
        final TagEntity two = new TagEntity(UUID.randomUUID().toString());
        final Set<TagEntity> tags = Sets.newHashSet(one, two);

        this.entity.setTags(tags);
        Assertions.assertThat(this.entity.getTags()).isEqualTo(tags);

        this.entity.setTags(null);
        Assertions.assertThat(this.entity.getTags()).isEmpty();
    }

    @Test
    void canSetRequestedCpu() {
        this.testOptionalField(this.entity::getRequestedCpu, this.entity::setRequestedCpu, 16);
    }

    @Test
    void canSetRequestedMemory() {
        this.testOptionalField(this.entity::getRequestedMemory, this.entity::setRequestedMemory, 2048);
    }

    @Test
    void canSetRequestedApplications() {
        final String application = UUID.randomUUID().toString();
        final List<String> applications = Lists.newArrayList(application);
        this.entity.setRequestedApplications(applications);
        Assertions.assertThat(this.entity.getRequestedApplications()).isEqualTo(applications);
    }

    @Test
    void canSetRequestedTimeout() {
        this.testOptionalField(this.entity::getRequestedTimeout, this.entity::setRequestedTimeout, 28023423);
    }

    @Test
    void canSetRequestedEnvironmentVariables() {
        Assertions.assertThat(this.entity.getRequestedEnvironmentVariables()).isEmpty();

        this.entity.setRequestedEnvironmentVariables(null);
        Assertions.assertThat(this.entity.getRequestedEnvironmentVariables()).isEmpty();

        final Map<String, String> variables = Maps.newHashMap();
        variables.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        this.entity.setRequestedEnvironmentVariables(variables);
        Assertions.assertThat(this.entity.getRequestedEnvironmentVariables()).isEqualTo(variables);

        // Make sure outside modifications of collection don't affect internal class state
        variables.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        Assertions.assertThat(this.entity.getRequestedEnvironmentVariables()).isNotEqualTo(variables);

        this.entity.setRequestedEnvironmentVariables(variables);
        Assertions.assertThat(this.entity.getRequestedEnvironmentVariables()).isEqualTo(variables);

        // Make sure this clears variables
        this.entity.setRequestedEnvironmentVariables(null);
        Assertions.assertThat(this.entity.getRequestedEnvironmentVariables()).isEmpty();
    }

    @Test
    void canSetEnvironmentVariables() {
        Assertions.assertThat(this.entity.getEnvironmentVariables()).isEmpty();

        this.entity.setEnvironmentVariables(null);
        Assertions.assertThat(this.entity.getEnvironmentVariables()).isEmpty();

        final Map<String, String> variables = Maps.newHashMap();
        variables.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        this.entity.setEnvironmentVariables(variables);
        Assertions.assertThat(this.entity.getEnvironmentVariables()).isEqualTo(variables);

        // Make sure outside modifications of collection don't affect internal class state
        variables.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        Assertions.assertThat(this.entity.getEnvironmentVariables()).isNotEqualTo(variables);

        this.entity.setEnvironmentVariables(variables);
        Assertions.assertThat(this.entity.getEnvironmentVariables()).isEqualTo(variables);

        // Make sure this clears variables
        this.entity.setEnvironmentVariables(null);
        Assertions.assertThat(this.entity.getEnvironmentVariables()).isEmpty();
    }

    @Test
    void canSetInteractive() {
        Assertions.assertThat(this.entity.isInteractive()).isFalse();
        this.entity.setInteractive(true);
        Assertions.assertThat(this.entity.isInteractive()).isTrue();
    }

    @Test
    void canSetResolved() {
        Assertions.assertThat(this.entity.isResolved()).isFalse();
        this.entity.setResolved(true);
        Assertions.assertThat(this.entity.isResolved()).isTrue();
    }

    @Test
    void canSetRequestedJobDirectoryLocation() {
        this.testOptionalField(
            this.entity::getRequestedJobDirectoryLocation,
            this.entity::setRequestedJobDirectoryLocation,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void canSetJobDirectoryLocation() {
        this.testOptionalField(
            this.entity::getJobDirectoryLocation,
            this.entity::setJobDirectoryLocation,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void canSetRequestedAgentConfigExt() {
        this.testOptionalField(
            this.entity::getRequestedAgentConfigExt,
            this.entity::setRequestedAgentConfigExt,
            Mockito.mock(JsonNode.class)
        );
    }

    @Test
    void canSetRequestedAgentEnvironmentExt() {
        this.testOptionalField(
            this.entity::getRequestedAgentEnvironmentExt,
            this.entity::setRequestedAgentEnvironmentExt,
            Mockito.mock(JsonNode.class)
        );
    }

    @Test
    void canSetAgentVersion() {
        this.testOptionalField(
            this.entity::getAgentVersion,
            this.entity::setAgentVersion,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void canSetAgentPid() {
        this.testOptionalField(this.entity::getAgentPid, this.entity::setAgentPid, 31_382);
    }

    @Test
    void canSetClaimed() {
        Assertions.assertThat(this.entity.isClaimed()).isFalse();
        this.entity.setClaimed(true);
        Assertions.assertThat(this.entity.isClaimed()).isTrue();
    }

    @Test
    void canSetTimeoutUsed() {
        this.testOptionalField(this.entity::getTimeoutUsed, this.entity::setTimeoutUsed, 324_323);
    }

    @Test
    void canSetApi() {
        Assertions.assertThat(this.entity.isApi()).isTrue();
        this.entity.setApi(false);
        Assertions.assertThat(this.entity.isApi()).isFalse();
    }

    @Test
    void canSetArchiveStatus() {
        this.testOptionalField(
            this.entity::getArchiveStatus,
            this.entity::setArchiveStatus,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void canSetRequestedLauncherExt() {
        this.testOptionalField(
            this.entity::getRequestedLauncherExt,
            this.entity::setRequestedLauncherExt,
            Mockito.mock(JsonNode.class)
        );
    }

    @Test
    void canSetLauncherExt() {
        this.testOptionalField(this.entity::getLauncherExt, this.entity::setLauncherExt, Mockito.mock(JsonNode.class));
    }

    @Test
    void canSetCpuUsed() {
        this.testOptionalField(this.entity::getCpuUsed, this.entity::setCpuUsed, 42);
    }

    @Test
    void canSetGpuRequested() {
        this.testOptionalField(this.entity::getRequestedGpu, this.entity::setRequestedGpu, 24);
    }

    @Test
    void canSetGpuUsed() {
        this.testOptionalField(this.entity::getGpuUsed, this.entity::setGpuUsed, 242524);
    }

    @Test
    void canSetRequestedDiskMb() {
        this.testOptionalField(this.entity::getRequestedDiskMb, this.entity::setRequestedDiskMb, 1_5234);
    }

    @Test
    void canSetDiskMbUsed() {
        this.testOptionalField(this.entity::getDiskMbUsed, this.entity::setDiskMbUsed, 1_234);
    }

    @Test
    void canSetRequestedNetworkMbps() {
        this.testOptionalField(this.entity::getRequestedNetworkMbps, this.entity::setRequestedNetworkMbps, 52);
    }

    @Test
    void canSetNetworkMbpsUsed() {
        this.testOptionalField(this.entity::getNetworkMbpsUsed, this.entity::setNetworkMbpsUsed, 521);
    }

    @Test
    void canSetRequestedImageName() {
        this.testOptionalField(
            this.entity::getRequestedImageName,
            this.entity::setRequestedImageName,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void canSetImageNameUsed() {
        this.testOptionalField(
            this.entity::getImageNameUsed,
            this.entity::setImageNameUsed,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void canSetRequestedImageTag() {
        this.testOptionalField(
            this.entity::getRequestedImageTag,
            this.entity::setRequestedImageTag,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void canSetImageTagUsed() {
        this.testOptionalField(
            this.entity::getImageTagUsed,
            this.entity::setImageTagUsed,
            UUID.randomUUID().toString()
        );
    }

    @Test
    void testToString() {
        Assertions.assertThat(this.entity.toString()).isNotBlank();
    }

    private <T> void testOptionalField(
        final Supplier<Optional<T>> getter,
        final Consumer<T> setter,
        final T testValue
    ) {
        Assertions.assertThat(getter.get()).isNotPresent();
        setter.accept(null);
        Assertions.assertThat(getter.get()).isNotPresent();
        setter.accept(testValue);
        Assertions.assertThat(getter.get()).isPresent().contains(testValue);
    }
}
