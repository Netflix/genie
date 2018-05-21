/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.web.jpa.entities.v4

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.common.dto.ApplicationStatus
import com.netflix.genie.common.dto.ClusterStatus
import com.netflix.genie.common.dto.CommandStatus
import com.netflix.genie.common.internal.dto.v4.AgentConfigRequest
import com.netflix.genie.common.internal.dto.v4.AgentEnvironmentRequest
import com.netflix.genie.common.internal.dto.v4.Criterion
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment
import com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria
import com.netflix.genie.common.internal.dto.v4.JobMetadata
import com.netflix.genie.common.internal.dto.v4.JobRequest
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieCommandNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.test.suppliers.RandomSuppliers
import com.netflix.genie.web.jpa.entities.ApplicationEntity
import com.netflix.genie.web.jpa.entities.ClusterEntity
import com.netflix.genie.web.jpa.entities.CommandEntity
import com.netflix.genie.web.jpa.entities.CriterionEntity
import com.netflix.genie.web.jpa.entities.FileEntity
import com.netflix.genie.web.jpa.entities.JobEntity
import com.netflix.genie.web.jpa.entities.TagEntity
import com.netflix.genie.web.jpa.entities.projections.v4.JobSpecificationProjection
import spock.lang.Specification

import java.util.function.Consumer

/**
 * Specifications for {@link EntityDtoConverters}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class EntityDtoConvertersSpec extends Specification {

    def "Can convert application entity to application v4 dto"() {
        def entity = new ApplicationEntity()
        def name = UUID.randomUUID().toString()
        entity.setName(name)
        def user = UUID.randomUUID().toString()
        entity.setUser(user)
        def version = UUID.randomUUID().toString()
        entity.setVersion(version)
        def id = UUID.randomUUID().toString()
        entity.setUniqueId(id)
        def created = entity.getCreated()
        def updated = entity.getUpdated()
        def description = UUID.randomUUID().toString()
        entity.setDescription(description)
        def metadata = "[\"" + UUID.randomUUID().toString() + "\"]"
        entity.setMetadata(metadata)
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        Set<TagEntity> tagEntities = tags.collect(
                {
                    new TagEntity(it)
                }
        )
        entity.setTags(tagEntities)
        def configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        Set<FileEntity> configEntities = configs.collect(
                {
                    new FileEntity(it)
                }
        )
        entity.setConfigs(configEntities)
        def setupFile = UUID.randomUUID().toString()
        def setupFileEntity = new FileEntity()
        setupFileEntity.setFile(setupFile)
        entity.setSetupFile(setupFileEntity)
        def dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        Set<FileEntity> dependencyEntities = dependencies.collect(
                {
                    new FileEntity(it)
                }
        )
        entity.setDependencies(dependencyEntities)
        entity.setStatus(ApplicationStatus.ACTIVE)

        when:
        def application = EntityDtoConverters.toV4ApplicationDto(entity)

        then:
        application.getId() == id
        application.getMetadata().getName() == name
        application.getMetadata().getUser() == user
        application.getMetadata().getVersion() == version
        application.getCreated() == created
        application.getUpdated() == updated
        application.getMetadata().getDescription().orElseGet(RandomSuppliers.STRING) == description
        application.getMetadata().getTags() == tags
        application.getResources().getConfigs() == configs
        application.getResources().getSetupFile().orElseGet(RandomSuppliers.STRING) == setupFile
        application.getResources().getDependencies() == dependencies
        application.getMetadata().getStatus() == ApplicationStatus.ACTIVE
        application.getMetadata().getMetadata().isPresent()
        GenieObjectMapper.getMapper().writeValueAsString(application.getMetadata().getMetadata().get()) == metadata
    }

    def "Can convert cluster entity to v4 cluster dto"() {
        def entity = new ClusterEntity()
        def name = UUID.randomUUID().toString()
        entity.setName(name)
        def user = UUID.randomUUID().toString()
        entity.setUser(user)
        def version = UUID.randomUUID().toString()
        entity.setVersion(version)
        entity.setStatus(ClusterStatus.TERMINATED)
        def id = UUID.randomUUID().toString()
        entity.setUniqueId(id)
        def created = entity.getCreated()
        def updated = entity.getUpdated()
        def description = UUID.randomUUID().toString()
        entity.setDescription(description)
        def metadata = "[\"" + UUID.randomUUID().toString() + "\"]"
        entity.setMetadata(metadata)
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<TagEntity> tagEntities = tags.collect(
                {
                    def tagEntity = new TagEntity()
                    tagEntity.setTag(it)
                    tagEntity
                }
        )
        entity.setTags(tagEntities)
        def configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<FileEntity> configEntities = configs.collect(
                {
                    def fileEntity = new FileEntity()
                    fileEntity.setFile(it)
                    fileEntity
                }
        )
        entity.setConfigs(configEntities)
        def dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<FileEntity> dependencyEntities = dependencies.collect(
                {
                    def fileEntity = new FileEntity()
                    fileEntity.setFile(it)
                    fileEntity
                }
        )
        entity.setDependencies(dependencyEntities)
        def setupFile = UUID.randomUUID().toString()
        entity.setSetupFile(new FileEntity(setupFile))

        when:
        def cluster = EntityDtoConverters.toV4ClusterDto(entity)

        then:
        cluster.getId() == id
        cluster.getMetadata().getName() == name
        cluster.getMetadata().getUser() == user
        cluster.getMetadata().getVersion() == version
        cluster.getMetadata().getDescription().orElseGet(RandomSuppliers.STRING) == description
        cluster.getMetadata().getStatus() == ClusterStatus.TERMINATED
        cluster.getCreated() == created
        cluster.getUpdated() == updated
        cluster.getMetadata().getTags() == tags
        cluster.getResources().getConfigs() == configs
        cluster.getResources().getDependencies() == dependencies
        cluster.getResources().getSetupFile().orElseGet(RandomSuppliers.STRING) == setupFile
        cluster.getMetadata().getMetadata().isPresent()
        GenieObjectMapper.getMapper().writeValueAsString(cluster.getMetadata().getMetadata().get()) == metadata
    }

    def "Can convert command entity to v4 command dto"() {
        def entity = new CommandEntity()
        def id = UUID.randomUUID().toString()
        entity.setUniqueId(id)
        def name = UUID.randomUUID().toString()
        entity.setName(name)
        def user = UUID.randomUUID().toString()
        entity.setUser(user)
        def version = UUID.randomUUID().toString()
        entity.setVersion(version)
        def description = UUID.randomUUID().toString()
        entity.setDescription(description)
        def created = entity.getCreated()
        def updated = entity.getUpdated()
        entity.setStatus(CommandStatus.DEPRECATED)
        def metadata = "[\"" + UUID.randomUUID().toString() + "\"]"
        entity.setMetadata(metadata)
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<TagEntity> tagEntities = tags.collect(
                {
                    new TagEntity(it)
                }
        )
        entity.setTags(tagEntities)
        def configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<FileEntity> configEntities = configs.collect(
                {
                    new FileEntity(it)
                }
        )
        entity.setConfigs(configEntities)
        def dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<FileEntity> dependencyEntities = dependencies.collect(
                {
                    new FileEntity(it)
                }
        )
        entity.setDependencies(dependencyEntities)
        def setupFile = UUID.randomUUID().toString()
        final FileEntity setupFileEntity = new FileEntity()
        setupFileEntity.setFile(setupFile)
        entity.setSetupFile(setupFileEntity)
        def executable = Lists.newArrayList(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        entity.setExecutable(executable)
        def checkDelay = 2180234L
        entity.setCheckDelay(checkDelay)
        def memory = 10_241
        entity.setMemory(memory)

        when:
        def command = EntityDtoConverters.toV4CommandDto(entity)

        then:
        command.getId() == id
        command.getMetadata().getName() == name
        command.getMetadata().getUser() == user
        command.getMetadata().getVersion() == version
        command.getMetadata().getStatus() == CommandStatus.DEPRECATED
        command.getMetadata().getDescription().orElseGet(RandomSuppliers.STRING) == description
        command.getCreated() == created
        command.getUpdated() == updated
        command.getExecutable() == executable
        command.getMetadata().getTags() == tags
        command.getResources().getSetupFile().orElseGet(RandomSuppliers.STRING) == setupFile
        command.getResources().getConfigs() == configs
        command.getResources().getDependencies() == dependencies
        command.getMemory().orElseGet(RandomSuppliers.INT) == memory
        command.getMetadata().getMetadata().isPresent()
        GenieObjectMapper.getMapper().writeValueAsString(command.getMetadata().getMetadata().get()) == metadata
        command.getCheckDelay() == checkDelay
    }

    def "Can convert job request projection to V4 job request DTO"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def description = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def metadataString = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def metadata = GenieObjectMapper
                .getMapper()
                .readTree(metadataString)
        def tags = Sets.newHashSet(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        def email = UUID.randomUUID().toString()
        def grouping = UUID.randomUUID().toString()
        def groupingInstance = UUID.randomUUID().toString()

        def interactive = true
        def commandArgs = Lists.newArrayList(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        def jobDirectoryLocation = "/tmp"

        def configs = Sets.newHashSet(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        def dependencies = Sets.newHashSet(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        def setupFile = UUID.randomUUID().toString()

        def commandCriterionId = UUID.randomUUID().toString()
        def commandCriterionName = UUID.randomUUID().toString()
        def commandCriterionVersion = UUID.randomUUID().toString()
        def commandCriterionStatus = UUID.randomUUID().toString()
        def commandCriterionTags = Sets.newHashSet(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        def commandCriterion = new Criterion.Builder()
                .withId(commandCriterionId)
                .withName(commandCriterionName)
                .withVersion(commandCriterionVersion)
                .withStatus(commandCriterionStatus)
                .withTags(commandCriterionTags)
                .build()

        def clusterCriterion0Id = UUID.randomUUID().toString()
        def clusterCriterion0Name = UUID.randomUUID().toString()
        def clusterCriterion0Version = UUID.randomUUID().toString()
        def clusterCriterion0Status = UUID.randomUUID().toString()
        def clusterCriterion0Tags = Sets.newHashSet(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        def clusterCriterion0 = new Criterion.Builder()
                .withId(clusterCriterion0Id)
                .withName(clusterCriterion0Name)
                .withVersion(clusterCriterion0Version)
                .withStatus(clusterCriterion0Status)
                .withTags(clusterCriterion0Tags)
                .build()

        def clusterCriterion1Id = UUID.randomUUID().toString()
        def clusterCriterion1Name = UUID.randomUUID().toString()
        def clusterCriterion1Version = UUID.randomUUID().toString()
        def clusterCriterion1Status = UUID.randomUUID().toString()
        def clusterCriterion1Tags = Sets.newHashSet(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        def clusterCriterion1 = new Criterion.Builder()
                .withId(clusterCriterion1Id)
                .withName(clusterCriterion1Name)
                .withVersion(clusterCriterion1Version)
                .withStatus(clusterCriterion1Status)
                .withTags(clusterCriterion1Tags)
                .build()

        def clusterCriterion2Id = UUID.randomUUID().toString()
        def clusterCriterion2Name = UUID.randomUUID().toString()
        def clusterCriterion2Version = UUID.randomUUID().toString()
        def clusterCriterion2Status = UUID.randomUUID().toString()
        def clusterCriterion2Tags = Sets.newHashSet(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        def clusterCriterion2 = new Criterion.Builder()
                .withId(clusterCriterion2Id)
                .withName(clusterCriterion2Name)
                .withVersion(clusterCriterion2Version)
                .withStatus(clusterCriterion2Status)
                .withTags(clusterCriterion2Tags)
                .build()
        def clusterCriteria = Lists.newArrayList(clusterCriterion0, clusterCriterion1, clusterCriterion2)

        def applicationIds = Lists.newArrayList(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )

        def requestedTimeout = 32_000
        def requestedMemory = 32_387
        def requestedCpu = 3

        def requestedEnvironmentVariables = ImmutableMap.of(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )

        def jobMetadata = new JobMetadata.Builder(name, user, version)
                .withDescription(description)
                .withMetadata(metadata)
                .withTags(tags)
                .withGrouping(grouping)
                .withGroupingInstance(groupingInstance)
                .withEmail(email)
                .build()

        def executionResourceCriteria = new ExecutionResourceCriteria(
                clusterCriteria,
                commandCriterion,
                applicationIds
        )

        def agentConfigRequest = new AgentConfigRequest.Builder()
                .withInteractive(interactive)
                .withArchivingDisabled(true)
                .withRequestedJobDirectoryLocation(jobDirectoryLocation)
                .withExt(metadata)
                .withTimeoutRequested(requestedTimeout)
                .build()

        def agentEnvironmentRequest = new AgentEnvironmentRequest.Builder()
                .withExt(metadata)
                .withRequestedJobMemory(requestedMemory)
                .withRequestedJobCpu(requestedCpu)
                .withRequestedEnvironmentVariables(requestedEnvironmentVariables)
                .build()

        def jobRequest0 = new JobRequest(
                id,
                new ExecutionEnvironment(configs, dependencies, setupFile),
                commandArgs,
                jobMetadata,
                executionResourceCriteria,
                agentEnvironmentRequest,
                agentConfigRequest
        )
        def jobRequest1 = new JobRequest(
                null,
                new ExecutionEnvironment(configs, dependencies, setupFile),
                commandArgs,
                jobMetadata,
                executionResourceCriteria,
                agentEnvironmentRequest,
                agentConfigRequest
        )

        def jobEntity = new JobEntity()
        jobEntity.setRequestedId(true)
        jobEntity.setUniqueId(id)
        jobEntity.setVersion(version)
        jobEntity.setName(name)
        jobEntity.setUser(user)
        jobEntity.setTags(tags.collect({ tag -> new TagEntity(tag) }).toSet())
        jobEntity.setMetadata(metadataString)
        jobEntity.setSetupFile(new FileEntity(setupFile))
        jobEntity.setRequestedAgentConfigExt(metadataString)
        jobEntity.setRequestedEnvironmentVariables(requestedEnvironmentVariables)
        jobEntity.setRequestedAgentEnvironmentExt(metadataString)
        jobEntity.setRequestedJobDirectoryLocation(jobDirectoryLocation)
        jobEntity.setCommandArgs(commandArgs)
        jobEntity.setDescription(description)
        jobEntity.setEmail(email)
        jobEntity.setGenieUserGroup(null)
        jobEntity.setGrouping(grouping)
        jobEntity.setGroupingInstance(groupingInstance)
        jobEntity.setRequestedApplications(applicationIds)
        jobEntity.setRequestedCpu(requestedCpu)
        jobEntity.setRequestedMemory(requestedMemory)
        jobEntity.setRequestedTimeout(requestedTimeout)
        jobEntity.setConfigs(configs.collect({ config -> new FileEntity(config) }).toSet())
        jobEntity.setDependencies(dependencies.collect({ dependency -> new FileEntity(dependency) }).toSet())
        jobEntity.setClusterCriteria(
                clusterCriteria.
                        collect(
                                {
                                    criterion ->
                                        new CriterionEntity(
                                                criterion.getId().orElse(null),
                                                criterion.getName().orElse(null),
                                                criterion.getVersion().orElse(null),
                                                criterion.getStatus().orElse(null),
                                                criterion.getTags().collect({ tag -> new TagEntity(tag) }).toSet()
                                        )
                                }
                        ).toList()
        )
        jobEntity.setCommandCriterion(
                new CriterionEntity(
                        commandCriterionId,
                        commandCriterionName,
                        commandCriterionVersion,
                        commandCriterionStatus,
                        commandCriterionTags.collect({ tag -> new TagEntity(tag) }).toSet()
                )
        )
        jobEntity.setInteractive(interactive)
        jobEntity.setArchivingDisabled(true)

        def jobRequestResult

        when:
        jobRequestResult = EntityDtoConverters.toV4JobRequestDto(jobEntity)

        then:
        jobRequestResult == jobRequest0

        when:
        jobEntity.setRequestedId(false)
        jobEntity.setUniqueId(UUID.randomUUID().toString())
        jobRequestResult = EntityDtoConverters.toV4JobRequestDto(jobEntity)

        then:
        jobRequestResult == jobRequest1
    }

    def "Can set JSON field from string"() {
        def json = "{\"" + UUID.randomUUID().toString() + "\": \"" + UUID.randomUUID().toString() + "\"}"
        Consumer<JsonNode> consumer = Mock(Consumer)

        when:
        EntityDtoConverters.setJsonField(json, consumer)

        then:
        1 * consumer.accept(_ as JsonNode)

        when:
        EntityDtoConverters.setJsonField("I'm not valid json", consumer)

        then:
        1 * consumer.accept(null)
    }

    def "Can set string field from json"() {
        def json = GenieObjectMapper
                .getMapper()
                .readTree("{\"" + UUID.randomUUID().toString() + "\": \"" + UUID.randomUUID().toString() + "\"}")
        Consumer<String> consumer = Mock(Consumer)

        when:
        EntityDtoConverters.setJsonField(json, consumer)

        then:
        1 * consumer.accept(_ as String)

        when:
        EntityDtoConverters.setJsonField((JsonNode) null, consumer)

        then:
        1 * consumer.accept(null)
    }

    def "Can JobSpecificationProjection to JobSpecification DTO"() {
        def id = UUID.randomUUID().toString()

        def environmentVariables = ImmutableMap.of(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        def jobDirectoryLocation = UUID.randomUUID().toString()
        def commandArgs = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def interactive = true
        def jobConfigs = Sets.newHashSet(UUID.randomUUID().toString())
        def jobDependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def jobSetupFile = null

        def clusterId = UUID.randomUUID().toString()
        def clusterConfigs = Sets.newHashSet(UUID.randomUUID().toString())
        def clusterDependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def clusterSetupFile = UUID.randomUUID().toString()
        def clusterEntity = Mock(ClusterEntity) {
            1 * getUniqueId() >> clusterId
            1 * getConfigs() >> clusterConfigs
                    .collect({ config -> new FileEntity(config) })
                    .toSet()
            1 * getDependencies() >> clusterDependencies
                    .collect({ dependency -> new FileEntity(dependency) })
                    .toSet()
            1 * getSetupFile() >> Optional.ofNullable(new FileEntity(clusterSetupFile))
        }

        def commandId = UUID.randomUUID().toString()
        def commandConfigs = Sets.newHashSet(UUID.randomUUID().toString())
        def commandDependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def commandSetupFile = UUID.randomUUID().toString()
        def executable = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def commandEntity = Mock(CommandEntity) {
            1 * getUniqueId() >> commandId
            1 * getConfigs() >> commandConfigs
                    .collect({ config -> new FileEntity(config) })
                    .toSet()
            1 * getDependencies() >> commandDependencies
                    .collect({ dependency -> new FileEntity(dependency) })
                    .toSet()
            1 * getSetupFile() >> Optional.ofNullable(new FileEntity(commandSetupFile))
            1 * getExecutable() >> executable
        }

        def application0Id = UUID.randomUUID().toString()
        def application0Configs = Sets.newHashSet(UUID.randomUUID().toString())
        def application0Dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def application0SetupFile = null
        def application0Entity = Mock(ApplicationEntity) {
            1 * getUniqueId() >> application0Id
            1 * getConfigs() >> application0Configs
                    .collect({ config -> new FileEntity(config) })
                    .toSet()
            1 * getDependencies() >> application0Dependencies
                    .collect({ dependency -> new FileEntity(dependency) })
                    .toSet()
            1 * getSetupFile() >> Optional.ofNullable(application0SetupFile)
        }

        def application1Id = UUID.randomUUID().toString()
        def application1Configs = Sets.newHashSet(UUID.randomUUID().toString())
        def application1Dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def application1SetupFile = UUID.randomUUID().toString()
        def application1Entity = Mock(ApplicationEntity) {
            1 * getUniqueId() >> application1Id
            1 * getConfigs() >> application1Configs
                    .collect({ config -> new FileEntity(config) })
                    .toSet()
            1 * getDependencies() >> application1Dependencies
                    .collect({ dependency -> new FileEntity(dependency) })
                    .toSet()
            1 * getSetupFile() >> Optional.ofNullable(new FileEntity(application1SetupFile))
        }
        def applications = Lists.newArrayList(application0Entity, application1Entity)

        def jobSpecificationProjection = Mock(JobSpecificationProjection)

        when:
        EntityDtoConverters.toJobSpecificationDto(jobSpecificationProjection)

        then:
        1 * jobSpecificationProjection.getUniqueId() >> id
        1 * jobSpecificationProjection.getCluster() >> Optional.empty()
        thrown(GenieRuntimeException)

        when:
        EntityDtoConverters.toJobSpecificationDto(jobSpecificationProjection)

        then:
        1 * jobSpecificationProjection.getUniqueId() >> id
        1 * jobSpecificationProjection.getCluster() >> Optional.of(clusterEntity)
        1 * jobSpecificationProjection.getCommand() >> Optional.empty()
        thrown(GenieRuntimeException)

        when:
        EntityDtoConverters.toJobSpecificationDto(jobSpecificationProjection)

        then:
        1 * jobSpecificationProjection.getUniqueId() >> id
        1 * jobSpecificationProjection.getCluster() >> Optional.of(clusterEntity)
        1 * jobSpecificationProjection.getCommand() >> Optional.of(commandEntity)
        1 * jobSpecificationProjection.getJobDirectoryLocation() >> Optional.empty()
        thrown(GenieRuntimeException)

        when:
        def jobSpecification = EntityDtoConverters.toJobSpecificationDto(jobSpecificationProjection)

        then:
        1 * jobSpecificationProjection.getUniqueId() >> id
        1 * jobSpecificationProjection.getCluster() >> Optional.of(clusterEntity)
        1 * jobSpecificationProjection.getCommand() >> Optional.of(commandEntity)
        1 * jobSpecificationProjection.getJobDirectoryLocation() >> Optional.of(jobDirectoryLocation)
        1 * jobSpecificationProjection.getApplications() >> applications
        1 * jobSpecificationProjection.getCommandArgs() >> commandArgs
        1 * jobSpecificationProjection.isInteractive() >> interactive
        1 * jobSpecificationProjection.getConfigs() >> jobConfigs
                .collect({ config -> new FileEntity(config) })
                .toSet()
        1 * jobSpecificationProjection.getDependencies() >> jobDependencies
                .collect({ dependency -> new FileEntity(dependency) })
                .toSet()
        1 * jobSpecificationProjection.getSetupFile() >> Optional.ofNullable(jobSetupFile)
        1 * jobSpecificationProjection.getEnvironmentVariables() >> environmentVariables
        jobSpecification.isInteractive()
        jobSpecification.getCommandArgs() == executable + commandArgs
        jobSpecification.getEnvironmentVariables() == environmentVariables
        jobSpecification.getJobDirectoryLocation() == new File(jobDirectoryLocation)
        jobSpecification.getJob() == new JobSpecification.ExecutionResource(
                id,
                new ExecutionEnvironment(
                        jobConfigs,
                        jobDependencies,
                        jobSetupFile
                )
        )
        jobSpecification.getCluster() == new JobSpecification.ExecutionResource(
                clusterId,
                new ExecutionEnvironment(
                        clusterConfigs,
                        clusterDependencies,
                        clusterSetupFile
                )
        )
        jobSpecification.getCommand() == new JobSpecification.ExecutionResource(
                commandId,
                new ExecutionEnvironment(
                        commandConfigs,
                        commandDependencies,
                        commandSetupFile
                )
        )
        jobSpecification.getApplications() == Lists.newArrayList(
                new JobSpecification.ExecutionResource(
                        application0Id,
                        new ExecutionEnvironment(
                                application0Configs,
                                application0Dependencies,
                                application0SetupFile
                        )
                ),
                new JobSpecification.ExecutionResource(
                        application1Id,
                        new ExecutionEnvironment(
                                application1Configs,
                                application1Dependencies,
                                application1SetupFile
                        )
                )
        )
    }

    def "Invalid Job Specification throws exceptions"() {
        def id = UUID.randomUUID().toString()
        def jobSpecificationProjection = Mock(JobSpecificationProjection)

        when:
        EntityDtoConverters.toJobSpecificationDto(jobSpecificationProjection)

        then:
        1 * jobSpecificationProjection.getUniqueId() >> id
        1 * jobSpecificationProjection.getCluster() >> Optional.empty()
        thrown(GenieClusterNotFoundException)

        when:
        EntityDtoConverters.toJobSpecificationDto(jobSpecificationProjection)

        then:
        1 * jobSpecificationProjection.getUniqueId() >> id
        1 * jobSpecificationProjection.getCluster() >> Optional.of(Mock(ClusterEntity))
        1 * jobSpecificationProjection.getCommand() >> Optional.empty()
        thrown(GenieCommandNotFoundException)

        when:
        EntityDtoConverters.toJobSpecificationDto(jobSpecificationProjection)

        then:
        1 * jobSpecificationProjection.getUniqueId() >> id
        1 * jobSpecificationProjection.getCluster() >> Optional.of(Mock(ClusterEntity))
        1 * jobSpecificationProjection.getCommand() >> Optional.of(Mock(CommandEntity))
        1 * jobSpecificationProjection.getJobDirectoryLocation() >> Optional.empty()
        thrown(GenieRuntimeException)
    }
}
