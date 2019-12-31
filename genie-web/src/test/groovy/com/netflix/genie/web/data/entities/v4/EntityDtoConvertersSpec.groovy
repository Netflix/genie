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
package com.netflix.genie.web.data.entities.v4

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.common.dto.ApplicationStatus
import com.netflix.genie.common.dto.ClusterStatus
import com.netflix.genie.common.dto.CommandStatus
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.external.dtos.v4.Criterion
import com.netflix.genie.common.internal.dto.v4.AgentConfigRequest
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment
import com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria
import com.netflix.genie.common.internal.dto.v4.FinishedJob
import com.netflix.genie.common.internal.dto.v4.JobArchivalDataRequest
import com.netflix.genie.common.internal.dto.v4.JobEnvironmentRequest
import com.netflix.genie.common.internal.dto.v4.JobMetadata
import com.netflix.genie.common.internal.dto.v4.JobRequest
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieCommandNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.test.suppliers.RandomSuppliers
import com.netflix.genie.web.data.entities.ApplicationEntity
import com.netflix.genie.web.data.entities.ClusterEntity
import com.netflix.genie.web.data.entities.CommandEntity
import com.netflix.genie.web.data.entities.CriterionEntity
import com.netflix.genie.web.data.entities.FileEntity
import com.netflix.genie.web.data.entities.JobEntity
import com.netflix.genie.web.data.entities.TagEntity
import com.netflix.genie.web.data.entities.projections.v4.FinishedJobProjection
import com.netflix.genie.web.data.entities.projections.v4.JobSpecificationProjection
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant
import java.util.function.Consumer
import java.util.stream.Stream

/**
 * Specifications for {@link EntityDtoConverters}.
 *
 * @author tgianos
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
        entity.setStatus(ApplicationStatus.ACTIVE.name())

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
        entity.setStatus(ClusterStatus.TERMINATED.name())
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
        entity.setStatus(CommandStatus.DEPRECATED.name())
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
        def requestedArchiveLocationPrefix = UUID.randomUUID().toString()

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

        def jobArchivalDataRequest = new JobArchivalDataRequest.Builder()
            .withRequestedArchiveLocationPrefix(requestedArchiveLocationPrefix)
            .build()

        def jobEnvironmentRequest = new JobEnvironmentRequest.Builder()
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
            jobEnvironmentRequest,
            agentConfigRequest,
            null
        )
        def jobRequest1 = new JobRequest(
            null,
            new ExecutionEnvironment(configs, dependencies, setupFile),
            commandArgs,
            jobMetadata,
            executionResourceCriteria,
            jobEnvironmentRequest,
            agentConfigRequest,
            null
        )

        def jobRequest2 = new JobRequest(
            null,
            new ExecutionEnvironment(configs, dependencies, setupFile),
            commandArgs,
            jobMetadata,
            executionResourceCriteria,
            jobEnvironmentRequest,
            agentConfigRequest,
            jobArchivalDataRequest
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

        when:
        jobEntity.setRequestedArchiveLocationPrefix(requestedArchiveLocationPrefix)
        jobRequestResult = EntityDtoConverters.toV4JobRequestDto(jobEntity)

        then:
        jobRequestResult == jobRequest2
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
        def jobArgs = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
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
        def executableArgs = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def commandEntity = Mock(CommandEntity) {
            1 * getUniqueId() >> commandId
            1 * getConfigs() >> commandConfigs
                .collect({ config -> new FileEntity(config) })
                .toSet()
            1 * getDependencies() >> commandDependencies
                .collect({ dependency -> new FileEntity(dependency) })
                .toSet()
            1 * getSetupFile() >> Optional.ofNullable(new FileEntity(commandSetupFile))
            1 * getExecutable() >> executableArgs
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
        def timeout = 242_383

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
        1 * jobSpecificationProjection.getArchiveLocation() >> Optional.of(UUID.randomUUID().toString())
        1 * jobSpecificationProjection.getJobDirectoryLocation() >> Optional.of(jobDirectoryLocation)
        1 * jobSpecificationProjection.getApplications() >> applications
        1 * jobSpecificationProjection.getCommandArgs() >> jobArgs
        1 * jobSpecificationProjection.isInteractive() >> interactive
        1 * jobSpecificationProjection.getConfigs() >> jobConfigs
            .collect({ config -> new FileEntity(config) })
            .toSet()
        1 * jobSpecificationProjection.getDependencies() >> jobDependencies
            .collect({ dependency -> new FileEntity(dependency) })
            .toSet()
        1 * jobSpecificationProjection.getSetupFile() >> Optional.ofNullable(jobSetupFile)
        1 * jobSpecificationProjection.getEnvironmentVariables() >> environmentVariables
        1 * jobSpecificationProjection.getTimeoutUsed() >> Optional.ofNullable(timeout)
        jobSpecification.isInteractive()
        jobSpecification.getExecutableArgs() == executableArgs
        jobSpecification.getJobArgs() == jobArgs
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
        jobSpecification.getTimeout().orElse(null) == timeout
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

    def "Can convert FinishedJobProjection to FinishedJob DTO lacking all optional fields"() {

        def created = Instant.now()

        CriterionEntity criterion = Mock(CriterionEntity) {
            getUniqueId() >> Optional.of("cId")
            getName() >> Optional.of("cName")
            getVersion() >> Optional.of("cVersion")
            getStatus() >> Optional.of("cStatus")
            getTags() >> Sets.newHashSet()
        }

        FinishedJobProjection p = Mock(FinishedJobProjection) {
            getUniqueId() >> "id"
            getName() >> "name"
            getUser() >> "user"
            getVersion() >> "version"
            getCreated() >> created
            getStatus() >> JobStatus.SUCCEEDED
            getCommandArgs() >> ["foo", "bar"]
            getCommandCriterion() >> criterion
            getClusterCriteria() >> [criterion]
            getStarted() >> Optional.empty()
            getFinished() >> Optional.empty()
            getDescription() >> Optional.empty()
            getGrouping() >> Optional.empty()
            getGroupingInstance() >> Optional.empty()
            getStatusMsg() >> Optional.empty()
            getRequestedMemory() >> Optional.empty()
            getRequestApiClientHostname() >> Optional.empty()
            getRequestApiClientUserAgent() >> Optional.empty()
            getRequestAgentClientHostname() >> Optional.empty()
            getRequestAgentClientVersion() >> Optional.empty()
            getNumAttachments() >> Optional.empty()
            getExitCode() >> Optional.empty()
            getArchiveLocation() >> Optional.empty()
            getMemoryUsed() >> Optional.empty()
            getMetadata() >> Optional.empty()
            getCommand() >> Optional.empty()
            getCluster() >> Optional.empty()
            getApplications() >> Lists.newArrayList()
            getTags() >> Sets.newHashSet()
        }

        when:
        FinishedJob dto = EntityDtoConverters.toFinishedJobDto(p)

        then:
        dto.getUniqueId() == "id"
        dto.getName() == "name"
        dto.getUser() == "user"
        dto.getVersion() == "version"
        dto.getCreated() == created
        dto.getStatus() == JobStatus.SUCCEEDED
        dto.getCommandArgs() == ["foo", "bar"]
        dto.getCommandCriterion() != null
        !dto.getClusterCriteria().isEmpty()
        !dto.getStarted().isPresent()
        !dto.getFinished().isPresent()
        !dto.getDescription().isPresent()
        !dto.getGrouping().isPresent()
        !dto.getGroupingInstance().isPresent()
        !dto.getStatusMessage().isPresent()
        !dto.getRequestedMemory().isPresent()
        !dto.getRequestApiClientHostname().isPresent()
        !dto.getRequestApiClientUserAgent().isPresent()
        !dto.getRequestAgentClientHostname().isPresent()
        !dto.getRequestAgentClientVersion().isPresent()
        !dto.getNumAttachments().isPresent()
        !dto.getExitCode().isPresent()
        !dto.getArchiveLocation().isPresent()
        !dto.getMemoryUsed().isPresent()
        !dto.getMetadata().isPresent()
        !dto.getCommand().isPresent()
        !dto.getCluster().isPresent()
        dto.getApplications().isEmpty()
        dto.getTags().isEmpty()
    }

    def "Can convert FinishedJobProjection to FinishedJob DTO"() {
        def created = Instant.now()
        def started = created + 5
        def finished = created + 60

        CriterionEntity criterion = Mock(CriterionEntity) {
            getUniqueId() >> Optional.of("cId")
            getName() >> Optional.of("cName")
            getVersion() >> Optional.of("cVersion")
            getStatus() >> Optional.of("cStatus")
            getTags() >> Sets.newHashSet()
        }

        CommandEntity commandEntity = Mock(CommandEntity) {
            getUser() >> "command_user"
            getUniqueId() >> "command_id"
            getName() >> "command_name"
            getVersion() >> "command_version"
            getStatus() >> CommandStatus.ACTIVE.name()
            getExecutable() >> ["spark"]
            getTags() >> []
            getDescription() >> Optional.empty()
            getMetadata() >> Optional.empty()
            getSetupFile() >> Optional.empty()
            getConfigs() >> []
            getDependencies() >> []
            getMemory() >> Optional.empty()
            getCheckDelay() >> 100
        }

        ClusterEntity clusterEntity = Mock(ClusterEntity) {
            getUser() >> "cluster_user"
            getUniqueId() >> "cluster_id"
            getName() >> "cluster_name"
            getVersion() >> "cluster_version"
            getTags() >> []
            getDescription() >> Optional.empty()
            getMetadata() >> Optional.empty()
            getSetupFile() >> Optional.empty()
            getConfigs() >> []
            getDependencies() >> []
            getStatus() >> ClusterStatus.UP.name()
        }

        ApplicationEntity applicationEntity = Mock(ApplicationEntity) {
            getUser() >> "app_user"
            getUniqueId() >> "app_id"
            getName() >> "app_name"
            getVersion() >> "app_version"
            getStatus() >> ApplicationStatus.ACTIVE.name()
            getTags() >> []
            getDescription() >> Optional.empty()
            getMetadata() >> Optional.empty()
            getSetupFile() >> Optional.empty()
            getConfigs() >> []
            getDependencies() >> []
            getType() >> Optional.empty()
        }

        TagEntity jobTag1 = Mock(TagEntity) {
            getTag() >> "tag1"
        }

        TagEntity jobTag2 = Mock(TagEntity) {
            getTag() >> "tag2"
        }

        FinishedJobProjection p = Mock(FinishedJobProjection) {
            getUniqueId() >> "id"
            getName() >> "name"
            getUser() >> "user"
            getVersion() >> "version"
            getCreated() >> created
            getStatus() >> JobStatus.SUCCEEDED.name()
            getCommandArgs() >> ["foo", "bar"]
            getCommandCriterion() >> criterion
            getClusterCriteria() >> [criterion]
            getStarted() >> Optional.of(started)
            getFinished() >> Optional.of(finished)
            getDescription() >> Optional.of("description")
            getGrouping() >> Optional.of("group")
            getGroupingInstance() >> Optional.of("group_instance")
            getStatusMsg() >> Optional.of("status message")
            getRequestedMemory() >> Optional.of(512)
            getRequestApiClientHostname() >> Optional.of("api_client_host")
            getRequestApiClientUserAgent() >> Optional.of("apl_client_user-agent")
            getRequestAgentClientHostname() >> Optional.of("agent_client_host")
            getRequestAgentClientVersion() >> Optional.of("agent_client_version")
            getNumAttachments() >> Optional.of(3)
            getExitCode() >> Optional.of(127)
            getArchiveLocation() >> Optional.of("s3://bucket/prefix/job")
            getMemoryUsed() >> Optional.of(1024)
            getMetadata() >> Optional.of("{ \"foo\" : \"bar\" }")
            getCommand() >> Optional.of(commandEntity)
            getCluster() >> Optional.of(clusterEntity)
            getApplications() >> Lists.newArrayList(applicationEntity)
            getTags() >> Sets.newHashSet(jobTag1, jobTag2)
        }

        when:
        FinishedJob dto = EntityDtoConverters.toFinishedJobDto(p)

        then:
        dto.getUniqueId() == "id"
        dto.getName() == "name"
        dto.getUser() == "user"
        dto.getVersion() == "version"
        dto.getCreated() == created
        dto.getStatus() == JobStatus.SUCCEEDED
        dto.getCommandArgs() == ["foo", "bar"]
        dto.getCommandCriterion() != null
        dto.getClusterCriteria().size() == 1
        dto.getStarted().isPresent()
        dto.getFinished().isPresent()
        dto.getDescription().isPresent()
        dto.getGrouping().isPresent()
        dto.getGroupingInstance().isPresent()
        dto.getStatusMessage().isPresent()
        dto.getRequestedMemory().isPresent()
        dto.getRequestApiClientHostname().isPresent()
        dto.getRequestApiClientUserAgent().isPresent()
        dto.getRequestAgentClientHostname().isPresent()
        dto.getRequestAgentClientVersion().isPresent()
        dto.getNumAttachments().isPresent()
        dto.getExitCode().isPresent()
        dto.getArchiveLocation().isPresent()
        dto.getMemoryUsed().isPresent()
        dto.getMetadata().isPresent()
        dto.getCommand().isPresent()
        dto.getCluster().isPresent()
        dto.getApplications().size() == 1
        dto.getTags().size() == 2
        dto.getTags() == Sets.newHashSet(["tag1", "tag2"])
    }

    @Unroll
    def "status conversions throw IllegalArgumentException on bad input #input"() {
        when:
        EntityDtoConverters.toApplicationStatus(input)

        then:
        thrown(IllegalArgumentException)

        when:
        EntityDtoConverters.toCommandStatus(input)

        then:
        thrown(IllegalArgumentException)

        when:
        EntityDtoConverters.toClusterStatus(input)

        then:
        thrown(IllegalArgumentException)

        when:
        EntityDtoConverters.toJobStatus(input)

        then:
        thrown(IllegalArgumentException)

        where:
        input                        | _
        ""                           | _
        null                         | _
        " "                          | _
        UUID.randomUUID().toString() | _
    }

    @Unroll
    def "can convert #input to application status #status"() {
        expect:
        EntityDtoConverters.toApplicationStatus(input) == status

        where:
        input                                             | status
        ApplicationStatus.ACTIVE.name()                   | ApplicationStatus.ACTIVE
        ApplicationStatus.ACTIVE.name().toLowerCase()     | ApplicationStatus.ACTIVE
        ApplicationStatus.INACTIVE.name()                 | ApplicationStatus.INACTIVE
        ApplicationStatus.INACTIVE.name().toLowerCase()   | ApplicationStatus.INACTIVE
        ApplicationStatus.DEPRECATED.name()               | ApplicationStatus.DEPRECATED
        ApplicationStatus.DEPRECATED.name().toLowerCase() | ApplicationStatus.DEPRECATED
    }

    @Unroll
    def "all possible ApplicationStatus values are covered"() {
        when:
        EntityDtoConverters.toApplicationStatus(input as String)

        then:
        noExceptionThrown()

        where:
        input << Stream.of(ApplicationStatus.values()).map { status -> status.name() }.collect()
    }

    @Unroll
    def "can convert #input to command status #status"() {
        expect:
        EntityDtoConverters.toCommandStatus(input) == status

        where:
        input                                         | status
        CommandStatus.ACTIVE.name()                   | CommandStatus.ACTIVE
        CommandStatus.ACTIVE.name().toLowerCase()     | CommandStatus.ACTIVE
        CommandStatus.INACTIVE.name()                 | CommandStatus.INACTIVE
        CommandStatus.INACTIVE.name().toLowerCase()   | CommandStatus.INACTIVE
        CommandStatus.DEPRECATED.name()               | CommandStatus.DEPRECATED
        CommandStatus.DEPRECATED.name().toLowerCase() | CommandStatus.DEPRECATED
    }

    @Unroll
    def "all possible CommandStatus values are covered"() {
        when:
        EntityDtoConverters.toCommandStatus(input as String)

        then:
        noExceptionThrown()

        where:
        input << Stream.of(CommandStatus.values()).map { status -> status.name() }.collect()
    }

    @Unroll
    def "can convert #input to cluster status #status"() {
        expect:
        EntityDtoConverters.toClusterStatus(input) == status

        where:
        input                                             | status
        ClusterStatus.UP.name()                           | ClusterStatus.UP
        ClusterStatus.UP.name().toLowerCase()             | ClusterStatus.UP
        ClusterStatus.TERMINATED.name()                   | ClusterStatus.TERMINATED
        ClusterStatus.TERMINATED.name().toLowerCase()     | ClusterStatus.TERMINATED
        ClusterStatus.OUT_OF_SERVICE.name()               | ClusterStatus.OUT_OF_SERVICE
        ClusterStatus.OUT_OF_SERVICE.name().toLowerCase() | ClusterStatus.OUT_OF_SERVICE
    }

    @Unroll
    def "all possible ClusterStatus values are covered"() {
        when:
        EntityDtoConverters.toClusterStatus(input as String)

        then:
        noExceptionThrown()

        where:
        input << Stream.of(ClusterStatus.values()).map { status -> status.name() }.collect()
    }

    @Unroll
    def "can convert #input to job status #status"() {
        expect:
        EntityDtoConverters.toJobStatus(input) == status

        where:
        input                                    | status
        JobStatus.ACCEPTED.name()                | JobStatus.ACCEPTED
        JobStatus.ACCEPTED.name().toLowerCase()  | JobStatus.ACCEPTED
        JobStatus.CLAIMED.name()                 | JobStatus.CLAIMED
        JobStatus.CLAIMED.name().toLowerCase()   | JobStatus.CLAIMED
        JobStatus.FAILED.name()                  | JobStatus.FAILED
        JobStatus.FAILED.name().toLowerCase()    | JobStatus.FAILED
        JobStatus.INIT.name()                    | JobStatus.INIT
        JobStatus.INIT.name().toLowerCase()      | JobStatus.INIT
        JobStatus.INVALID.name()                 | JobStatus.INVALID
        JobStatus.INVALID.name().toLowerCase()   | JobStatus.INVALID
        JobStatus.KILLED.name()                  | JobStatus.KILLED
        JobStatus.KILLED.name().toLowerCase()    | JobStatus.KILLED
        JobStatus.RESOLVED.name()                | JobStatus.RESOLVED
        JobStatus.RESOLVED.name().toLowerCase()  | JobStatus.RESOLVED
        JobStatus.RESERVED.name()                | JobStatus.RESERVED
        JobStatus.RESERVED.name().toLowerCase()  | JobStatus.RESERVED
        JobStatus.RUNNING.name()                 | JobStatus.RUNNING
        JobStatus.RUNNING.name().toLowerCase()   | JobStatus.RUNNING
        JobStatus.SUCCEEDED.name()               | JobStatus.SUCCEEDED
        JobStatus.SUCCEEDED.name().toLowerCase() | JobStatus.SUCCEEDED
    }

    @Unroll
    def "all possible JobStatus values are covered"() {
        when:
        EntityDtoConverters.toJobStatus(input as String)

        then:
        noExceptionThrown()

        where:
        input << Stream.of(JobStatus.values()).map { status -> status.name() }.collect()
    }
}
