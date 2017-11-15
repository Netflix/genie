/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.core.jpa.services

import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.common.dto.*
import com.netflix.genie.core.jpa.entities.*
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.test.suppliers.RandomSuppliers
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for the JpaServiceUtils class.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Category(UnitTest.class)
class JpaServiceUtilsSpec extends Specification {

    def "Can convert application entity to application dto"() {
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
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        Set<TagEntity> tagEntities = tags.collect(
                {
                    def tagEntity = new TagEntity()
                    tagEntity.setTag(it)
                    tagEntity
                }
        )
        entity.setTags(tagEntities)
        def configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        Set<FileEntity> configEntities = configs.collect(
                {
                    def fileEntity = new FileEntity()
                    fileEntity.setFile(it)
                    fileEntity
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
                    def fileEntity = new FileEntity()
                    fileEntity.setFile(it)
                    fileEntity
                }
        )
        entity.setDependencies(dependencyEntities)
        entity.setStatus(ApplicationStatus.ACTIVE)

        when:
        def application = JpaServiceUtils.toApplicationDto(entity)

        then:
        application.getId().orElseGet(RandomSuppliers.STRING) == id
        application.getName() == name
        application.getUser() == user
        application.getVersion() == version
        application.getCreated().orElseGet(RandomSuppliers.DATE) == created
        application.getUpdated().orElseGet(RandomSuppliers.DATE) == updated
        application.getDescription().orElseGet(RandomSuppliers.STRING) == description
        application.getTags() == tags
        application.getConfigs() == configs
        application.getSetupFile().orElseGet(RandomSuppliers.STRING) == setupFile
        application.getDependencies() == dependencies
        application.getStatus() == ApplicationStatus.ACTIVE
    }

    def "Can convert cluster entity to cluster dto"() {
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
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<TagEntity> tagEntities = tags.collect(
                {
                    def tagEntity = new TagEntity()
                    tagEntity.setTag(it)
                    tagEntity
                }
        )
        entity.setTags(tagEntities)
        def confs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<FileEntity> configs = confs.collect(
                {
                    def fileEntity = new FileEntity()
                    fileEntity.setFile(it)
                    fileEntity
                }
        )
        entity.setConfigs(configs)
        def dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<FileEntity> dependencyEntities = dependencies.collect(
                {
                    def fileEntity = new FileEntity()
                    fileEntity.setFile(it)
                    fileEntity
                }
        )
        entity.setDependencies(dependencyEntities)

        when:
        def cluster = JpaServiceUtils.toClusterDto(entity)

        then:
        cluster.getId().orElseGet(RandomSuppliers.STRING) == id
        cluster.getName() == name
        cluster.getUser() == user
        cluster.getVersion() == version
        cluster.getDescription().orElseGet(RandomSuppliers.STRING) == description
        cluster.getStatus() == ClusterStatus.TERMINATED
        cluster.getCreated().orElseGet(RandomSuppliers.DATE) == created
        cluster.getUpdated().orElseGet(RandomSuppliers.DATE) == updated
        cluster.getTags() == tags
        cluster.getConfigs() == confs
        cluster.getDependencies() == dependencies
    }

    def "Can convert command entity to command DTO"() {
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
        final FileEntity setupFileEntity = new FileEntity()
        setupFileEntity.setFile(setupFile)
        entity.setSetupFile(setupFileEntity)
        def executable = UUID.randomUUID().toString()
        entity.setExecutable(executable)
        def checkDelay = 2180234L
        entity.setCheckDelay(checkDelay)
        def memory = 10_241
        entity.setMemory(memory)

        when:
        def command = JpaServiceUtils.toCommandDto(entity)

        then:
        command.getId().orElseGet(RandomSuppliers.STRING) == id
        command.getName() == name
        command.getUser() == user
        command.getVersion() == version
        command.getStatus() == CommandStatus.DEPRECATED
        command.getDescription().orElseGet(RandomSuppliers.STRING) == description
        command.getCreated().orElseGet(RandomSuppliers.DATE) == created
        command.getUpdated().orElseGet(RandomSuppliers.DATE) == updated
        command.getExecutable() == executable
        command.getCheckDelay() == checkDelay
        command.getTags() == tags
        command.getSetupFile().orElseGet(RandomSuppliers.STRING) == setupFile
        command.getConfigs() == configs
        command.getDependencies() == dependencies
        command.getMemory().orElseGet(RandomSuppliers.INT) == memory
    }

    def "Can convert Job Projection of Job Entity to Job DTO"() {
        def entity = new JobEntity()
        def id = UUID.randomUUID().toString()
        entity.setUniqueId(id)
        def name = UUID.randomUUID().toString()
        entity.setName(name)
        def user = UUID.randomUUID().toString()
        entity.setUser(user)
        def version = UUID.randomUUID().toString()
        entity.setVersion(version)
        def clusterName = UUID.randomUUID().toString()
        entity.setClusterName(clusterName)
        def commandName = UUID.randomUUID().toString()
        entity.setCommandName(commandName)
        def created = entity.getCreated()
        def updated = entity.getUpdated()
        def description = UUID.randomUUID().toString()
        entity.setDescription(description)
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        final Set<TagEntity> tagEntities = tags.collect(
                {
                    def tagEntity = new TagEntity()
                    tagEntity.setTag(it)
                    tagEntity
                }
        )
        entity.setTags(tagEntities)
        def archiveLocation = UUID.randomUUID().toString()
        entity.setArchiveLocation(archiveLocation)
        def started = new Date()
        entity.setStarted(started)
        def finished = new Date()
        entity.setFinished(finished)
        entity.setStatus(JobStatus.SUCCEEDED)
        def statusMessage = UUID.randomUUID().toString()
        entity.setStatusMsg(statusMessage)

        when:
        def job = JpaServiceUtils.toJobDto(entity)

        then:
        job.getId().orElseGet(RandomSuppliers.STRING) == id
        job.getName() == name
        job.getUser() == user
        job.getVersion() == version
        job.getDescription().orElseGet(RandomSuppliers.STRING) == description
        job.getCreated().orElseGet(RandomSuppliers.DATE) == created
        job.getUpdated().orElseGet(RandomSuppliers.DATE) == updated
        job.getClusterName().orElseGet(RandomSuppliers.STRING) == clusterName
        job.getCommandName().orElseGet(RandomSuppliers.STRING) == commandName
        job.getTags() == tags
        job.getArchiveLocation().orElseGet(RandomSuppliers.STRING) == archiveLocation
        job.getStarted().orElseGet(RandomSuppliers.DATE) == started
        job.getFinished().orElseGet(RandomSuppliers.DATE) == finished
        job.getStatus() == JobStatus.SUCCEEDED
        job.getStatusMsg().orElseGet(RandomSuppliers.STRING) == statusMessage
    }

    def "Can convert Job Execution Projection to Job Execution DTO"() {
        def entity = new JobEntity()
        def id = UUID.randomUUID().toString()
        entity.setUniqueId(id)
        def hostName = UUID.randomUUID().toString()
        entity.setHostName(hostName)
        def processId = 29038
        entity.setProcessId(processId)
        def checkDelay = 1890347L
        entity.setCheckDelay(checkDelay)
        def exitCode = 2084390
        entity.setExitCode(exitCode)
        def timeout = new Date()
        entity.setTimeout(timeout)
        def memory = 10_265
        entity.setMemoryUsed(memory)

        when:
        def execution = JpaServiceUtils.toJobExecutionDto(entity)

        then:
        execution.getId().orElseGet(RandomSuppliers.STRING) == id
        execution.getCreated().orElseGet(RandomSuppliers.DATE) == entity.getCreated()
        execution.getUpdated().orElseGet(RandomSuppliers.DATE) == entity.getUpdated()
        execution.getExitCode().orElseGet(RandomSuppliers.INT) == exitCode
        execution.getHostName() == hostName
        execution.getProcessId().orElseGet(RandomSuppliers.INT) == processId
        execution.getCheckDelay().orElseGet(RandomSuppliers.LONG) == checkDelay
        execution.getTimeout().orElseGet(RandomSuppliers.DATE) == timeout
        execution.getMemory().orElseGet(RandomSuppliers.INT) == memory
    }

    def "Can convert Job Metadata Projection to Job Metadata DTO"() {
        def entity = new JobEntity()
        def id = UUID.randomUUID().toString()
        entity.setUniqueId(id)
        def clientHost = UUID.randomUUID().toString()
        entity.setClientHost(clientHost)
        def userAgent = UUID.randomUUID().toString()
        entity.setUserAgent(userAgent)
        def numAttachments = 3
        entity.setNumAttachments(numAttachments)
        def totalSizeOfAttachments = 38023423L
        entity.setTotalSizeOfAttachments(totalSizeOfAttachments)
        def stdOutSize = 8088234L
        entity.setStdOutSize(stdOutSize)
        def stdErrSize = 898088234L
        entity.setStdErrSize(stdErrSize)

        when:
        def metadata = JpaServiceUtils.toJobMetadataDto(entity)

        then:
        metadata.getId().orElseGet(RandomSuppliers.STRING) == id
        metadata.getClientHost().orElseGet(RandomSuppliers.STRING) == clientHost
        metadata.getUserAgent().orElseGet(RandomSuppliers.STRING) == userAgent
        metadata.getNumAttachments().orElseGet(RandomSuppliers.INT) == numAttachments
        metadata.getTotalSizeOfAttachments().orElseGet(RandomSuppliers.LONG) == totalSizeOfAttachments
        metadata.getStdOutSize().orElseGet(RandomSuppliers.LONG) == stdOutSize
        metadata.getStdErrSize().orElseGet(RandomSuppliers.LONG) == stdErrSize
    }

    def "Can convert Job Request Projection to Job Request DTO"() {
        def entity = new JobEntity()
        def id = UUID.randomUUID().toString()
        entity.setUniqueId(id)
        def name = UUID.randomUUID().toString()
        entity.setName(name)
        def user = UUID.randomUUID().toString()
        entity.setUser(user)
        def version = UUID.randomUUID().toString()
        entity.setVersion(version)
        final Date created = entity.getCreated()
        final Date updated = entity.getUpdated()
        def description = UUID.randomUUID().toString()
        entity.setDescription(description)
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        Set<TagEntity> tagEntities = tags.collect(
                {
                    def tagEntity = new TagEntity()
                    tagEntity.setTag(it)
                    tagEntity
                }
        )
        entity.setTags(tagEntities)
        def commandArgs = UUID.randomUUID().toString()
        entity.setCommandArgs(commandArgs)

        def one = Sets.newHashSet("one", "two", "three")
        def two = Sets.newHashSet("four", "five", "six")
        def three = Sets.newHashSet("seven", "eight", "nine")
        def clusterCriterias = Lists.newArrayList(
                new ClusterCriteria(one),
                new ClusterCriteria(two),
                new ClusterCriteria(three)
        )
        List<ClusterCriteriaEntity> clusterCriteriaEntities = clusterCriterias.collect(
                {
                    final Set<TagEntity> clusterCriteriaTags = it.tags.collect(
                            {
                                def tagEntity = new TagEntity()
                                tagEntity.setTag(it)
                                tagEntity
                            }
                    )
                    def clusterCriteriaEntity = new ClusterCriteriaEntity()
                    clusterCriteriaEntity.setTags(clusterCriteriaTags)
                    clusterCriteriaEntity
                }
        )
        entity.setClusterCriterias(clusterCriteriaEntities)

        def commandCriteria = Sets.newHashSet(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        final Set<TagEntity> commandCriteriaEntities = commandCriteria.collect(
                {
                    def tagEntity = new TagEntity()
                    tagEntity.setTag(it)
                    tagEntity
                }
        )
        entity.setCommandCriteria(commandCriteriaEntities)

        def configs = Sets.newHashSet(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        final Set<FileEntity> configEntities = configs.collect(
                {
                    def fileEntity = new FileEntity()
                    fileEntity.setFile(it)
                    fileEntity
                }
        )
        entity.setConfigs(configEntities)

        def fileDependencies = Sets.newHashSet(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        final Set<FileEntity> dependencies = fileDependencies.collect(
                {
                    def fileEntity = new FileEntity()
                    fileEntity.setFile(it)
                    fileEntity
                }
        )
        entity.setDependencies(dependencies)

        entity.setDisableLogArchival(true)

        def email = UUID.randomUUID().toString()
        entity.setEmail(email)

        def group = UUID.randomUUID().toString()
        entity.setGenieUserGroup(group)

        def setupFile = UUID.randomUUID().toString()
        def setupFileEntity = new FileEntity()
        setupFileEntity.setFile(setupFile)
        entity.setSetupFile(setupFileEntity)

        final int cpu = 38
        entity.setCpuRequested(cpu)

        final int memory = 3060
        entity.setMemoryRequested(memory)

        final List<String> applications = Lists.newArrayList(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        entity.setApplicationsRequested(applications)

        final int timeout = 824197
        entity.setTimeoutRequested(timeout)

        when:
        def request = JpaServiceUtils.toJobRequestDto(entity)

        then:
        request.getId().orElseGet(RandomSuppliers.STRING) == id
        request.getName() == name
        request.getUser() == user
        request.getVersion() == version
        request.getDescription().orElseGet(RandomSuppliers.STRING) == description
        request.getCreated().orElseGet(RandomSuppliers.DATE) == created
        request.getUpdated().orElseGet(RandomSuppliers.DATE) == updated
        request.getTags() == tags
        request.getCommandArgs().orElseGet(RandomSuppliers.STRING) == commandArgs

        def criterias = request.getClusterCriterias()
        criterias.size() == 3
        criterias.get(0).getTags() == one
        criterias.get(1).getTags() == two
        criterias.get(2).getTags() == three

        request.getCommandCriteria() == commandCriteria
        request.getConfigs() == configs
        request.getDependencies() == fileDependencies
        request.isDisableLogArchival()
        request.getEmail().orElseGet(RandomSuppliers.STRING) == email
        request.getGroup().orElseGet(RandomSuppliers.STRING) == group
        request.getSetupFile().orElseGet(RandomSuppliers.STRING) == setupFile
        request.getCpu().orElseGet(RandomSuppliers.INT) == cpu
        request.getMemory().orElseGet(RandomSuppliers.INT) == memory
        request.getApplications() == applications
        request.getTimeout().orElseGet(RandomSuppliers.INT) == timeout
    }
}
