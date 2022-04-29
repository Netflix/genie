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
package com.netflix.genie.web.data.services.impl.jpa.converters

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.common.dto.ArchiveStatus
import com.netflix.genie.common.dto.ClusterCriteria
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.dto.UserResourcesSummary
import com.netflix.genie.test.suppliers.RandomSuppliers
import com.netflix.genie.web.data.services.impl.jpa.entities.CriterionEntity
import com.netflix.genie.web.data.services.impl.jpa.entities.FileEntity
import com.netflix.genie.web.data.services.impl.jpa.entities.JobEntity
import com.netflix.genie.web.data.services.impl.jpa.entities.TagEntity
import com.netflix.genie.web.data.services.impl.jpa.queries.aggregates.UserJobResourcesAggregate
import org.apache.commons.lang3.StringUtils
import spock.lang.Specification

import java.time.Instant

/**
 * Specifications for the {@link EntityV3DtoConverters} class.
 *
 * @author tgianos
 */
class EntityV3DtoConvertersSpec extends Specification {

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
        def metadata = Mock(JsonNode)
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
        def archiveLocation = UUID.randomUUID().toString()
        entity.setArchiveLocation(archiveLocation)
        def started = Instant.now()
        entity.setStarted(started)
        def finished = Instant.now()
        entity.setFinished(finished)
        entity.setStatus(JobStatus.SUCCEEDED.name())
        def statusMessage = UUID.randomUUID().toString()
        entity.setStatusMsg(statusMessage)

        when:
        def job = EntityV3DtoConverters.toJobDto(entity)

        then:
        job.getId().orElseGet(RandomSuppliers.STRING) == id
        job.getName() == name
        job.getUser() == user
        job.getVersion() == version
        job.getDescription().orElseGet(RandomSuppliers.STRING) == description
        job.getCreated().orElseGet(RandomSuppliers.INSTANT) == created
        job.getUpdated().orElseGet(RandomSuppliers.INSTANT) == updated
        job.getClusterName().orElseGet(RandomSuppliers.STRING) == clusterName
        job.getCommandName().orElseGet(RandomSuppliers.STRING) == commandName
        job.getTags() == tags
        job.getArchiveLocation().orElseGet(RandomSuppliers.STRING) == archiveLocation
        job.getStarted().orElseGet(RandomSuppliers.INSTANT) == started
        job.getFinished().orElseGet(RandomSuppliers.INSTANT) == finished
        job.getStatus() == JobStatus.SUCCEEDED
        job.getStatusMsg().orElseGet(RandomSuppliers.STRING) == statusMessage
        job.getMetadata().isPresent()
        job.getMetadata().get() == metadata
    }

    def "Can convert Job Execution Projection to Job Execution DTO"() {
        def entity = new JobEntity()
        def id = UUID.randomUUID().toString()
        entity.setUniqueId(id)
        def hostName = UUID.randomUUID().toString()
        entity.setAgentHostname(hostName)
        def processId = 29038
        entity.setProcessId(processId)
        def exitCode = 2084390
        entity.setExitCode(exitCode)
        def started = Instant.now()
        def timeout = started.plusSeconds(50L)
        entity.setStarted(started)
        entity.setTimeoutUsed(50)
        def memory = 10_265L
        entity.setMemoryUsed(memory)
        entity.setArchiveStatus(ArchiveStatus.ARCHIVED.toString())

        when:
        def execution = EntityV3DtoConverters.toJobExecutionDto(entity)

        then:
        execution.getId().orElseGet(RandomSuppliers.STRING) == id
        execution.getCreated().orElseGet(RandomSuppliers.INSTANT) == entity.getCreated()
        execution.getUpdated().orElseGet(RandomSuppliers.INSTANT) == entity.getUpdated()
        execution.getExitCode().orElseGet(RandomSuppliers.INT) == exitCode
        execution.getHostName() == hostName
        execution.getProcessId().orElseGet(RandomSuppliers.INT) == processId
        execution.getCheckDelay().isEmpty()
        execution.getTimeout().orElseGet(RandomSuppliers.INSTANT) == timeout
        execution.getMemory().orElseGet(RandomSuppliers.LONG) == memory
        execution.getArchiveStatus().orElse(null) == ArchiveStatus.ARCHIVED
    }

    def "Can convert Job Execution Projection with null archiveStatus to Job Execution DTO"() {
        def entity = new JobEntity()
        entity.setUniqueId(UUID.randomUUID().toString())

        when:
        def execution = EntityV3DtoConverters.toJobExecutionDto(entity)

        then:
        execution.getArchiveStatus().orElse(null) == ArchiveStatus.UNKNOWN
    }

    def "Can convert Job Metadata Projection to Job Metadata DTO"() {
        def entity = new JobEntity()
        def id = UUID.randomUUID().toString()
        entity.setUniqueId(id)
        def clientHost = UUID.randomUUID().toString()
        entity.setRequestApiClientHostname(clientHost)
        def userAgent = UUID.randomUUID().toString()
        entity.setRequestApiClientUserAgent(userAgent)
        def numAttachments = 3
        entity.setNumAttachments(numAttachments)
        def totalSizeOfAttachments = 38023423L
        entity.setTotalSizeOfAttachments(totalSizeOfAttachments)
        def stdOutSize = 8088234L
        entity.setStdOutSize(stdOutSize)
        def stdErrSize = 898088234L
        entity.setStdErrSize(stdErrSize)

        when:
        def metadata = EntityV3DtoConverters.toJobMetadataDto(entity)

        then:
        metadata.getId().orElseGet(RandomSuppliers.STRING) == id
        metadata.getClientHost().orElseGet(RandomSuppliers.STRING) == clientHost
        metadata.getUserAgent().orElseGet(RandomSuppliers.STRING) == userAgent
        metadata.getNumAttachments().orElseGet(RandomSuppliers.INT) == numAttachments
        metadata.getTotalSizeOfAttachments().orElseGet(RandomSuppliers.LONG) == totalSizeOfAttachments
        metadata.getStdOutSize().orElseGet(RandomSuppliers.LONG) == stdOutSize
        metadata.getStdErrSize().orElseGet(RandomSuppliers.LONG) == stdErrSize
    }

    def "Can convert user resources aggregate to a summary DTO"() {
        UserJobResourcesAggregate aggregate = Mock(UserJobResourcesAggregate)
        UserResourcesSummary summary

        when:
        summary = EntityV3DtoConverters.toUserResourceSummaryDto(aggregate)

        then:
        1 * aggregate.getUser() >> "username"
        1 * aggregate.getRunningJobsCount() >> 3L
        1 * aggregate.getUsedMemory() >> 1024L
        summary.getUser() == "username"
        summary.getRunningJobsCount() == 3L
        summary.getUsedMemory() == 1024L

        when:
        summary = EntityV3DtoConverters.toUserResourceSummaryDto(aggregate)

        then:
        1 * aggregate.getUser() >> null
        1 * aggregate.getRunningJobsCount() >> null
        1 * aggregate.getUsedMemory() >> null
        summary.getUser() == "NULL"
        summary.getRunningJobsCount() == 0
        summary.getUsedMemory() == 0
    }
}
