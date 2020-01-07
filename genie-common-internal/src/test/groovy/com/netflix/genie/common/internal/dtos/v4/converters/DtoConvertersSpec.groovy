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
package com.netflix.genie.common.internal.dtos.v4.converters

import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.common.dto.ClusterCriteria
import com.netflix.genie.common.external.dtos.v4.AgentConfigRequest
import com.netflix.genie.common.external.dtos.v4.Application
import com.netflix.genie.common.external.dtos.v4.ApplicationMetadata
import com.netflix.genie.common.external.dtos.v4.ApplicationRequest
import com.netflix.genie.common.external.dtos.v4.ApplicationStatus
import com.netflix.genie.common.external.dtos.v4.Cluster
import com.netflix.genie.common.external.dtos.v4.ClusterMetadata
import com.netflix.genie.common.external.dtos.v4.ClusterRequest
import com.netflix.genie.common.external.dtos.v4.ClusterStatus
import com.netflix.genie.common.external.dtos.v4.Command
import com.netflix.genie.common.external.dtos.v4.CommandMetadata
import com.netflix.genie.common.external.dtos.v4.CommandRequest
import com.netflix.genie.common.external.dtos.v4.CommandStatus
import com.netflix.genie.common.external.dtos.v4.Criterion
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment
import com.netflix.genie.common.external.dtos.v4.ExecutionResourceCriteria
import com.netflix.genie.common.external.dtos.v4.JobMetadata
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.external.util.GenieObjectMapper
import org.apache.commons.lang3.StringUtils
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant
import java.util.stream.Stream

/**
 * Specifications for the {@link com.netflix.genie.common.internal.dtos.v4.converters.DtoConverters} class which handles converting between v3 and v4 DTOs for API
 * backwards compatibility.
 *
 * @author tgianos
 */
class DtoConvertersSpec extends Specification {

    def "Can convert V3 Application to V4 Application Request"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = com.netflix.genie.common.dto.ApplicationStatus.DEPRECATED
        def tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            DtoConverters.GENIE_ID_PREFIX + id,
            DtoConverters.GENIE_NAME_PREFIX + name
        )
        def metadata = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def type = UUID.randomUUID().toString()
        def description = UUID.randomUUID().toString()
        def configs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def setupFile = UUID.randomUUID().toString()
        com.netflix.genie.common.dto.Application v3Application
        ApplicationRequest applicationRequest

        when:
        v3Application = new com.netflix.genie.common.dto.Application.Builder(
            name,
            user,
            version,
            status
        )
            .withId(id)
            .withType(type)
            .withTags(tags)
            .withMetadata(metadata)
            .withDescription(description)
            .withConfigs(configs)
            .withDependencies(dependencies)
            .withSetupFile(setupFile)
            .build()
        applicationRequest = DtoConverters.toV4ApplicationRequest(v3Application)

        then:
        applicationRequest.getMetadata().getStatus() == ApplicationStatus.DEPRECATED
        applicationRequest.getMetadata().getType().orElse(null) == type
        applicationRequest.getMetadata().getMetadata().isPresent()
        applicationRequest.getMetadata().getTags().size() == 2
        applicationRequest.getMetadata().getDescription().orElse(null) == description
        applicationRequest.getMetadata().getVersion() == version
        applicationRequest.getMetadata().getUser() == user
        applicationRequest.getMetadata().getName() == name
        applicationRequest.getRequestedId().orElse(null) == id
        applicationRequest.getResources().getSetupFile().orElse(null) == setupFile
        applicationRequest.getResources().getConfigs() == configs
        applicationRequest.getResources().getDependencies() == dependencies

        when:
        v3Application = new com.netflix.genie.common.dto.Application.Builder(
            name,
            user,
            version,
            status
        ).build()
        applicationRequest = DtoConverters.toV4ApplicationRequest(v3Application)

        then:
        applicationRequest.getMetadata().getStatus() == ApplicationStatus.DEPRECATED
        !applicationRequest.getMetadata().getType().isPresent()
        !applicationRequest.getMetadata().getMetadata().isPresent()
        applicationRequest.getMetadata().getTags().isEmpty()
        !applicationRequest.getMetadata().getDescription().isPresent()
        applicationRequest.getMetadata().getVersion() == version
        applicationRequest.getMetadata().getUser() == user
        applicationRequest.getMetadata().getName() == name
        !applicationRequest.getRequestedId().isPresent()
        !applicationRequest.getResources().getSetupFile().isPresent()
        applicationRequest.getResources().getConfigs().isEmpty()
        applicationRequest.getResources().getDependencies().isEmpty()
    }

    def "Can convert V3 Application to V4 Application"() {
        def id = UUID.randomUUID().toString()
        def created = Instant.now()
        def updated = Instant.now()
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = com.netflix.genie.common.dto.ApplicationStatus.DEPRECATED
        def tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            DtoConverters.GENIE_ID_PREFIX + id,
            DtoConverters.GENIE_NAME_PREFIX + name
        )
        def metadata = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def type = UUID.randomUUID().toString()
        def description = UUID.randomUUID().toString()
        def configs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def setupFile = UUID.randomUUID().toString()
        com.netflix.genie.common.dto.Application v3Application
        Application v4Application

        when:
        v3Application = new com.netflix.genie.common.dto.Application.Builder(
            name,
            user,
            version,
            status
        )
            .withId(id)
            .withCreated(created)
            .withUpdated(updated)
            .withType(type)
            .withTags(tags)
            .withMetadata(metadata)
            .withDescription(description)
            .withConfigs(configs)
            .withDependencies(dependencies)
            .withSetupFile(setupFile)
            .build()
        v4Application = DtoConverters.toV4Application(v3Application)

        then:
        v4Application.getMetadata().getStatus() == ApplicationStatus.DEPRECATED
        v4Application.getMetadata().getType().orElse(null) == type
        v4Application.getMetadata().getMetadata().isPresent()
        v4Application.getMetadata().getTags().size() == 2
        v4Application.getMetadata().getDescription().orElse(null) == description
        v4Application.getMetadata().getVersion() == version
        v4Application.getMetadata().getUser() == user
        v4Application.getMetadata().getName() == name
        v4Application.getId() == id
        v4Application.getResources().getSetupFile().orElse(null) == setupFile
        v4Application.getResources().getConfigs() == configs
        v4Application.getResources().getDependencies() == dependencies
        v4Application.getCreated() == created
        v4Application.getUpdated() == updated
    }

    def "Can convert V4 Application to V3 Application"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ApplicationStatus.DEPRECATED
        def tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def metadata = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def type = UUID.randomUUID().toString()
        def description = UUID.randomUUID().toString()
        def configs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def setupFile = UUID.randomUUID().toString()
        def created = Instant.now()
        def updated = Instant.now()
        Application v4Application
        com.netflix.genie.common.dto.Application v3Application

        when:
        v4Application = new Application(
            id,
            created,
            updated,
            new ExecutionEnvironment(
                configs,
                dependencies,
                setupFile
            ),
            new ApplicationMetadata.Builder(name, user, version, status)
                .withDescription(description)
                .withMetadata(metadata)
                .withTags(tags)
                .withType(type)
                .build()
        )
        v3Application = DtoConverters.toV3Application(v4Application)

        then:
        v3Application.getStatus() == com.netflix.genie.common.dto.ApplicationStatus.DEPRECATED
        v3Application.getType().orElse(null) == type
        v3Application.getMetadata().isPresent()
        v3Application.getTags().size() == 4
        v3Application.getTags().containsAll(tags)
        v3Application.getTags().contains(DtoConverters.GENIE_ID_PREFIX + id)
        v3Application.getTags().contains(DtoConverters.GENIE_NAME_PREFIX + name)
        v3Application.getDescription().orElse(null) == description
        v3Application.getVersion() == version
        v3Application.getUser() == user
        v3Application.getName() == name
        v3Application.getId().orElse(null) == id
        v3Application.getSetupFile().orElse(null) == setupFile
        v3Application.getConfigs() == configs
        v3Application.getDependencies() == dependencies

        when:
        v4Application = new Application(
            id,
            created,
            updated,
            null,
            new ApplicationMetadata.Builder(name, user, version, status).build()
        )
        v3Application = DtoConverters.toV3Application(v4Application)

        then:
        v3Application.getStatus() == com.netflix.genie.common.dto.ApplicationStatus.DEPRECATED
        !v3Application.getType().isPresent()
        !v3Application.getMetadata().isPresent()
        v3Application.getTags().size() == 2
        v3Application.getTags().contains(DtoConverters.GENIE_ID_PREFIX + id)
        v3Application.getTags().contains(DtoConverters.GENIE_NAME_PREFIX + name)
        !v3Application.getDescription().isPresent()
        v3Application.getVersion() == version
        v3Application.getUser() == user
        v3Application.getName() == name
        v3Application.getId().orElse(null) == id
        !v3Application.getSetupFile().isPresent()
        v3Application.getConfigs().isEmpty()
        v3Application.getDependencies().isEmpty()
    }

    def "Can convert V3 Cluster to V4 Cluster Request"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = com.netflix.genie.common.dto.ClusterStatus.TERMINATED
        def tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            DtoConverters.GENIE_ID_PREFIX + id,
            DtoConverters.GENIE_NAME_PREFIX + name
        )
        def metadata = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def description = UUID.randomUUID().toString()
        def configs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def setupFile = UUID.randomUUID().toString()
        com.netflix.genie.common.dto.Cluster v3Cluster
        ClusterRequest clusterRequest

        when:
        v3Cluster = new com.netflix.genie.common.dto.Cluster.Builder(
            name,
            user,
            version,
            status
        )
            .withId(id)
            .withTags(tags)
            .withMetadata(metadata)
            .withDescription(description)
            .withConfigs(configs)
            .withDependencies(dependencies)
            .withSetupFile(setupFile)
            .build()
        clusterRequest = DtoConverters.toV4ClusterRequest(v3Cluster)

        then:
        clusterRequest.getMetadata().getStatus() == ClusterStatus.TERMINATED
        clusterRequest.getMetadata().getMetadata().isPresent()
        clusterRequest.getMetadata().getTags().size() == 2
        clusterRequest.getMetadata().getDescription().orElse(null) == description
        clusterRequest.getMetadata().getVersion() == version
        clusterRequest.getMetadata().getUser() == user
        clusterRequest.getMetadata().getName() == name
        clusterRequest.getRequestedId().orElse(null) == id
        clusterRequest.getResources().getSetupFile().orElse(null) == setupFile
        clusterRequest.getResources().getConfigs() == configs
        clusterRequest.getResources().getDependencies() == dependencies

        when:
        v3Cluster = new com.netflix.genie.common.dto.Cluster.Builder(
            name,
            user,
            version,
            status
        ).build()
        clusterRequest = DtoConverters.toV4ClusterRequest(v3Cluster)

        then:
        clusterRequest.getMetadata().getStatus() == ClusterStatus.TERMINATED
        !clusterRequest.getMetadata().getMetadata().isPresent()
        clusterRequest.getMetadata().getTags().isEmpty()
        !clusterRequest.getMetadata().getDescription().isPresent()
        clusterRequest.getMetadata().getVersion() == version
        clusterRequest.getMetadata().getUser() == user
        clusterRequest.getMetadata().getName() == name
        !clusterRequest.getRequestedId().isPresent()
        !clusterRequest.getResources().getSetupFile().isPresent()
        clusterRequest.getResources().getConfigs().isEmpty()
        clusterRequest.getResources().getDependencies().isEmpty()
    }

    def "Can convert V3 Cluster to V4 Cluster"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = com.netflix.genie.common.dto.ClusterStatus.TERMINATED
        def tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            DtoConverters.GENIE_ID_PREFIX + id,
            DtoConverters.GENIE_NAME_PREFIX + name
        )
        def metadata = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def description = UUID.randomUUID().toString()
        def configs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def setupFile = UUID.randomUUID().toString()
        com.netflix.genie.common.dto.Cluster v3Cluster
        Cluster v4Cluster

        when:
        v3Cluster = new com.netflix.genie.common.dto.Cluster.Builder(
            name,
            user,
            version,
            status
        )
            .withId(id)
            .withTags(tags)
            .withMetadata(metadata)
            .withDescription(description)
            .withConfigs(configs)
            .withDependencies(dependencies)
            .withSetupFile(setupFile)
            .build()
        v4Cluster = DtoConverters.toV4Cluster(v3Cluster)

        then:
        v4Cluster.getMetadata().getStatus() == ClusterStatus.TERMINATED
        v4Cluster.getMetadata().getMetadata().isPresent()
        v4Cluster.getMetadata().getTags().size() == 2
        v4Cluster.getMetadata().getDescription().orElse(null) == description
        v4Cluster.getMetadata().getVersion() == version
        v4Cluster.getMetadata().getUser() == user
        v4Cluster.getMetadata().getName() == name
        v4Cluster.getId() == id
        v4Cluster.getResources().getSetupFile().orElse(null) == setupFile
        v4Cluster.getResources().getConfigs() == configs
        v4Cluster.getResources().getDependencies() == dependencies
    }

    def "Can convert V4 Cluster to V3 Cluster"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ClusterStatus.UP
        def tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def metadata = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def description = UUID.randomUUID().toString()
        def configs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def setupFile = UUID.randomUUID().toString()
        def created = Instant.now()
        def updated = Instant.now()
        Cluster v4Cluster
        com.netflix.genie.common.dto.Cluster v3Cluster

        when:
        v4Cluster = new Cluster(
            id,
            created,
            updated,
            new ExecutionEnvironment(
                configs,
                dependencies,
                setupFile
            ),
            new ClusterMetadata.Builder(name, user, version, status)
                .withDescription(description)
                .withMetadata(metadata)
                .withTags(tags)
                .build()
        )
        v3Cluster = DtoConverters.toV3Cluster(v4Cluster)

        then:
        v3Cluster.getStatus() == com.netflix.genie.common.dto.ClusterStatus.UP
        v3Cluster.getMetadata().isPresent()
        v3Cluster.getTags().size() == 4
        v3Cluster.getTags().containsAll(tags)
        v3Cluster.getTags().contains(DtoConverters.GENIE_ID_PREFIX + id)
        v3Cluster.getTags().contains(DtoConverters.GENIE_NAME_PREFIX + name)
        v3Cluster.getDescription().orElse(null) == description
        v3Cluster.getVersion() == version
        v3Cluster.getUser() == user
        v3Cluster.getName() == name
        v3Cluster.getId().orElse(null) == id
        v3Cluster.getSetupFile().orElse(null) == setupFile
        v3Cluster.getConfigs() == configs
        v3Cluster.getDependencies() == dependencies

        when:
        v4Cluster = new Cluster(
            id,
            created,
            updated,
            null,
            new ClusterMetadata.Builder(name, user, version, status).build()
        )
        v3Cluster = DtoConverters.toV3Cluster(v4Cluster)

        then:
        v3Cluster.getStatus() == com.netflix.genie.common.dto.ClusterStatus.UP
        !v3Cluster.getMetadata().isPresent()
        v3Cluster.getTags().size() == 2
        v3Cluster.getTags().contains(DtoConverters.GENIE_ID_PREFIX + id)
        v3Cluster.getTags().contains(DtoConverters.GENIE_NAME_PREFIX + name)
        !v3Cluster.getDescription().isPresent()
        v3Cluster.getVersion() == version
        v3Cluster.getUser() == user
        v3Cluster.getName() == name
        v3Cluster.getId().orElse(null) == id
        !v3Cluster.getSetupFile().isPresent()
        v3Cluster.getConfigs().isEmpty()
        v3Cluster.getDependencies().isEmpty()
    }

    def "Can convert V3 Command to V4 Command Request"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = com.netflix.genie.common.dto.CommandStatus.INACTIVE
        def tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            DtoConverters.GENIE_ID_PREFIX + id,
            DtoConverters.GENIE_NAME_PREFIX + name
        )
        def binary = UUID.randomUUID().toString()
        def defaultBinaryArgument = UUID.randomUUID().toString()
        def executableAndArgs = Lists.newArrayList(binary, defaultBinaryArgument)
        def memory = 128_347
        def metadata = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def description = UUID.randomUUID().toString()
        def configs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def setupFile = UUID.randomUUID().toString()
        def checkDelay = 380_234L
        def clusterCriteria = Lists.newArrayList(new Criterion.Builder().withId(UUID.randomUUID().toString()).build())
        com.netflix.genie.common.dto.Command v3Command
        CommandRequest commandRequest

        when:
        v3Command = new com.netflix.genie.common.dto.Command.Builder(
            name,
            user,
            version,
            status,
            executableAndArgs,
            checkDelay
        )
            .withId(id)
            .withTags(tags)
            .withMetadata(metadata)
            .withDescription(description)
            .withConfigs(configs)
            .withDependencies(dependencies)
            .withSetupFile(setupFile)
            .withMemory(memory)
            .withClusterCriteria(clusterCriteria)
            .build()
        commandRequest = DtoConverters.toV4CommandRequest(v3Command)

        then:
        commandRequest.getMetadata().getStatus() == CommandStatus.INACTIVE
        commandRequest.getMetadata().getMetadata().isPresent()
        commandRequest.getMetadata().getTags().size() == 2
        commandRequest.getMetadata().getDescription().orElse(null) == description
        commandRequest.getMetadata().getVersion() == version
        commandRequest.getMetadata().getUser() == user
        commandRequest.getMetadata().getName() == name
        commandRequest.getRequestedId().orElse(null) == id
        commandRequest.getResources().getSetupFile().orElse(null) == setupFile
        commandRequest.getResources().getConfigs() == configs
        commandRequest.getResources().getDependencies() == dependencies
        commandRequest.getMemory().orElse(-1) == memory
        commandRequest.getExecutable().size() == 2
        commandRequest.getExecutable().get(0) == binary
        commandRequest.getExecutable().get(1) == defaultBinaryArgument
        commandRequest.getCheckDelay().orElse(null) == checkDelay
        commandRequest.getClusterCriteria() == clusterCriteria

        when:
        v3Command = new com.netflix.genie.common.dto.Command.Builder(
            name,
            user,
            version,
            status,
            executableAndArgs,
            checkDelay
        ).build()
        commandRequest = DtoConverters.toV4CommandRequest(v3Command)

        then:
        commandRequest.getMetadata().getStatus() == CommandStatus.INACTIVE
        !commandRequest.getMetadata().getMetadata().isPresent()
        commandRequest.getMetadata().getTags().isEmpty()
        !commandRequest.getMetadata().getDescription().isPresent()
        commandRequest.getMetadata().getVersion() == version
        commandRequest.getMetadata().getUser() == user
        commandRequest.getMetadata().getName() == name
        !commandRequest.getRequestedId().isPresent()
        !commandRequest.getResources().getSetupFile().isPresent()
        commandRequest.getResources().getConfigs().isEmpty()
        commandRequest.getResources().getDependencies().isEmpty()
        !commandRequest.getMemory().isPresent()
        commandRequest.getExecutable().size() == 2
        commandRequest.getExecutable().get(0) == binary
        commandRequest.getExecutable().get(1) == defaultBinaryArgument
        commandRequest.getCheckDelay().orElse(null) == checkDelay
        commandRequest.getClusterCriteria().isEmpty()
    }

    def "Can convert V3 Command to V4 Command"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = com.netflix.genie.common.dto.CommandStatus.DEPRECATED
        def tags = Sets.newHashSet(
            DtoConverters.GENIE_ID_PREFIX + id,
            DtoConverters.GENIE_NAME_PREFIX + name,
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def binary = UUID.randomUUID().toString()
        def defaultBinaryArgument = UUID.randomUUID().toString()
        def executableAndArgs = Lists.newArrayList(binary, defaultBinaryArgument)
        def memory = 128_347
        def metadata = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def description = UUID.randomUUID().toString()
        def configs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def setupFile = UUID.randomUUID().toString()
        def created = Instant.now()
        def updated = Instant.now()
        def checkDelay = 987_345L
        def clusterCriteria = Lists.newArrayList(
            new Criterion.Builder().withTags(Sets.newHashSet(UUID.randomUUID().toString())).build()
        )
        Command v4Command
        com.netflix.genie.common.dto.Command v3Command

        when:
        v3Command = new com.netflix.genie.common.dto.Command.Builder(
            name,
            user,
            version,
            status,
            executableAndArgs,
            checkDelay
        )
            .withId(id)
            .withCreated(created)
            .withUpdated(updated)
            .withConfigs(configs)
            .withDependencies(dependencies)
            .withSetupFile(setupFile)
            .withMetadata(metadata)
            .withTags(tags)
            .withDescription(description)
            .withMemory(memory)
            .withClusterCriteria(clusterCriteria)
            .build()
        v4Command = DtoConverters.toV4Command(v3Command)

        then:
        v4Command.getId() == id
        v4Command.getCreated() == created
        v4Command.getUpdated() == updated
        v4Command.getCheckDelay() == checkDelay
        v4Command.getMemory().orElse(null) == memory
        v4Command.getExecutable() == executableAndArgs
        v4Command.getMetadata().getName() == name
        v4Command.getMetadata().getUser() == user
        v4Command.getMetadata().getVersion() == version
        v4Command.getMetadata().getTags().size() == 2
        !v4Command.getMetadata().getTags().contains(DtoConverters.GENIE_ID_PREFIX + id)
        !v4Command.getMetadata().getTags().contains(DtoConverters.GENIE_NAME_PREFIX + name)
        v4Command.getMetadata().getStatus() == CommandStatus.DEPRECATED
        v4Command.getMetadata().getDescription().orElse(null) == description
        v4Command.getMetadata().getMetadata().orElse(null) == GenieObjectMapper.getMapper().readTree(metadata)
        v4Command.getResources().getConfigs() == configs
        v4Command.getResources().getDependencies() == dependencies
        v4Command.getResources().getSetupFile().orElse(null) == setupFile
        v4Command.getClusterCriteria() == clusterCriteria

        when:
        v3Command = new com.netflix.genie.common.dto.Command.Builder(
            name,
            user,
            version,
            status,
            executableAndArgs,
            checkDelay
        )
            .withId(id)
            .build()
        v4Command = DtoConverters.toV4Command(v3Command)

        then:
        v4Command.getId() == id
        v4Command.getCreated() != null
        v4Command.getUpdated() != null
        v4Command.getCheckDelay() == checkDelay
        !v4Command.getMemory().isPresent()
        v4Command.getExecutable() == executableAndArgs
        v4Command.getMetadata().getName() == name
        v4Command.getMetadata().getUser() == user
        v4Command.getMetadata().getVersion() == version
        v4Command.getMetadata().getTags().isEmpty()
        v4Command.getMetadata().getStatus() == CommandStatus.DEPRECATED
        !v4Command.getMetadata().getDescription().isPresent()
        !v4Command.getMetadata().getMetadata().isPresent()
        v4Command.getResources().getConfigs().isEmpty()
        v4Command.getResources().getDependencies().isEmpty()
        !v4Command.getResources().getSetupFile().isPresent()
        v4Command.getClusterCriteria().isEmpty()
    }

    def "Can convert V4 Command to V3 Command"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = CommandStatus.DEPRECATED
        def tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def binary = UUID.randomUUID().toString()
        def defaultBinaryArgument = UUID.randomUUID().toString()
        def executable = Lists.newArrayList(binary, defaultBinaryArgument)
        def memory = 128_347
        def metadata = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
        def description = UUID.randomUUID().toString()
        def configs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def setupFile = UUID.randomUUID().toString()
        def created = Instant.now()
        def updated = Instant.now()
        def clusterCriteria = Lists.newArrayList(new Criterion.Builder().withName(UUID.randomUUID().toString()).build())
        def checkDelay = 987_345L
        Command v4Command
        com.netflix.genie.common.dto.Command v3Command

        when:
        v4Command = new Command(
            id,
            created,
            updated,
            new ExecutionEnvironment(
                configs,
                dependencies,
                setupFile
            ),
            new CommandMetadata.Builder(name, user, version, status)
                .withDescription(description)
                .withMetadata(metadata)
                .withTags(tags)
                .build(),
            executable,
            memory,
            checkDelay,
            clusterCriteria
        )
        v3Command = DtoConverters.toV3Command(v4Command)

        then:
        v3Command.getStatus() == com.netflix.genie.common.dto.CommandStatus.DEPRECATED
        v3Command.getMetadata().isPresent()
        v3Command.getTags().size() == 4
        v3Command.getTags().containsAll(tags)
        v3Command.getTags().contains(DtoConverters.GENIE_ID_PREFIX + id)
        v3Command.getTags().contains(DtoConverters.GENIE_NAME_PREFIX + name)
        v3Command.getDescription().orElse(null) == description
        v3Command.getVersion() == version
        v3Command.getUser() == user
        v3Command.getName() == name
        v3Command.getId().orElse(null) == id
        v3Command.getSetupFile().orElse(null) == setupFile
        v3Command.getConfigs() == configs
        v3Command.getDependencies() == dependencies
        v3Command.getExecutable() == binary + ' ' + defaultBinaryArgument
        v3Command.getMemory().orElse(-1) == memory
        v3Command.getCheckDelay() == checkDelay
        v3Command.getClusterCriteria() == clusterCriteria

        when:
        v4Command = new Command(
            id,
            created,
            updated,
            null,
            new CommandMetadata.Builder(name, user, version, status).build(),
            executable,
            null,
            checkDelay,
            null
        )
        v3Command = DtoConverters.toV3Command(v4Command)

        then:
        v3Command.getStatus() == com.netflix.genie.common.dto.CommandStatus.DEPRECATED
        !v3Command.getMetadata().isPresent()
        v3Command.getTags().size() == 2
        v3Command.getTags().contains(DtoConverters.GENIE_ID_PREFIX + id)
        v3Command.getTags().contains(DtoConverters.GENIE_NAME_PREFIX + name)
        !v3Command.getDescription().isPresent()
        v3Command.getVersion() == version
        v3Command.getUser() == user
        v3Command.getName() == name
        v3Command.getId().orElse(null) == id
        !v3Command.getSetupFile().isPresent()
        v3Command.getConfigs().isEmpty()
        v3Command.getDependencies().isEmpty()
        v3Command.getExecutable() == binary + ' ' + defaultBinaryArgument
        !v3Command.getMemory().isPresent()
        v3Command.getCheckDelay() == checkDelay
        v3Command.getClusterCriteria().isEmpty()
    }

    def "Can convert V4 Job Request to V3"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def clusterCriteria = Lists.newArrayList(
            new Criterion.Builder()
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build(),
            new Criterion.Builder()
                .withName(UUID.randomUUID().toString())
                .withStatus(UUID.randomUUID().toString())
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build()
        )
        def commandCriterion = new Criterion.Builder()
            .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
            .build()
        def applicationIds = Lists.newArrayList(UUID.randomUUID().toString())
        def commandArgs = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def tags = Sets.newHashSet(UUID.randomUUID().toString())
        def email = UUID.randomUUID().toString()
        def group = UUID.randomUUID().toString()
        def grouping = UUID.randomUUID().toString()
        def groupingInstance = UUID.randomUUID().toString()
        def description = UUID.randomUUID().toString()
        def metadata = GenieObjectMapper.mapper.createObjectNode()
        def configs = Sets.newHashSet(UUID.randomUUID().toString())
        def dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def setupFile = UUID.randomUUID().toString()
        def timeout = 10835

        def jobRequest = new JobRequest(
            id,
            new ExecutionEnvironment(configs, dependencies, setupFile),
            commandArgs,
            new JobMetadata.Builder(name, user, version)
                .withTags(tags)
                .withGroupingInstance(groupingInstance)
                .withGrouping(grouping)
                .withGroup(group)
                .withEmail(email)
                .withMetadata(metadata)
                .withDescription(description)
                .build(),
            new ExecutionResourceCriteria(clusterCriteria, commandCriterion, applicationIds),
            null,
            new AgentConfigRequest
                .Builder()
                .withTimeoutRequested(timeout)
                .withInteractive(true)
                .withArchivingDisabled(true)
                .build(),
            null
        )

        when:
        def v3JobRequest = DtoConverters.toV3JobRequest(jobRequest)

        then:
        v3JobRequest.getId().orElse(UUID.randomUUID().toString()) == id
        v3JobRequest.getName() == name
        v3JobRequest.getUser() == user
        v3JobRequest.getVersion() == version
        v3JobRequest.getTags() == tags
        v3JobRequest.getApplications() == applicationIds
        v3JobRequest.getCommandArgs().orElse(UUID.randomUUID().toString()) == StringUtils.join(commandArgs, " ")
        !v3JobRequest.getMemory().isPresent()
        v3JobRequest.getTimeout().orElse(-1) == timeout
        v3JobRequest.getMetadata().orElse(null) == metadata
        v3JobRequest.getGrouping().orElse(UUID.randomUUID().toString()) == grouping
        v3JobRequest.getGroupingInstance().orElse(UUID.randomUUID().toString()) == groupingInstance
        v3JobRequest.getGroup().orElse(UUID.randomUUID().toString()) == group
        v3JobRequest.getDescription().orElse(UUID.randomUUID().toString()) == description
        !v3JobRequest.getCpu().isPresent()
        v3JobRequest.getDependencies() == dependencies
        v3JobRequest.getConfigs() == configs
        v3JobRequest.getSetupFile().orElse(UUID.randomUUID().toString()) == setupFile
        v3JobRequest.getEmail().orElse(UUID.randomUUID().toString()) == email
        v3JobRequest.getCommandCriteria() == commandCriterion.getTags()
        v3JobRequest.getClusterCriterias().size() == 2
        v3JobRequest.getClusterCriterias().get(0).getTags() == DtoConverters.toV3CriterionTags(clusterCriteria.get(0))
        v3JobRequest.getClusterCriterias().get(1).getTags() == DtoConverters.toV3CriterionTags(clusterCriteria.get(1))
    }

    def "Can convert V3 Job Request to V4"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def clusterCriteria = Lists.newArrayList(
            new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())),
            new ClusterCriteria(
                Sets.newHashSet(
                    DtoConverters.GENIE_ID_PREFIX + UUID.randomUUID().toString(),
                    DtoConverters.GENIE_NAME_PREFIX + UUID.randomUUID().toString()
                )
            )
        )
        def commandCriterion = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def applicationIds = Lists.newArrayList(UUID.randomUUID().toString())
        def commandArgs = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def tags = Sets.newHashSet(UUID.randomUUID().toString())
        def email = UUID.randomUUID().toString()
        def group = UUID.randomUUID().toString()
        def grouping = UUID.randomUUID().toString()
        def groupingInstance = UUID.randomUUID().toString()
        def description = UUID.randomUUID().toString()
        def metadata = GenieObjectMapper.mapper.createObjectNode()
        def configs = Sets.newHashSet(UUID.randomUUID().toString())
        def dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def setupFile = UUID.randomUUID().toString()
        def timeout = 10835
        def cpu = 3
        def memory = 1512

        def v3JobRequest = new com.netflix.genie.common.dto.JobRequest.Builder(
            name,
            user,
            version,
            clusterCriteria,
            commandCriterion
        )
            .withId(id)
            .withCommandArgs(commandArgs)
            .withApplications(applicationIds)
            .withTags(tags)
            .withEmail(email)
            .withGroup(group)
            .withGrouping(grouping)
            .withGroupingInstance(groupingInstance)
            .withDescription(description)
            .withMetadata(metadata)
            .withConfigs(configs)
            .withDependencies(dependencies)
            .withSetupFile(setupFile)
            .withTimeout(timeout)
            .withCpu(cpu)
            .withMemory(memory)
            .build()

        when:
        def v4JobRequest = DtoConverters.toV4JobRequest(v3JobRequest)

        then:
        v4JobRequest.getRequestedId().orElse(null) == id
        v4JobRequest.getMetadata().getName() == name
        v4JobRequest.getMetadata().getUser() == user
        v4JobRequest.getMetadata().getVersion() == version
        v4JobRequest.getMetadata().getTags() == tags
        v4JobRequest.getCriteria().getApplicationIds() == applicationIds
        v4JobRequest.getCommandArgs() == [StringUtils.join(commandArgs, StringUtils.SPACE)] as List
        v4JobRequest.getRequestedJobEnvironment().getRequestedJobMemory().orElse(null) == memory
        v4JobRequest.getRequestedAgentConfig().getTimeoutRequested().orElse(-1) == timeout
        v4JobRequest.getMetadata().getMetadata().orElse(null) == metadata
        v4JobRequest.getMetadata().getGrouping().orElse(null) == grouping
        v4JobRequest.getMetadata().getGroupingInstance().orElse(null) == groupingInstance
        v4JobRequest.getMetadata().getGroup().orElse(null) == group
        v4JobRequest.getMetadata().getDescription().orElse(null) == description
        v4JobRequest.getRequestedJobEnvironment().getRequestedJobCpu().orElse(null) == cpu
        !v4JobRequest.getRequestedAgentConfig().getRequestedJobDirectoryLocation().isPresent()
        !v4JobRequest.getRequestedJobEnvironment().getExt().isPresent()
        v4JobRequest.getResources().getDependencies() == dependencies
        v4JobRequest.getResources().getConfigs() == configs
        v4JobRequest.getResources().getSetupFile().orElse(null) == setupFile
        v4JobRequest.getMetadata().getEmail().orElse(UUID.randomUUID().toString()) == email
        !v4JobRequest.getCriteria().getCommandCriterion().getId().isPresent()
        !v4JobRequest.getCriteria().getCommandCriterion().getName().isPresent()
        !v4JobRequest.getCriteria().getCommandCriterion().getStatus().isPresent()
        v4JobRequest.getCriteria().getCommandCriterion().getTags() == commandCriterion
        v4JobRequest.getCriteria().getClusterCriteria().size() == 2
        !v4JobRequest.getCriteria().getClusterCriteria().get(0).getId().isPresent()
        !v4JobRequest.getCriteria().getClusterCriteria().get(0).getName().isPresent()
        !v4JobRequest.getCriteria().getClusterCriteria().get(0).getStatus().isPresent()
        v4JobRequest.getCriteria().getClusterCriteria().get(0).getTags() == clusterCriteria.get(0).getTags()
        v4JobRequest.getCriteria().getClusterCriteria().get(1).getId().isPresent()
        v4JobRequest.getCriteria().getClusterCriteria().get(1).getName().isPresent()
        !v4JobRequest.getCriteria().getClusterCriteria().get(1).getStatus().isPresent()
        v4JobRequest.getCriteria().getClusterCriteria().get(1).getTags().isEmpty()
    }

    @Unroll
    def "Can convert V3 Job Request to V4 with command args tokenization: #tokenize"() {

        def commandArgs = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def expectedCommandArgs
        if (tokenize) {
            expectedCommandArgs = commandArgs
        } else {
            expectedCommandArgs = [StringUtils.join(commandArgs, StringUtils.SPACE)] as List
        }
        def v3JobRequest = new com.netflix.genie.common.dto.JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Lists.newArrayList(
                new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString())),
            ),
            Sets.newHashSet(UUID.randomUUID().toString())
        )
            .withCommandArgs(commandArgs)
            .build()

        when:
        def v4JobRequest = DtoConverters.toV4JobRequest(v3JobRequest, tokenize)

        then:
        v4JobRequest.getCommandArgs() == expectedCommandArgs

        where:
        tokenize | _
        true     | _
        false    | _
    }

    @Unroll
    def "Can convert v3 #v3Status to v4 #v4Status Application Status"() {
        expect:
        DtoConverters.toV4ApplicationStatus(v3Status) == v4Status

        where:
        v3Status                                                  | v4Status
        com.netflix.genie.common.dto.ApplicationStatus.ACTIVE     | ApplicationStatus.ACTIVE
        com.netflix.genie.common.dto.ApplicationStatus.INACTIVE   | ApplicationStatus.INACTIVE
        com.netflix.genie.common.dto.ApplicationStatus.DEPRECATED | ApplicationStatus.DEPRECATED
    }

    @Unroll
    def "Can successfully convert all V3 application statuses: #status"() {
        when:
        DtoConverters.toV4ApplicationStatus((com.netflix.genie.common.dto.ApplicationStatus) status)

        then:
        noExceptionThrown()

        where:
        status << com.netflix.genie.common.dto.ApplicationStatus.values()
    }

    @Unroll
    def "Can convert v4 #v4Status to v3 #v3Status Application Status"() {
        expect:
        DtoConverters.toV3ApplicationStatus(v4Status) == v3Status

        where:
        v3Status                                                  | v4Status
        com.netflix.genie.common.dto.ApplicationStatus.ACTIVE     | ApplicationStatus.ACTIVE
        com.netflix.genie.common.dto.ApplicationStatus.INACTIVE   | ApplicationStatus.INACTIVE
        com.netflix.genie.common.dto.ApplicationStatus.DEPRECATED | ApplicationStatus.DEPRECATED
    }

    @Unroll
    def "Can successfully convert all V4 application statuses: #status"() {
        when:
        DtoConverters.toV3ApplicationStatus((ApplicationStatus) status)

        then:
        noExceptionThrown()

        where:
        status << ApplicationStatus.values()
    }

    @Unroll
    def "can convert #input to application status #status"() {
        expect:
        DtoConverters.toV4ApplicationStatus(input) == status

        where:
        input                                                            | status
        ApplicationStatus.ACTIVE.name()                                  | ApplicationStatus.ACTIVE
        ApplicationStatus.ACTIVE.name().toLowerCase()                    | ApplicationStatus.ACTIVE
        ApplicationStatus.INACTIVE.name()                                | ApplicationStatus.INACTIVE
        ApplicationStatus.INACTIVE.name().toLowerCase()                  | ApplicationStatus.INACTIVE
        ApplicationStatus.DEPRECATED.name()                              | ApplicationStatus.DEPRECATED
        ApplicationStatus.DEPRECATED.name().toLowerCase()                | ApplicationStatus.DEPRECATED
        com.netflix.genie.common.dto.ApplicationStatus.ACTIVE.name()     | ApplicationStatus.ACTIVE
        com.netflix.genie.common.dto.ApplicationStatus.INACTIVE.name()   | ApplicationStatus.INACTIVE
        com.netflix.genie.common.dto.ApplicationStatus.DEPRECATED.name() | ApplicationStatus.DEPRECATED
    }

    @Unroll
    def "all possible V3 and V4 ApplicationStatus values are covered: #input"() {
        when:
        DtoConverters.toV4ApplicationStatus(input as String)

        then:
        noExceptionThrown()

        where:
        input << Stream.concat(
            Stream.of(ApplicationStatus.values()).map { status -> status.name() },
            Stream.of(com.netflix.genie.common.dto.ApplicationStatus.values()).map { status -> status.name() }
        )
            .collect()
    }

    @Unroll
    def "Can convert v3 #v3Status to v4 #v4Status Command Status"() {
        expect:
        DtoConverters.toV4CommandStatus(v3Status) == v4Status

        where:
        v3Status                                              | v4Status
        com.netflix.genie.common.dto.CommandStatus.ACTIVE     | CommandStatus.ACTIVE
        com.netflix.genie.common.dto.CommandStatus.INACTIVE   | CommandStatus.INACTIVE
        com.netflix.genie.common.dto.CommandStatus.DEPRECATED | CommandStatus.DEPRECATED
    }

    @Unroll
    def "Can successfully convert all V3 command statuses: #status"() {
        when:
        DtoConverters.toV4CommandStatus((com.netflix.genie.common.dto.CommandStatus) status)

        then:
        noExceptionThrown()

        where:
        status << com.netflix.genie.common.dto.CommandStatus.values()
    }

    @Unroll
    def "Can convert v4 #v4Status to v3 #v3Status Command Status"() {
        expect:
        DtoConverters.toV3CommandStatus(v4Status) == v3Status

        where:
        v3Status                                              | v4Status
        com.netflix.genie.common.dto.CommandStatus.ACTIVE     | CommandStatus.ACTIVE
        com.netflix.genie.common.dto.CommandStatus.INACTIVE   | CommandStatus.INACTIVE
        com.netflix.genie.common.dto.CommandStatus.DEPRECATED | CommandStatus.DEPRECATED
    }

    @Unroll
    def "Can successfully convert all V4 command statuses: #status"() {
        when:
        DtoConverters.toV3CommandStatus((CommandStatus) status)

        then:
        noExceptionThrown()

        where:
        status << CommandStatus.values()
    }

    @Unroll
    def "can convert #input to command status #status"() {
        expect:
        DtoConverters.toV4CommandStatus(input) == status

        where:
        input                                                        | status
        CommandStatus.ACTIVE.name()                                  | CommandStatus.ACTIVE
        CommandStatus.ACTIVE.name().toLowerCase()                    | CommandStatus.ACTIVE
        CommandStatus.INACTIVE.name()                                | CommandStatus.INACTIVE
        CommandStatus.INACTIVE.name().toLowerCase()                  | CommandStatus.INACTIVE
        CommandStatus.DEPRECATED.name()                              | CommandStatus.DEPRECATED
        CommandStatus.DEPRECATED.name().toLowerCase()                | CommandStatus.DEPRECATED
        com.netflix.genie.common.dto.CommandStatus.ACTIVE.name()     | CommandStatus.ACTIVE
        com.netflix.genie.common.dto.CommandStatus.INACTIVE.name()   | CommandStatus.INACTIVE
        com.netflix.genie.common.dto.CommandStatus.DEPRECATED.name() | CommandStatus.DEPRECATED
    }

    @Unroll
    def "all possible CommandStatus values are covered: #input"() {
        when:
        DtoConverters.toV4CommandStatus(input as String)

        then:
        noExceptionThrown()

        where:
        input << Stream.concat(
            Stream.of(CommandStatus.values()).map { status -> status.name() },
            Stream.of(com.netflix.genie.common.dto.CommandStatus.values()).map { status -> status.name() }
        )
            .collect()
    }

    @Unroll
    def "Can convert v3 #v3Status to v4 #v4Status Cluster Status"() {
        expect:
        DtoConverters.toV4ClusterStatus(v3Status) == v4Status

        where:
        v3Status                                                  | v4Status
        com.netflix.genie.common.dto.ClusterStatus.UP             | ClusterStatus.UP
        com.netflix.genie.common.dto.ClusterStatus.TERMINATED     | ClusterStatus.TERMINATED
        com.netflix.genie.common.dto.ClusterStatus.OUT_OF_SERVICE | ClusterStatus.OUT_OF_SERVICE
    }

    @Unroll
    def "Can successfully convert all V3 cluster statuses: #status"() {
        when:
        DtoConverters.toV4ClusterStatus((com.netflix.genie.common.dto.ClusterStatus) status)

        then:
        noExceptionThrown()

        where:
        status << com.netflix.genie.common.dto.ClusterStatus.values()
    }

    @Unroll
    def "Can convert v4 #v4Status to v3 #v3Status Cluster Status"() {
        expect:
        DtoConverters.toV3ClusterStatus(v4Status) == v3Status

        where:
        v3Status                                                  | v4Status
        com.netflix.genie.common.dto.ClusterStatus.UP             | ClusterStatus.UP
        com.netflix.genie.common.dto.ClusterStatus.TERMINATED     | ClusterStatus.TERMINATED
        com.netflix.genie.common.dto.ClusterStatus.OUT_OF_SERVICE | ClusterStatus.OUT_OF_SERVICE
    }

    @Unroll
    def "Can successfully convert all V4 cluster statuses: #status"() {
        when:
        DtoConverters.toV3ClusterStatus((ClusterStatus) status)

        then:
        noExceptionThrown()

        where:
        status << ClusterStatus.values()
    }

    @Unroll
    def "can convert #input to cluster status #status"() {
        expect:
        DtoConverters.toV4ClusterStatus(input) == status

        where:
        input                                                            | status
        ClusterStatus.UP.name()                                          | ClusterStatus.UP
        ClusterStatus.UP.name().toLowerCase()                            | ClusterStatus.UP
        ClusterStatus.TERMINATED.name()                                  | ClusterStatus.TERMINATED
        ClusterStatus.TERMINATED.name().toLowerCase()                    | ClusterStatus.TERMINATED
        ClusterStatus.OUT_OF_SERVICE.name()                              | ClusterStatus.OUT_OF_SERVICE
        ClusterStatus.OUT_OF_SERVICE.name().toLowerCase()                | ClusterStatus.OUT_OF_SERVICE
        com.netflix.genie.common.dto.ClusterStatus.UP.name()             | ClusterStatus.UP
        com.netflix.genie.common.dto.ClusterStatus.TERMINATED.name()     | ClusterStatus.TERMINATED
        com.netflix.genie.common.dto.ClusterStatus.OUT_OF_SERVICE.name() | ClusterStatus.OUT_OF_SERVICE
    }

    @Unroll
    def "all possible ClusterStatus name values are covered: #input"() {
        when:
        DtoConverters.toV4ClusterStatus(input as String)

        then:
        noExceptionThrown()

        where:
        input << Stream.concat(
            Stream.of(ClusterStatus.values()).map { status -> status.name() },
            Stream.of(com.netflix.genie.common.dto.ClusterStatus.values()).map { status -> status.name() }
        )
            .collect()
    }

    @Unroll
    def "Can convert v3 #v3Status to v4 #v4Status Job Status"() {
        expect:
        DtoConverters.toV4JobStatus(v3Status) == v4Status

        where:
        v3Status                                         | v4Status
        com.netflix.genie.common.dto.JobStatus.ACCEPTED  | JobStatus.ACCEPTED
        com.netflix.genie.common.dto.JobStatus.CLAIMED   | JobStatus.CLAIMED
        com.netflix.genie.common.dto.JobStatus.FAILED    | JobStatus.FAILED
        com.netflix.genie.common.dto.JobStatus.INIT      | JobStatus.INIT
        com.netflix.genie.common.dto.JobStatus.INVALID   | JobStatus.INVALID
        com.netflix.genie.common.dto.JobStatus.KILLED    | JobStatus.KILLED
        com.netflix.genie.common.dto.JobStatus.RESERVED  | JobStatus.RESERVED
        com.netflix.genie.common.dto.JobStatus.RESOLVED  | JobStatus.RESOLVED
        com.netflix.genie.common.dto.JobStatus.RUNNING   | JobStatus.RUNNING
        com.netflix.genie.common.dto.JobStatus.SUCCEEDED | JobStatus.SUCCEEDED
    }

    @Unroll
    def "Can successfully convert all V3 job statuses: #status"() {
        when:
        DtoConverters.toV4JobStatus((com.netflix.genie.common.dto.JobStatus) status)

        then:
        noExceptionThrown()

        where:
        status << com.netflix.genie.common.dto.JobStatus.values()
    }

    @Unroll
    def "Can convert v4 #v4Status to v3 #v3Status Job Status"() {
        expect:
        DtoConverters.toV3JobStatus(v4Status) == v3Status

        where:
        v3Status                                         | v4Status
        com.netflix.genie.common.dto.JobStatus.ACCEPTED  | JobStatus.ACCEPTED
        com.netflix.genie.common.dto.JobStatus.CLAIMED   | JobStatus.CLAIMED
        com.netflix.genie.common.dto.JobStatus.FAILED    | JobStatus.FAILED
        com.netflix.genie.common.dto.JobStatus.INIT      | JobStatus.INIT
        com.netflix.genie.common.dto.JobStatus.INVALID   | JobStatus.INVALID
        com.netflix.genie.common.dto.JobStatus.KILLED    | JobStatus.KILLED
        com.netflix.genie.common.dto.JobStatus.RESERVED  | JobStatus.RESERVED
        com.netflix.genie.common.dto.JobStatus.RESOLVED  | JobStatus.RESOLVED
        com.netflix.genie.common.dto.JobStatus.RUNNING   | JobStatus.RUNNING
        com.netflix.genie.common.dto.JobStatus.SUCCEEDED | JobStatus.SUCCEEDED
    }

    @Unroll
    def "Can successfully convert all V4 job statuses: #status"() {
        when:
        DtoConverters.toV3JobStatus((JobStatus) status)

        then:
        noExceptionThrown()

        where:
        status << JobStatus.values()
    }

    @Unroll
    def "can convert #input to job status #status"() {
        expect:
        DtoConverters.toV4JobStatus(input) == status

        where:
        input                                                   | status
        JobStatus.ACCEPTED.name()                               | JobStatus.ACCEPTED
        JobStatus.ACCEPTED.name().toLowerCase()                 | JobStatus.ACCEPTED
        JobStatus.CLAIMED.name()                                | JobStatus.CLAIMED
        JobStatus.CLAIMED.name().toLowerCase()                  | JobStatus.CLAIMED
        JobStatus.FAILED.name()                                 | JobStatus.FAILED
        JobStatus.FAILED.name().toLowerCase()                   | JobStatus.FAILED
        JobStatus.INIT.name()                                   | JobStatus.INIT
        JobStatus.INIT.name().toLowerCase()                     | JobStatus.INIT
        JobStatus.INVALID.name()                                | JobStatus.INVALID
        JobStatus.INVALID.name().toLowerCase()                  | JobStatus.INVALID
        JobStatus.KILLED.name()                                 | JobStatus.KILLED
        JobStatus.KILLED.name().toLowerCase()                   | JobStatus.KILLED
        JobStatus.RESOLVED.name()                               | JobStatus.RESOLVED
        JobStatus.RESOLVED.name().toLowerCase()                 | JobStatus.RESOLVED
        JobStatus.RESERVED.name()                               | JobStatus.RESERVED
        JobStatus.RESERVED.name().toLowerCase()                 | JobStatus.RESERVED
        JobStatus.RUNNING.name()                                | JobStatus.RUNNING
        JobStatus.RUNNING.name().toLowerCase()                  | JobStatus.RUNNING
        JobStatus.SUCCEEDED.name()                              | JobStatus.SUCCEEDED
        JobStatus.SUCCEEDED.name().toLowerCase()                | JobStatus.SUCCEEDED
        com.netflix.genie.common.dto.JobStatus.ACCEPTED.name()  | JobStatus.ACCEPTED
        com.netflix.genie.common.dto.JobStatus.CLAIMED.name()   | JobStatus.CLAIMED
        com.netflix.genie.common.dto.JobStatus.FAILED.name()    | JobStatus.FAILED
        com.netflix.genie.common.dto.JobStatus.INIT.name()      | JobStatus.INIT
        com.netflix.genie.common.dto.JobStatus.INVALID.name()   | JobStatus.INVALID
        com.netflix.genie.common.dto.JobStatus.KILLED.name()    | JobStatus.KILLED
        com.netflix.genie.common.dto.JobStatus.RESOLVED.name()  | JobStatus.RESOLVED
        com.netflix.genie.common.dto.JobStatus.RESERVED.name()  | JobStatus.RESERVED
        com.netflix.genie.common.dto.JobStatus.RUNNING.name()   | JobStatus.RUNNING
        com.netflix.genie.common.dto.JobStatus.SUCCEEDED.name() | JobStatus.SUCCEEDED
    }

    @Unroll
    def "all possible JobStatus name values are covered: #input"() {
        when:
        DtoConverters.toV4JobStatus(input as String)

        then:
        noExceptionThrown()

        where:
        input << Stream.concat(
            Stream.of(JobStatus.values()).map { status -> status.name() },
            Stream.of(com.netflix.genie.common.dto.JobStatus.values()).map { status -> status.name() }
        )
            .collect()
    }

    @Unroll
    def "string status conversions throw IllegalArgumentException on bad input #input"() {
        when:
        DtoConverters.toV4ApplicationStatus(input)

        then:
        thrown(IllegalArgumentException)

        when:
        DtoConverters.toV4CommandStatus(input)

        then:
        thrown(IllegalArgumentException)

        when:
        DtoConverters.toV4ClusterStatus(input)

        then:
        thrown(IllegalArgumentException)

        when:
        DtoConverters.toV4JobStatus(input)

        then:
        thrown(IllegalArgumentException)

        where:
        input                        | _
        ""                           | _
        null                         | _
        " "                          | _
        UUID.randomUUID().toString() | _
    }
}
