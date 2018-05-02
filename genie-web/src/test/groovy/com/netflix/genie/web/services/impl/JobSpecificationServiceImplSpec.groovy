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
package com.netflix.genie.web.services.impl

import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.google.common.collect.Sets
import com.netflix.genie.common.dto.ClusterStatus
import com.netflix.genie.common.dto.CommandStatus
import com.netflix.genie.common.internal.dto.v4.AgentConfigRequest
import com.netflix.genie.common.internal.dto.v4.Cluster
import com.netflix.genie.common.internal.dto.v4.ClusterMetadata
import com.netflix.genie.common.internal.dto.v4.Command
import com.netflix.genie.common.internal.dto.v4.CommandMetadata
import com.netflix.genie.common.internal.dto.v4.Criterion
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment
import com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria
import com.netflix.genie.common.internal.dto.v4.JobMetadata
import com.netflix.genie.common.internal.dto.v4.JobRequest
import com.netflix.genie.common.internal.jobs.JobConstants
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.properties.JobsProperties
import com.netflix.genie.web.services.ApplicationPersistenceService
import com.netflix.genie.web.services.ClusterLoadBalancer
import com.netflix.genie.web.services.ClusterPersistenceService
import com.netflix.genie.web.services.CommandPersistenceService
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.commons.lang3.StringUtils
import org.junit.experimental.categories.Category
import spock.lang.Specification

import java.time.Instant

/**
 * Specifications for the {@link JobSpecificationServiceImpl} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class JobSpecificationServiceImplSpec extends Specification {

    def "Can generate job specification"() {
        def jobId = UUID.randomUUID().toString()
        def jobName = UUID.randomUUID().toString()
        def commandArgs = Lists.newArrayList(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        def cluster1Id = UUID.randomUUID().toString()
        def cluster2Id = UUID.randomUUID().toString()
        def clusterTags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def commandId = UUID.randomUUID().toString()
        def commandTags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def clusterCriteria = Lists.newArrayList(
                new Criterion
                        .Builder()
                        .withTags(Sets.newHashSet(UUID.randomUUID().toString()))
                        .build(),
                new Criterion
                        .Builder()
                        .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                        .build()
        )
        def commandCriterion = new Criterion.Builder().withTags(Sets.newHashSet(UUID.randomUUID().toString())).build()
        def jobRequest = new JobRequest(
                null,
                null,
                commandArgs,
                new JobMetadata.Builder(jobName, UUID.randomUUID().toString()).build(),
                new ExecutionResourceCriteria(clusterCriteria, commandCriterion, null),
                null,
                null
        )
        def cluster1 = new Cluster(
                cluster1Id,
                Instant.now(),
                Instant.now(),
                new ExecutionEnvironment(null, null, null),
                new ClusterMetadata.Builder(
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        ClusterStatus.UP
                ).withTags(clusterTags).build()
        )
        def cluster2 = new Cluster(
                cluster2Id,
                Instant.now(),
                Instant.now(),
                new ExecutionEnvironment(null, null, null),
                new ClusterMetadata.Builder(
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        ClusterStatus.UP
                ).withTags(clusterTags).build()
        )

        def clusters = Sets.newHashSet(cluster1, cluster2)
        def executableBinary = UUID.randomUUID().toString()
        def executableArgument0 = UUID.randomUUID().toString()
        def executableArgument1 = UUID.randomUUID().toString()
        def executable = Lists.newArrayList(executableBinary, executableArgument0, executableArgument1)
        def command = new Command(
                commandId,
                Instant.now(),
                Instant.now(),
                new ExecutionEnvironment(null, null, null),
                new CommandMetadata.Builder(
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        CommandStatus.ACTIVE
                ).withTags(commandTags).build(),
                executable,
                null,
                100L
        )

        def jobCommandArgs = Lists.newArrayList(executableBinary, executableArgument0, executableArgument1)
        jobCommandArgs.addAll(commandArgs)

        def jobsProperties = new JobsProperties()
        Map<Cluster, String> clusterCommandMap = Maps.newHashMap()
        clusterCommandMap.put(cluster1, commandId)
        clusterCommandMap.put(cluster2, commandId)
        def clusterService = Mock(ClusterPersistenceService) {
            1 * findClustersAndCommandsForCriteria(clusterCriteria, commandCriterion) >> clusterCommandMap
        }
        def loadBalancer = Mock(ClusterLoadBalancer) {
            1 * selectCluster(clusters, _ as com.netflix.genie.common.dto.JobRequest) >> cluster1
        }
        def applicationService = Mock(ApplicationPersistenceService)
        def commandService = Mock(CommandPersistenceService) {
            1 * getCommand(commandId) >> command
            1 * getApplicationsForCommand(commandId) >> Lists.newArrayList()
        }
        def service = new JobSpecificationServiceImpl(
                applicationService,
                clusterService,
                commandService,
                Lists.newArrayList(loadBalancer),
                new SimpleMeterRegistry(),
                jobsProperties
        )

        when:
        def jobSpec = service.resolveJobSpecification(jobId, jobRequest)

        then:
        jobSpec.getCommandArgs() == jobCommandArgs
        jobSpec.getJob().getId() == jobId
        jobSpec.getCluster().getId() == cluster1Id
        jobSpec.getCommand().getId() == commandId
        jobSpec.getApplications().isEmpty()
        !jobSpec.isInteractive()
        jobSpec.getEnvironmentVariables().size() == 15
    }

    def "Can convert tags to string"() {
        def service = new JobSpecificationServiceImpl(
                Mock(ApplicationPersistenceService),
                Mock(ClusterPersistenceService),
                Mock(CommandPersistenceService),
                Lists.newArrayList(),
                Mock(MeterRegistry),
                new JobsProperties()
        )

        expect:
        service.tagsToString(input) == output

        where:
        input                                                       | output
        Sets.newHashSet()                                           | ""
        Sets.newHashSet("some.tag:t")                               | "some.tag:t"
        Sets.newHashSet("foo", "bar")                               | "bar,foo"
        Sets.newHashSet("bar", "foo")                               | "bar,foo"
        Sets.newHashSet("foo", "bar", "tag,with,commas")            | "bar,foo,tag,with,commas"
        Sets.newHashSet("foo", "bar", "tag with spaces")            | "bar,foo,tag with spaces"
        Sets.newHashSet("foo", "bar", "tag\nwith\nnewlines")        | "bar,foo,tag\nwith\nnewlines"
        Sets.newHashSet("foo", "bar", "\"tag-with-double-quotes\"") | "\"tag-with-double-quotes\",bar,foo"
        Sets.newHashSet("foo", "bar", "\'tag-with-single-quotes\'") | "\'tag-with-single-quotes\',bar,foo"
    }

    def "Can generate correct environment variables"() {
        def jobsProperties = new JobsProperties()
        def service = new JobSpecificationServiceImpl(
                Mock(ApplicationPersistenceService),
                Mock(ClusterPersistenceService),
                Mock(CommandPersistenceService),
                Lists.newArrayList(),
                Mock(MeterRegistry),
                jobsProperties
        )
        def jobId = UUID.randomUUID().toString()
        def jobName = UUID.randomUUID().toString()
        def clusterId = UUID.randomUUID().toString()
        def clusterName = UUID.randomUUID().toString()
        def clusterTags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def commandId = UUID.randomUUID().toString()
        def commandName = UUID.randomUUID().toString()
        def commandTags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def clusterCriteria = Lists.newArrayList(
                new Criterion
                        .Builder()
                        .withTags(Sets.newHashSet(UUID.randomUUID().toString()))
                        .build(),
                new Criterion
                        .Builder()
                        .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                        .build()
        )
        def commandCriterion = new Criterion.Builder().withTags(Sets.newHashSet(UUID.randomUUID().toString())).build()
        def jobRequest = new JobRequest(
                null,
                null,
                null,
                new JobMetadata
                        .Builder(jobName, UUID.randomUUID().toString())
                        .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                        .build(),
                new ExecutionResourceCriteria(clusterCriteria, commandCriterion, null),
                null,
                null
        )
        def cluster = new Cluster(
                clusterId,
                Instant.now(),
                Instant.now(),
                new ExecutionEnvironment(null, null, null),
                new ClusterMetadata.Builder(
                        clusterName,
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        ClusterStatus.UP
                ).withTags(clusterTags).build()
        )
        def command = new Command(
                commandId,
                Instant.now(),
                Instant.now(),
                new ExecutionEnvironment(null, null, null),
                new CommandMetadata.Builder(
                        commandName,
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        CommandStatus.ACTIVE
                ).withTags(commandTags).build(),
                Lists.newArrayList(UUID.randomUUID().toString()),
                null,
                100L
        )

        when:
        def envVariables = service.generateEnvironmentVariables(jobId, jobRequest, cluster, command)

        then:
        envVariables.get("GENIE_VERSION") == "4"
        envVariables.get(JobConstants.GENIE_CLUSTER_ID_ENV_VAR) == clusterId
        envVariables.get(JobConstants.GENIE_CLUSTER_NAME_ENV_VAR) == clusterName
        envVariables.get(JobConstants.GENIE_CLUSTER_TAGS_ENV_VAR) == service.tagsToString(clusterTags)
        envVariables.get(JobConstants.GENIE_COMMAND_ID_ENV_VAR) == commandId
        envVariables.get(JobConstants.GENIE_COMMAND_NAME_ENV_VAR) == commandName
        envVariables.get(JobConstants.GENIE_COMMAND_TAGS_ENV_VAR) == service.tagsToString(commandTags)
        envVariables.get(JobConstants.GENIE_JOB_ID_ENV_VAR) == jobId
        envVariables.get(JobConstants.GENIE_JOB_NAME_ENV_VAR) == jobName
        envVariables.get(JobConstants.GENIE_JOB_MEMORY_ENV_VAR) == String.valueOf(jobsProperties.getMemory().getDefaultJobMemory())
        envVariables.get(JobConstants.GENIE_REQUESTED_COMMAND_TAGS_ENV_VAR) == service.tagsToString(commandCriterion.getTags())
        envVariables.get(JobConstants.GENIE_REQUESTED_CLUSTER_TAGS_ENV_VAR + "_0") == service.tagsToString(clusterCriteria.get(0).getTags())
        envVariables.get(JobConstants.GENIE_REQUESTED_CLUSTER_TAGS_ENV_VAR + "_1") == service.tagsToString(clusterCriteria.get(1).getTags())
        envVariables.get(JobConstants.GENIE_REQUESTED_CLUSTER_TAGS_ENV_VAR) == "[[" + service.tagsToString(clusterCriteria.get(0).getTags()) + "],[" + service.tagsToString(clusterCriteria.get(1).getTags()) + "]]"
        envVariables.get(JobConstants.GENIE_JOB_TAGS_ENV_VAR) == service.tagsToString(jobRequest.getMetadata().getTags())
    }

    def "Can convert V4 Criterion to V3 tags"() {
        def jobsProperties = new JobsProperties()
        def service = new JobSpecificationServiceImpl(
                Mock(ApplicationPersistenceService),
                Mock(ClusterPersistenceService),
                Mock(CommandPersistenceService),
                Lists.newArrayList(),
                Mock(MeterRegistry),
                jobsProperties
        )
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        Set<String> v3Tags

        when:
        v3Tags = service.toV3Tags(new Criterion.Builder().withId(id).withName(name).withTags(tags).build())

        then:
        v3Tags.size() == 4
        v3Tags.contains("genie.id:" + id)
        v3Tags.contains("genie.name:" + name)
        v3Tags.containsAll(tags)

        when:
        v3Tags = service.toV3Tags(new Criterion.Builder().withId(id).withTags(tags).build())

        then:
        v3Tags.size() == 3
        v3Tags.contains("genie.id:" + id)
        v3Tags.containsAll(tags)

        when:
        v3Tags = service.toV3Tags(new Criterion.Builder().withName(name).withTags(tags).build())

        then:
        v3Tags.size() == 3
        v3Tags.contains("genie.name:" + name)
        v3Tags.containsAll(tags)

        when:
        v3Tags = service.toV3Tags(new Criterion.Builder().withTags(tags).build())

        then:
        v3Tags.size() == 2
        v3Tags.containsAll(tags)
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


        def jobsProperties = new JobsProperties()
        def service = new JobSpecificationServiceImpl(
                Mock(ApplicationPersistenceService),
                Mock(ClusterPersistenceService),
                Mock(CommandPersistenceService),
                Lists.newArrayList(),
                Mock(MeterRegistry),
                jobsProperties
        )

        def jobRequest = new JobRequest(
                null,
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
                        .withArchivingDisabled(true)
                        .withInteractive(true)
                        .withTimeoutRequested(timeout)
                        .build()
        )

        when:
        def v3JobRequest = service.toV3JobRequest(id, jobRequest)

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
        v3JobRequest.getClusterCriterias().get(0).getTags() == clusterCriteria.get(0).getTags()
        v3JobRequest.getClusterCriterias().get(1).getTags() == service.toV3Tags(clusterCriteria.get(1))
    }
}
