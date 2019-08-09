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
import com.google.common.collect.Sets
import com.netflix.genie.common.dto.ClusterStatus
import com.netflix.genie.common.dto.CommandStatus
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.internal.dto.v4.AgentConfigRequest
import com.netflix.genie.common.internal.dto.v4.Cluster
import com.netflix.genie.common.internal.dto.v4.ClusterMetadata
import com.netflix.genie.common.internal.dto.v4.Command
import com.netflix.genie.common.internal.dto.v4.CommandMetadata
import com.netflix.genie.common.internal.dto.v4.Criterion
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment
import com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria
import com.netflix.genie.common.internal.dto.v4.JobArchivalDataRequest
import com.netflix.genie.common.internal.dto.v4.JobEnvironmentRequest
import com.netflix.genie.common.internal.dto.v4.JobMetadata
import com.netflix.genie.common.internal.dto.v4.JobRequest
import com.netflix.genie.common.internal.jobs.JobConstants
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.web.data.services.ApplicationPersistenceService
import com.netflix.genie.web.data.services.ClusterPersistenceService
import com.netflix.genie.web.data.services.CommandPersistenceService
import com.netflix.genie.web.data.services.JobPersistenceService
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.properties.JobsProperties
import com.netflix.genie.web.services.ClusterLoadBalancer
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.commons.lang3.StringUtils
import spock.lang.Specification
import wiremock.com.google.common.collect.ImmutableMap

import javax.annotation.Nullable
import java.time.Instant

/**
 * Specifications for the {@link JobResolverServiceImpl} class.
 *
 * @author tgianos
 */
class JobResolverServiceImplSpec extends Specification {

    def "Can resolve a job"() {
        def cluster1Id = UUID.randomUUID().toString()
        def cluster2Id = UUID.randomUUID().toString()
        def cluster1 = createCluster(cluster1Id)
        def cluster2 = createCluster(cluster2Id)
        def clusters = Sets.newHashSet(cluster1, cluster2)

        def commandId = UUID.randomUUID().toString()
        def executableBinary = UUID.randomUUID().toString()
        def executableArgument0 = UUID.randomUUID().toString()
        def executableArgument1 = UUID.randomUUID().toString()
        def executable = Lists.newArrayList(executableBinary, executableArgument0, executableArgument1)
        def command = createCommand(commandId, executable)

        def jobId = UUID.randomUUID().toString()
        def commandArgs = Lists.newArrayList(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
        def requestedArchiveLocationPrefix = UUID.randomUUID().toString()

        def expectedJobCommandArgs = Lists.newArrayList(executableBinary, executableArgument0, executableArgument1)
        expectedJobCommandArgs.addAll(commandArgs)
        Map<Cluster, String> clusterCommandMap = ImmutableMap.of(cluster1, commandId, cluster2, commandId)
        def jobRequest = createJobRequest(commandArgs, requestedArchiveLocationPrefix, null)
        def jobRequestNoArchivalData = createJobRequest(commandArgs, null, null)
        def requestedMemory = 6_323
        def savedJobRequest = createJobRequest(commandArgs, null, requestedMemory)

        def clusterService = Mock(ClusterPersistenceService)
        def loadBalancer = Mock(ClusterLoadBalancer)
        def applicationService = Mock(ApplicationPersistenceService)
        def commandService = Mock(CommandPersistenceService)
        def jobPersistenceService = Mock(JobPersistenceService)
        def jobsProperties = JobsProperties.getJobsPropertiesDefaults();
        def service = new JobResolverServiceImpl(
            applicationService,
            clusterService,
            commandService,
            jobPersistenceService,
            Lists.newArrayList(loadBalancer),
            new SimpleMeterRegistry(),
            jobsProperties
        )

        when:
        def resolvedJob = service.resolveJob(jobId, jobRequest)
        def jobSpec = resolvedJob.getJobSpecification()
        def jobEnvironment = resolvedJob.getJobEnvironment()

        then:
        0 * jobPersistenceService.saveResolvedJob(_ as String, _ as ResolvedJob)
        1 * clusterService.findClustersAndCommandsForCriteria(
            jobRequest.getCriteria().getClusterCriteria(),
            jobRequest.getCriteria().getCommandCriterion()
        ) >> clusterCommandMap
        1 * loadBalancer.selectCluster(clusters, _ as com.netflix.genie.common.dto.JobRequest) >> cluster1
        1 * commandService.getCommand(commandId) >> command
        1 * commandService.getApplicationsForCommand(commandId) >> Lists.newArrayList()
        jobSpec.getCommandArgs() == expectedJobCommandArgs
        jobSpec.getJob().getId() == jobId
        jobSpec.getCluster().getId() == cluster1Id
        jobSpec.getCommand().getId() == commandId
        jobSpec.getApplications().isEmpty()
        !jobSpec.isInteractive()
        jobSpec.getEnvironmentVariables().size() == 17
        jobSpec.getArchiveLocation() == Optional.of(requestedArchiveLocationPrefix + File.separator + jobId)
        jobEnvironment.getEnvironmentVariables() == jobSpec.getEnvironmentVariables()
        jobEnvironment.getMemory() == jobsProperties.getMemory().getDefaultJobMemory()
        jobEnvironment.getCpu() == 1
        !jobEnvironment.getExt().isPresent()

        when:
        def resolvedJobNoArchivalData = service.resolveJob(jobId, jobRequestNoArchivalData)
        def jobSpecNoArchivalData = resolvedJobNoArchivalData.getJobSpecification()
        def jobEnvironmentNoArchivalData = resolvedJobNoArchivalData.getJobEnvironment()

        then:
        0 * jobPersistenceService.saveResolvedJob(_ as String, _ as ResolvedJob)
        1 * clusterService.findClustersAndCommandsForCriteria(
            jobRequestNoArchivalData.getCriteria().getClusterCriteria(),
            jobRequestNoArchivalData.getCriteria().getCommandCriterion()
        ) >> clusterCommandMap
        1 * loadBalancer.selectCluster(clusters, _ as com.netflix.genie.common.dto.JobRequest) >> cluster1
        1 * commandService.getCommand(commandId) >> command
        1 * commandService.getApplicationsForCommand(commandId) >> Lists.newArrayList()
        jobSpecNoArchivalData.getCommandArgs() == expectedJobCommandArgs
        jobSpecNoArchivalData.getJob().getId() == jobId
        jobSpecNoArchivalData.getCluster().getId() == cluster1Id
        jobSpecNoArchivalData.getCommand().getId() == commandId
        jobSpecNoArchivalData.getApplications().isEmpty()
        !jobSpecNoArchivalData.isInteractive()
        jobSpecNoArchivalData.getEnvironmentVariables().size() == 17
        jobSpecNoArchivalData.getArchiveLocation() ==
            Optional.of(
                StringUtils.endsWith(jobsProperties.getLocations().getArchives(), File.separator)
                    ? jobsProperties.getLocations().getArchives() + jobId
                    : jobsProperties.getLocations().getArchives() + File.separator + jobId
            )
        jobEnvironmentNoArchivalData.getEnvironmentVariables() == jobSpecNoArchivalData.getEnvironmentVariables()
        jobEnvironmentNoArchivalData.getMemory() == jobsProperties.getMemory().getDefaultJobMemory()
        jobEnvironmentNoArchivalData.getCpu() == 1
        !jobEnvironmentNoArchivalData.getExt().isPresent()

        when: "We try to resolve a saved job"
        def resolvedSavedJobData = service.resolveJob(jobId)
        def jobSpecSavedData = resolvedSavedJobData.getJobSpecification()
        def jobEnvironmentSavedData = resolvedSavedJobData.getJobEnvironment()

        then: "the resolution is saved"
        1 * jobPersistenceService.getJobStatus(jobId) >> JobStatus.RESERVED
        1 * jobPersistenceService.getJobRequest(jobId) >> Optional.of(savedJobRequest)
        1 * clusterService.findClustersAndCommandsForCriteria(
            savedJobRequest.getCriteria().getClusterCriteria(),
            savedJobRequest.getCriteria().getCommandCriterion()
        ) >> clusterCommandMap
        1 * loadBalancer.selectCluster(clusters, _ as com.netflix.genie.common.dto.JobRequest) >> cluster1
        1 * commandService.getCommand(commandId) >> command
        1 * commandService.getApplicationsForCommand(commandId) >> Lists.newArrayList()
        1 * jobPersistenceService.saveResolvedJob(jobId, _ as ResolvedJob)
        jobSpecSavedData.getCommandArgs() == expectedJobCommandArgs
        jobSpecSavedData.getJob().getId() == jobId
        jobSpecSavedData.getCluster().getId() == cluster1Id
        jobSpecSavedData.getCommand().getId() == commandId
        jobSpecSavedData.getApplications().isEmpty()
        !jobSpecSavedData.isInteractive()
        jobSpecSavedData.getEnvironmentVariables().size() == 17
        jobSpecSavedData.getArchiveLocation() ==
            Optional.of(
                StringUtils.endsWith(jobsProperties.getLocations().getArchives(), File.separator)
                    ? jobsProperties.getLocations().getArchives() + jobId
                    : jobsProperties.getLocations().getArchives() + File.separator + jobId
            )
        jobEnvironmentSavedData.getEnvironmentVariables() == jobSpecSavedData.getEnvironmentVariables()
        jobEnvironmentSavedData.getMemory() == requestedMemory
        jobEnvironmentSavedData.getCpu() == 1
        !jobEnvironmentSavedData.getExt().isPresent()
    }

    def "Can convert tags to string"() {
        def service = new JobResolverServiceImpl(
            Mock(ApplicationPersistenceService),
            Mock(ClusterPersistenceService),
            Mock(CommandPersistenceService),
            Mock(JobPersistenceService),
            Lists.newArrayList(),
            Mock(MeterRegistry),
            JobsProperties.getJobsPropertiesDefaults()
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
        def jobsProperties = JobsProperties.getJobsPropertiesDefaults()
        def service = new JobResolverServiceImpl(
            Mock(ApplicationPersistenceService),
            Mock(ClusterPersistenceService),
            Mock(CommandPersistenceService),
            Mock(JobPersistenceService),
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
        def grouping = UUID.randomUUID().toString()
        def groupingInstance = UUID.randomUUID().toString()
        def jobRequest = new JobRequest(
            null,
            null,
            null,
            new JobMetadata
                .Builder(jobName, UUID.randomUUID().toString())
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .withGrouping(grouping)
                .withGroupingInstance(groupingInstance)
                .build(),
            new ExecutionResourceCriteria(clusterCriteria, commandCriterion, null),
            null,
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
        def envVariables = service.generateEnvironmentVariables(
            jobId,
            jobRequest,
            cluster,
            command,
            jobsProperties.getMemory().getDefaultJobMemory()
        )

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
        envVariables.get(JobConstants.GENIE_JOB_GROUPING_ENV_VAR) == grouping
        envVariables.get(JobConstants.GENIE_JOB_GROUPING_INSTANCE_ENV_VAR) == groupingInstance
    }

    def "Can convert V4 Criterion to V3 tags"() {
        def jobsProperties = JobsProperties.getJobsPropertiesDefaults()
        def service = new JobResolverServiceImpl(
            Mock(ApplicationPersistenceService),
            Mock(ClusterPersistenceService),
            Mock(CommandPersistenceService),
            Mock(JobPersistenceService),
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


        def jobsProperties = JobsProperties.getJobsPropertiesDefaults()
        def service = new JobResolverServiceImpl(
            Mock(ApplicationPersistenceService),
            Mock(ClusterPersistenceService),
            Mock(CommandPersistenceService),
            Mock(JobPersistenceService),
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
                .build(),
            new JobArchivalDataRequest
                .Builder()
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

    private static Cluster createCluster(String id) {
        return new Cluster(
            id,
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ClusterMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ClusterStatus.UP
            ).withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build()
        )
    }

    private static Command createCommand(String id, List<String> executable) {
        return new Command(
            id,
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new CommandMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.ACTIVE
            )
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build(),
            executable,
            null,
            100L
        )
    }

    private static JobRequest createJobRequest(
        List<String> commandArgs,
        @Nullable String requestedArchiveLocationPrefix,
        @Nullable Integer requestedMemory
    ) {
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
        return new JobRequest(
            null,
            null,
            commandArgs,
            new JobMetadata.Builder(UUID.randomUUID().toString(), UUID.randomUUID().toString()).build(),
            new ExecutionResourceCriteria(clusterCriteria, commandCriterion, null),
            requestedMemory == null
                ? null
                : new JobEnvironmentRequest.Builder().withRequestedJobMemory(requestedMemory).build(),
            new AgentConfigRequest.Builder().build(),
            requestedArchiveLocationPrefix == null
                ? null
                : new JobArchivalDataRequest.Builder()
                .withRequestedArchiveLocationPrefix(requestedArchiveLocationPrefix)
                .build()
        )
    }
}
