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

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.common.external.dtos.v4.AgentConfigRequest
import com.netflix.genie.common.external.dtos.v4.Cluster
import com.netflix.genie.common.external.dtos.v4.ClusterMetadata
import com.netflix.genie.common.external.dtos.v4.ClusterStatus
import com.netflix.genie.common.external.dtos.v4.Command
import com.netflix.genie.common.external.dtos.v4.CommandMetadata
import com.netflix.genie.common.external.dtos.v4.CommandStatus
import com.netflix.genie.common.external.dtos.v4.Criterion
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment
import com.netflix.genie.common.external.dtos.v4.ExecutionResourceCriteria
import com.netflix.genie.common.external.dtos.v4.JobArchivalDataRequest
import com.netflix.genie.common.external.dtos.v4.JobEnvironmentRequest
import com.netflix.genie.common.external.dtos.v4.JobMetadata
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException
import com.netflix.genie.common.internal.jobs.JobConstants
import com.netflix.genie.web.data.services.DataServices
import com.netflix.genie.web.data.services.PersistenceService
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.dtos.ResourceSelectionResult
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException
import com.netflix.genie.web.properties.JobsProperties
import com.netflix.genie.web.selectors.ClusterSelector
import com.netflix.genie.web.selectors.CommandSelector
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.commons.lang3.StringUtils
import org.springframework.core.env.Environment
import spock.lang.Specification

import javax.annotation.Nullable
import java.time.Instant

/**
 * Specifications for the {@link JobResolverServiceImpl} class.
 *
 * @author tgianos
 */
@SuppressWarnings("GroovyAccessibility")
class JobResolverServiceImplSpec extends Specification {

    PersistenceService persistenceService
    ClusterSelector clusterSelector
    CommandSelector commandSelector
    JobsProperties jobsProperties
    Environment environment

    JobResolverServiceImpl service

    def setup() {
        this.persistenceService = Mock(PersistenceService)
        this.clusterSelector = Mock(ClusterSelector)
        this.commandSelector = Mock(CommandSelector)
        this.jobsProperties = JobsProperties.getJobsPropertiesDefaults()
        this.environment = Mock(Environment)
        def dataServices = Mock(DataServices) {
            getPersistenceService() >> this.persistenceService
        }
        this.service = new JobResolverServiceImpl(
            dataServices,
            Lists.newArrayList(this.clusterSelector),
            this.commandSelector,
            new SimpleMeterRegistry(),
            this.jobsProperties,
            this.environment
        )
    }

    def "Can resolve a job with V3 algorithm"() {
        def cluster1Id = UUID.randomUUID().toString()
        def cluster2Id = UUID.randomUUID().toString()
        def cluster1 = createCluster(cluster1Id)
        def cluster2 = createCluster(cluster2Id)
        def clusters = Sets.newHashSet(cluster1, cluster2)
        ResourceSelectionResult<Cluster> clusterSelectionResult = Mock(ResourceSelectionResult) {
            getSelectorClass() >> this.getClass()
            getSelectionRationale() >> Optional.empty()
        }

        def commandId = UUID.randomUUID().toString()
        def executableBinary = UUID.randomUUID().toString()
        def executableArgument0 = UUID.randomUUID().toString()
        def executableArgument1 = UUID.randomUUID().toString()
        def executable = Lists.newArrayList(executableBinary, executableArgument0, executableArgument1)
        def arguments = Lists.newArrayList(UUID.randomUUID().toString())
        def command = createCommand(commandId, executable)

        def jobId = UUID.randomUUID().toString()
        def requestedArchiveLocationPrefix = UUID.randomUUID().toString()

        def expectedCommandArgs = executable
        def expectedJobArgs = arguments

        Map<Cluster, String> clusterCommandMap = ImmutableMap.of(cluster1, commandId, cluster2, commandId)
        def jobRequest = createJobRequest(arguments, requestedArchiveLocationPrefix, null, null)
        def jobRequestNoArchivalData = createJobRequest(arguments, null, null, 5_002)
        def requestedMemory = 6_323
        def savedJobRequest = createJobRequest(arguments, null, requestedMemory, null)

        when:
        def resolvedJob = this.service.resolveJob(jobId, jobRequest, true)
        def jobSpec = resolvedJob.getJobSpecification()
        def jobEnvironment = resolvedJob.getJobEnvironment()

        then:
        1 * this.environment.getProperty(
            JobResolverServiceImpl.V4_PROBABILITY_PROPERTY_KEY,
            Double.class,
            JobResolverServiceImpl.DEFAULT_V4_PROBABILITY
        ) >> 0.0d
        1 * this.environment.getProperty(JobResolverServiceImpl.DUAL_RESOLVE_PROPERTY_KEY, Boolean.class, false) >> false
        0 * this.persistenceService.findCommandsMatchingCriterion(_ as Criterion, _ as boolean)
        0 * this.persistenceService.saveResolvedJob(_ as String, _ as ResolvedJob)
        1 * this.persistenceService.findClustersAndCommandsForCriteria(
            jobRequest.getCriteria().getClusterCriteria(),
            jobRequest.getCriteria().getCommandCriterion()
        ) >> clusterCommandMap
        1 * this.clusterSelector.select(clusters, jobRequest, jobId) >> clusterSelectionResult
        1 * clusterSelectionResult.getSelectedResource() >> Optional.of(cluster1)
        1 * this.persistenceService.getCommand(commandId) >> command
        1 * this.persistenceService.getApplicationsForCommand(commandId) >> Lists.newArrayList()
        jobSpec.getExecutableArgs() == expectedCommandArgs
        jobSpec.getJobArgs() == expectedJobArgs
        jobSpec.getJob().getId() == jobId
        jobSpec.getCluster().getId() == cluster1Id
        jobSpec.getCommand().getId() == commandId
        jobSpec.getApplications().isEmpty()
        !jobSpec.isInteractive()
        jobSpec.getEnvironmentVariables().size() == 19
        jobSpec.getArchiveLocation() == Optional.of(requestedArchiveLocationPrefix + File.separator + jobId)
        jobEnvironment.getEnvironmentVariables() == jobSpec.getEnvironmentVariables()
        jobEnvironment.getMemory() == this.jobsProperties.getMemory().getDefaultJobMemory()
        jobEnvironment.getCpu() == 1
        !jobEnvironment.getExt().isPresent()
        jobSpec.getTimeout().orElse(null) == com.netflix.genie.common.dto.JobRequest.DEFAULT_TIMEOUT_DURATION

        when:
        def resolvedJobNoArchivalData = this.service.resolveJob(jobId, jobRequestNoArchivalData, true)
        def jobSpecNoArchivalData = resolvedJobNoArchivalData.getJobSpecification()
        def jobEnvironmentNoArchivalData = resolvedJobNoArchivalData.getJobEnvironment()

        then:
        0 * this.persistenceService.saveResolvedJob(_ as String, _ as ResolvedJob)
        1 * this.environment.getProperty(
            JobResolverServiceImpl.V4_PROBABILITY_PROPERTY_KEY,
            Double.class,
            JobResolverServiceImpl.DEFAULT_V4_PROBABILITY
        ) >> 0.0d
        1 * this.environment.getProperty(JobResolverServiceImpl.DUAL_RESOLVE_PROPERTY_KEY, Boolean.class, false) >> true
        1 * this.persistenceService.findCommandsMatchingCriterion(
            jobRequestNoArchivalData.getCriteria().getCommandCriterion(),
            true
        ) >> Sets.newHashSet(createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString())))
        1 * this.persistenceService.findClustersAndCommandsForCriteria(
            jobRequestNoArchivalData.getCriteria().getClusterCriteria(),
            jobRequestNoArchivalData.getCriteria().getCommandCriterion()
        ) >> clusterCommandMap
        1 * this.clusterSelector.select(clusters, jobRequestNoArchivalData, jobId) >> clusterSelectionResult
        1 * clusterSelectionResult.getSelectedResource() >> Optional.of(cluster1)
        1 * this.persistenceService.getCommand(commandId) >> command
        1 * this.persistenceService.getApplicationsForCommand(commandId) >> Lists.newArrayList()
        jobSpecNoArchivalData.getExecutableArgs() == expectedCommandArgs
        jobSpecNoArchivalData.getJobArgs() == expectedJobArgs
        jobSpecNoArchivalData.getJob().getId() == jobId
        jobSpecNoArchivalData.getCluster().getId() == cluster1Id
        jobSpecNoArchivalData.getCommand().getId() == commandId
        jobSpecNoArchivalData.getApplications().isEmpty()
        !jobSpecNoArchivalData.isInteractive()
        jobSpecNoArchivalData.getEnvironmentVariables().size() == 19
        jobSpecNoArchivalData.getArchiveLocation() ==
            Optional.of(
                StringUtils.endsWith(this.jobsProperties.getLocations().getArchives().toString(), File.separator)
                    ? this.jobsProperties.getLocations().getArchives().toString() + jobId
                    : this.jobsProperties.getLocations().getArchives().toString() + File.separator + jobId
            )
        jobEnvironmentNoArchivalData.getEnvironmentVariables() == jobSpecNoArchivalData.getEnvironmentVariables()
        jobEnvironmentNoArchivalData.getMemory() == this.jobsProperties.getMemory().getDefaultJobMemory()
        jobEnvironmentNoArchivalData.getCpu() == 1
        !jobEnvironmentNoArchivalData.getExt().isPresent()
        jobSpecNoArchivalData.getTimeout().orElse(null) == 5_002

        when: "We try to resolve a saved job"
        def resolvedSavedJobData = this.service.resolveJob(jobId)
        def jobSpecSavedData = resolvedSavedJobData.getJobSpecification()
        def jobEnvironmentSavedData = resolvedSavedJobData.getJobEnvironment()

        then: "the resolution is saved"
        1 * this.environment.getProperty(
            JobResolverServiceImpl.V4_PROBABILITY_PROPERTY_KEY,
            Double.class,
            JobResolverServiceImpl.DEFAULT_V4_PROBABILITY
        ) >> 0.0d
        1 * this.environment.getProperty(JobResolverServiceImpl.DUAL_RESOLVE_PROPERTY_KEY, Boolean.class, false) >> true
        1 * this.persistenceService.findCommandsMatchingCriterion(savedJobRequest.getCriteria().getCommandCriterion(), true) >> Sets.newHashSet(command)
        1 * this.persistenceService.getJobStatus(jobId) >> JobStatus.RESERVED
        1 * this.persistenceService.isApiJob(jobId) >> false
        1 * this.persistenceService.getJobRequest(jobId) >> savedJobRequest
        1 * this.persistenceService.findClustersAndCommandsForCriteria(
            savedJobRequest.getCriteria().getClusterCriteria(),
            savedJobRequest.getCriteria().getCommandCriterion()
        ) >> clusterCommandMap
        1 * this.clusterSelector.select(clusters, savedJobRequest, jobId) >> clusterSelectionResult
        1 * clusterSelectionResult.getSelectedResource() >> Optional.of(cluster1)
        1 * this.persistenceService.getCommand(commandId) >> command
        1 * this.persistenceService.getApplicationsForCommand(commandId) >> Lists.newArrayList()
        1 * this.persistenceService.saveResolvedJob(jobId, _ as ResolvedJob)
        jobSpecSavedData.getExecutableArgs() == expectedCommandArgs
        jobSpecSavedData.getJobArgs() == expectedJobArgs
        jobSpecSavedData.getJob().getId() == jobId
        jobSpecSavedData.getCluster().getId() == cluster1Id
        jobSpecSavedData.getCommand().getId() == commandId
        jobSpecSavedData.getApplications().isEmpty()
        !jobSpecSavedData.isInteractive()
        jobSpecSavedData.getEnvironmentVariables().size() == 19
        jobSpecSavedData.getArchiveLocation() ==
            Optional.of(
                StringUtils.endsWith(this.jobsProperties.getLocations().getArchives().toString(), File.separator)
                    ? this.jobsProperties.getLocations().getArchives().toString() + jobId
                    : this.jobsProperties.getLocations().getArchives().toString() + File.separator + jobId
            )
        jobEnvironmentSavedData.getEnvironmentVariables() == jobSpecSavedData.getEnvironmentVariables()
        jobEnvironmentSavedData.getMemory() == requestedMemory
        jobEnvironmentSavedData.getCpu() == 1
        !jobEnvironmentSavedData.getExt().isPresent()
        !jobSpecSavedData.getTimeout().isPresent()
    }

    def "Can resolve a job with V4 algorithm"() {
        def cluster1Id = UUID.randomUUID().toString()
        def cluster2Id = UUID.randomUUID().toString()
        def cluster1 = createCluster(cluster1Id)
        def cluster2 = createCluster(cluster2Id)
        def clusters = Sets.newHashSet(cluster1, cluster2)
        ResourceSelectionResult<Cluster> clusterSelectionResult = Mock(ResourceSelectionResult) {
            getSelectorClass() >> this.getClass()
            getSelectionRationale() >> Optional.empty()
        }

        def command0Id = UUID.randomUUID().toString()
        def command1Id = UUID.randomUUID().toString()
        def executableBinary = UUID.randomUUID().toString()
        def executableArgument0 = UUID.randomUUID().toString()
        def executableArgument1 = UUID.randomUUID().toString()
        def executable = Lists.newArrayList(executableBinary, executableArgument0, executableArgument1)
        def arguments = Lists.newArrayList(UUID.randomUUID().toString())
        def command0 = createCommand(command0Id, executable)
        def command1 = createCommand(command1Id, executable)
        def commands = Sets.newHashSet(command0, command1)
        ResourceSelectionResult<Command> commandSelectionResult = Mock(ResourceSelectionResult) {
            getSelectorClass() >> this.getClass()
            getSelectionRationale() >> Optional.empty()
        }

        def jobId = UUID.randomUUID().toString()
        def requestedArchiveLocationPrefix = UUID.randomUUID().toString()

        def expectedCommandArgs = executable
        def expectedJobArgs = arguments

        def jobRequest = createJobRequest(arguments, requestedArchiveLocationPrefix, null, null)
        def jobRequestNoArchivalData = createJobRequest(arguments, null, null, 5_002)
        def requestedMemory = 6_323
        def savedJobRequest = createJobRequest(arguments, null, requestedMemory, null)

        when: "An API job is called without archive information"
        def resolvedJob = this.service.resolveJob(jobId, jobRequest, true)
        def jobSpec = resolvedJob.getJobSpecification()
        def jobEnvironment = resolvedJob.getJobEnvironment()

        then: "It is resolved but not saved"
        1 * this.environment.getProperty(
            JobResolverServiceImpl.V4_PROBABILITY_PROPERTY_KEY,
            Double.class,
            JobResolverServiceImpl.DEFAULT_V4_PROBABILITY
        ) >> 1.0d
        0 * this.environment.getProperty(JobResolverServiceImpl.DUAL_RESOLVE_PROPERTY_KEY, Boolean.class, false) >> false
        0 * this.persistenceService.saveResolvedJob(_ as String, _ as ResolvedJob)
        1 * this.persistenceService.findCommandsMatchingCriterion(jobRequest.getCriteria().getCommandCriterion(), true) >> commands
        1 * this.commandSelector.select(commands, jobRequest, jobId) >> commandSelectionResult
        1 * commandSelectionResult.getSelectedResource() >> Optional.of(command0)
        1 * this.persistenceService.findClustersMatchingCriterion(_ as Criterion, true) >> clusters
        1 * this.clusterSelector.select(clusters, jobRequest, jobId) >> clusterSelectionResult
        1 * clusterSelectionResult.getSelectedResource() >> Optional.of(cluster1)
        1 * this.persistenceService.getApplicationsForCommand(command0Id) >> Lists.newArrayList()
        jobSpec.getExecutableArgs() == expectedCommandArgs
        jobSpec.getJobArgs() == expectedJobArgs
        jobSpec.getJob().getId() == jobId
        jobSpec.getCluster().getId() == cluster1Id
        jobSpec.getCommand().getId() == command0Id
        jobSpec.getApplications().isEmpty()
        !jobSpec.isInteractive()
        jobSpec.getEnvironmentVariables().size() == 19
        jobSpec.getArchiveLocation() == Optional.of(requestedArchiveLocationPrefix + File.separator + jobId)
        jobEnvironment.getEnvironmentVariables() == jobSpec.getEnvironmentVariables()
        jobEnvironment.getMemory() == this.jobsProperties.getMemory().getDefaultJobMemory()
        jobEnvironment.getCpu() == 1
        !jobEnvironment.getExt().isPresent()
        jobSpec.getTimeout().orElse(null) == com.netflix.genie.common.dto.JobRequest.DEFAULT_TIMEOUT_DURATION

        when: "When an API job is called with archive location"
        def resolvedJobNoArchivalData = this.service.resolveJob(jobId, jobRequestNoArchivalData, true)
        def jobSpecNoArchivalData = resolvedJobNoArchivalData.getJobSpecification()
        def jobEnvironmentNoArchivalData = resolvedJobNoArchivalData.getJobEnvironment()

        then: "It is resolved with archive information"
        0 * this.persistenceService.saveResolvedJob(_ as String, _ as ResolvedJob)
        1 * this.environment.getProperty(
            JobResolverServiceImpl.V4_PROBABILITY_PROPERTY_KEY,
            Double.class,
            JobResolverServiceImpl.DEFAULT_V4_PROBABILITY
        ) >> 1.0d
        0 * this.environment.getProperty(JobResolverServiceImpl.DUAL_RESOLVE_PROPERTY_KEY, Boolean.class, false) >> false
        1 * this.persistenceService.findCommandsMatchingCriterion(jobRequestNoArchivalData.getCriteria().getCommandCriterion(), true) >> commands
        1 * this.commandSelector.select(commands, jobRequestNoArchivalData, jobId) >> commandSelectionResult
        1 * commandSelectionResult.getSelectedResource() >> Optional.of(command0)
        1 * this.persistenceService.findClustersMatchingCriterion(_ as Criterion, true) >> clusters
        1 * this.clusterSelector.select(clusters, jobRequestNoArchivalData, jobId) >> clusterSelectionResult
        1 * clusterSelectionResult.getSelectedResource() >> Optional.of(cluster1)
        1 * this.persistenceService.getApplicationsForCommand(command0Id) >> Lists.newArrayList()
        jobSpecNoArchivalData.getExecutableArgs() == expectedCommandArgs
        jobSpecNoArchivalData.getJobArgs() == expectedJobArgs
        jobSpecNoArchivalData.getJob().getId() == jobId
        jobSpecNoArchivalData.getCluster().getId() == cluster1Id
        jobSpecNoArchivalData.getCommand().getId() == command0Id
        jobSpecNoArchivalData.getApplications().isEmpty()
        !jobSpecNoArchivalData.isInteractive()
        jobSpecNoArchivalData.getEnvironmentVariables().size() == 19
        jobSpecNoArchivalData.getArchiveLocation() ==
            Optional.of(
                StringUtils.endsWith(this.jobsProperties.getLocations().getArchives().toString(), File.separator)
                    ? this.jobsProperties.getLocations().getArchives().toString() + jobId
                    : this.jobsProperties.getLocations().getArchives().toString() + File.separator + jobId
            )
        jobEnvironmentNoArchivalData.getEnvironmentVariables() == jobSpecNoArchivalData.getEnvironmentVariables()
        jobEnvironmentNoArchivalData.getMemory() == this.jobsProperties.getMemory().getDefaultJobMemory()
        jobEnvironmentNoArchivalData.getCpu() == 1
        !jobEnvironmentNoArchivalData.getExt().isPresent()
        jobSpecNoArchivalData.getTimeout().orElse(null) == 5_002

        when: "We try to resolve a saved job"
        def resolvedSavedJobData = this.service.resolveJob(jobId)
        def jobSpecSavedData = resolvedSavedJobData.getJobSpecification()
        def jobEnvironmentSavedData = resolvedSavedJobData.getJobEnvironment()

        then: "the resolution is saved"
        1 * this.environment.getProperty(
            JobResolverServiceImpl.V4_PROBABILITY_PROPERTY_KEY,
            Double.class,
            JobResolverServiceImpl.DEFAULT_V4_PROBABILITY
        ) >> 1.0d
        0 * this.environment.getProperty(JobResolverServiceImpl.DUAL_RESOLVE_PROPERTY_KEY, Boolean.class, false) >> false
        1 * this.persistenceService.getJobStatus(jobId) >> JobStatus.RESERVED
        1 * this.persistenceService.isApiJob(jobId) >> false
        1 * this.persistenceService.getJobRequest(jobId) >> savedJobRequest
        1 * this.persistenceService.findCommandsMatchingCriterion(savedJobRequest.getCriteria().getCommandCriterion(), true) >> commands
        1 * this.commandSelector.select(commands, savedJobRequest, jobId) >> commandSelectionResult
        1 * commandSelectionResult.getSelectedResource() >> Optional.of(command1)
        1 * this.persistenceService.findClustersMatchingCriterion(_ as Criterion, true) >> clusters
        1 * this.clusterSelector.select(clusters, savedJobRequest, jobId) >> clusterSelectionResult
        1 * clusterSelectionResult.getSelectedResource() >> Optional.of(cluster2)
        1 * this.persistenceService.getApplicationsForCommand(command1Id) >> Lists.newArrayList()
        1 * this.persistenceService.saveResolvedJob(jobId, _ as ResolvedJob)
        jobSpecSavedData.getExecutableArgs() == expectedCommandArgs
        jobSpecSavedData.getJobArgs() == expectedJobArgs
        jobSpecSavedData.getJob().getId() == jobId
        jobSpecSavedData.getCluster().getId() == cluster2Id
        jobSpecSavedData.getCommand().getId() == command1Id
        jobSpecSavedData.getApplications().isEmpty()
        !jobSpecSavedData.isInteractive()
        jobSpecSavedData.getEnvironmentVariables().size() == 19
        jobSpecSavedData.getArchiveLocation() ==
            Optional.of(
                StringUtils.endsWith(this.jobsProperties.getLocations().getArchives().toString(), File.separator)
                    ? this.jobsProperties.getLocations().getArchives().toString() + jobId
                    : this.jobsProperties.getLocations().getArchives().toString() + File.separator + jobId
            )
        jobEnvironmentSavedData.getEnvironmentVariables() == jobSpecSavedData.getEnvironmentVariables()
        jobEnvironmentSavedData.getMemory() == requestedMemory
        jobEnvironmentSavedData.getCpu() == 1
        !jobEnvironmentSavedData.getExt().isPresent()
        !jobSpecSavedData.getTimeout().isPresent()
    }

    def "Can convert tags to string"(Set<String> input, String output) {
        expect:
        this.service.tagsToString(input) == output

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
        def user = UUID.randomUUID().toString()
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
                .Builder(jobName, user)
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
            100L,
            null
        )

        when:
        def envVariables = this.service.generateEnvironmentVariables(
            jobId,
            jobRequest,
            cluster,
            command,
            this.jobsProperties.getMemory().getDefaultJobMemory()
        )

        then:
        envVariables.get("GENIE_VERSION") == "4"
        envVariables.get(JobConstants.GENIE_CLUSTER_ID_ENV_VAR) == clusterId
        envVariables.get(JobConstants.GENIE_CLUSTER_NAME_ENV_VAR) == clusterName
        envVariables.get(JobConstants.GENIE_CLUSTER_TAGS_ENV_VAR) == this.service.tagsToString(clusterTags)
        envVariables.get(JobConstants.GENIE_COMMAND_ID_ENV_VAR) == commandId
        envVariables.get(JobConstants.GENIE_COMMAND_NAME_ENV_VAR) == commandName
        envVariables.get(JobConstants.GENIE_COMMAND_TAGS_ENV_VAR) == this.service.tagsToString(commandTags)
        envVariables.get(JobConstants.GENIE_JOB_ID_ENV_VAR) == jobId
        envVariables.get(JobConstants.GENIE_JOB_NAME_ENV_VAR) == jobName
        envVariables.get(JobConstants.GENIE_JOB_MEMORY_ENV_VAR) == String.valueOf(this.jobsProperties.getMemory().getDefaultJobMemory())
        envVariables.get(JobConstants.GENIE_REQUESTED_COMMAND_TAGS_ENV_VAR) == this.service.tagsToString(commandCriterion.getTags())
        envVariables.get(JobConstants.GENIE_REQUESTED_CLUSTER_TAGS_ENV_VAR + "_0") == this.service.tagsToString(clusterCriteria.get(0).getTags())
        envVariables.get(JobConstants.GENIE_REQUESTED_CLUSTER_TAGS_ENV_VAR + "_1") == this.service.tagsToString(clusterCriteria.get(1).getTags())
        envVariables.get(JobConstants.GENIE_REQUESTED_CLUSTER_TAGS_ENV_VAR) == "[[" + this.service.tagsToString(clusterCriteria.get(0).getTags()) + "],[" + this.service.tagsToString(clusterCriteria.get(1).getTags()) + "]]"
        envVariables.get(JobConstants.GENIE_JOB_TAGS_ENV_VAR) == this.service.tagsToString(jobRequest.getMetadata().getTags())
        envVariables.get(JobConstants.GENIE_JOB_GROUPING_ENV_VAR) == grouping
        envVariables.get(JobConstants.GENIE_JOB_GROUPING_INSTANCE_ENV_VAR) == groupingInstance
        envVariables.get(JobConstants.GENIE_USER_ENV_VAR) == user
        envVariables.get(JobConstants.GENIE_USER_GROUP_ENV_VAR) == ""
    }

    def "can merge criterion strings: #one #two with expected result #expected"() {
        expect:
        expected == this.service.mergeCriteriaStrings(one, two, UUID.randomUUID().toString())

        where:
        one   | two   | expected
        "one" | "one" | "one"
        "one" | null  | "one"
        null  | "two" | "two"
        null  | null  | null
    }

    def "different non-null strings throw expected exception for criterion merge"() {
        when:
        this.service.mergeCriteriaStrings(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )

        then:
        thrown(IllegalArgumentException)
    }

    def "can merge criteria"() {
        def criterion0 = new Criterion.Builder().withId(UUID.randomUUID().toString()).build()
        def criterion1 = new Criterion.Builder().withName(UUID.randomUUID().toString()).build()
        def criterion2 = new Criterion.Builder().withStatus(UUID.randomUUID().toString()).build()
        def criterion3 = new Criterion.Builder().withVersion(UUID.randomUUID().toString()).build()
        def criterion4 = new Criterion.Builder().withTags(Sets.newHashSet(UUID.randomUUID().toString())).build()
        def criterion5 = new Criterion.Builder().withTags(
            Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        ).build()
        def criterion6 = new Criterion.Builder().withId(UUID.randomUUID().toString()).build()

        when:
        def mergedCriterion = this.service.mergeCriteria(criterion0, criterion1)

        then:
        mergedCriterion.getId() == criterion0.getId()
        mergedCriterion.getName() == criterion1.getName()
        !mergedCriterion.getStatus().isPresent()
        !mergedCriterion.getVersion().isPresent()
        mergedCriterion.getTags().isEmpty()

        when:
        mergedCriterion = this.service.mergeCriteria(mergedCriterion, criterion2)

        then:
        mergedCriterion.getId() == criterion0.getId()
        mergedCriterion.getName() == criterion1.getName()
        mergedCriterion.getStatus() == criterion2.getStatus()
        !mergedCriterion.getVersion().isPresent()
        mergedCriterion.getTags().isEmpty()

        when:
        mergedCriterion = this.service.mergeCriteria(mergedCriterion, criterion3)

        then:
        mergedCriterion.getId() == criterion0.getId()
        mergedCriterion.getName() == criterion1.getName()
        mergedCriterion.getStatus() == criterion2.getStatus()
        mergedCriterion.getVersion() == criterion3.getVersion()
        mergedCriterion.getTags().isEmpty()

        when:
        mergedCriterion = this.service.mergeCriteria(mergedCriterion, criterion4)

        then:
        mergedCriterion.getId() == criterion0.getId()
        mergedCriterion.getName() == criterion1.getName()
        mergedCriterion.getStatus() == criterion2.getStatus()
        mergedCriterion.getVersion() == criterion3.getVersion()
        mergedCriterion.getTags() == criterion4.getTags()

        when:
        mergedCriterion = this.service.mergeCriteria(mergedCriterion, criterion5)

        then:
        mergedCriterion.getId() == criterion0.getId()
        mergedCriterion.getName() == criterion1.getName()
        mergedCriterion.getStatus() == criterion2.getStatus()
        mergedCriterion.getVersion() == criterion3.getVersion()
        mergedCriterion.getTags().containsAll(criterion4.getTags())
        mergedCriterion.getTags().containsAll(criterion5.getTags())

        when:
        def mergedCriterion2 = this.service.mergeCriteria(mergedCriterion, mergedCriterion)

        then:
        mergedCriterion2 == mergedCriterion

        when:
        this.service.mergeCriteria(criterion0, criterion6)

        then:
        thrown(IllegalArgumentException)
    }

    def "can resolve command"() {
        def jobRequest = createJobRequest(Lists.newArrayList(UUID.randomUUID().toString()), null, null, null)
        def jobId = UUID.randomUUID().toString()
        def command0 = createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))
        def command1 = createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))
        def command2 = createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))
        def allCommands = Sets.newHashSet(command0, command1, command2)
        def commandCriterion = jobRequest.getCriteria().getCommandCriterion()
        ResourceSelectionResult<Command> selectionResult = Mock(ResourceSelectionResult)

        when: "No commands are found in the database which match the users criterion"
        this.service.resolveCommand(jobRequest, jobId)

        then: "An exception is thrown"
        1 * this.persistenceService.findCommandsMatchingCriterion(commandCriterion, true) >> Sets.newHashSet()
        0 * this.commandSelector.select(_ as Set<Command>, _ as JobRequest, _ as String)
        thrown(GenieJobResolutionException)

        when: "Only a single command is found which matches the criterion"
        def resolvedCommand = this.service.resolveCommand(jobRequest, jobId)

        then: "It is immediately returned and no selectors are invoked"
        1 * this.persistenceService.findCommandsMatchingCriterion(commandCriterion, true) >> Sets.newHashSet(command1)
        0 * this.commandSelector.select(_ as Set<Command>, _ as JobRequest, _ as String)
        resolvedCommand == command1

        when: "Many commands are found which match the criterion but nothing is selected by the selectors"
        this.service.resolveCommand(jobRequest, jobId)

        then: "An exception is thrown"
        1 * this.persistenceService.findCommandsMatchingCriterion(commandCriterion, true) >> allCommands
        1 * this.commandSelector.select(allCommands, jobRequest, jobId) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.empty()
        1 * selectionResult.getSelectorClass() >> this.getClass()
        1 * selectionResult.getSelectionRationale() >> Optional.empty()
        thrown(GenieJobResolutionException)

        when: "The selectors throw an exception"
        this.service.resolveCommand(jobRequest, jobId)

        then: "It is propagated"
        1 * this.persistenceService.findCommandsMatchingCriterion(commandCriterion, true) >> allCommands
        1 * this.commandSelector.select(allCommands, jobRequest, jobId) >> { throw new ResourceSelectionException() }
        thrown(GenieJobResolutionException)

        when: "The selectors select a command"
        resolvedCommand = this.service.resolveCommand(jobRequest, jobId)

        then: "It is returned"
        1 * this.persistenceService.findCommandsMatchingCriterion(commandCriterion, true) >> allCommands
        1 * this.commandSelector.select(allCommands, jobRequest, jobId) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.of(command0)
        1 * selectionResult.getSelectorClass() >> this.getClass()
        1 * selectionResult.getSelectionRationale() >> Optional.of("Selected command 0")
        resolvedCommand == command0
    }

    def "Can resolve cluster"() {
        def command = Mock(Command)
        def cluster0 = createCluster(UUID.randomUUID().toString())
        def cluster1 = createCluster(UUID.randomUUID().toString())
        def cluster2 = createCluster(UUID.randomUUID().toString())
        def allClusters = Sets.newHashSet(cluster0, cluster1, cluster2)
        def jobId = UUID.randomUUID().toString()
        def jobRequestTemplate = createJobRequest(Lists.newArrayList(UUID.randomUUID().toString()), null, null, null)
        def commandClusterCriteria = [
            new Criterion.Builder()
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build(),
            new Criterion.Builder()
                .withId(cluster0.getId())
                .build()
        ]
        def jobClusterCriteria = [
            new Criterion.Builder()
                .withId(cluster1.getId())
                .build(),
            new Criterion.Builder()
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build()
        ]
        // Note: commandClusterCriteria[1] and jobClusterCriteria[0] throws exception
        def mergedCriterion0 = this.service.mergeCriteria(commandClusterCriteria[0], jobClusterCriteria[0])
        def mergedCriterion1 = this.service.mergeCriteria(commandClusterCriteria[0], jobClusterCriteria[1])
        def mergedCriterion2 = this.service.mergeCriteria(commandClusterCriteria[1], jobClusterCriteria[1])
        def jobRequest = new JobRequest(
            jobId,
            jobRequestTemplate.getResources(),
            jobRequestTemplate.getCommandArgs(),
            jobRequestTemplate.getMetadata(),
            new ExecutionResourceCriteria(
                jobClusterCriteria,
                new Criterion.Builder().withId(UUID.randomUUID().toString()).build(),
                null
            ),
            jobRequestTemplate.getRequestedJobEnvironment(),
            jobRequestTemplate.getRequestedAgentConfig(),
            jobRequestTemplate.getRequestedJobArchivalData()
        )
        ResourceSelectionResult<Cluster> selectionResult = Mock(ResourceSelectionResult)

        when: "A command with no cluster criteria was resolved"
        this.service.resolveCluster(command, jobRequest, jobId)

        then: "An exception is thrown cause nothing can be selected"
        1 * command.getClusterCriteria() >> []
        0 * this.persistenceService.findClustersMatchingCriterion(_ as Criterion, true)
        thrown(GenieJobResolutionException)

        when: "No clusters are found which match the criterion"
        this.service.resolveCluster(command, jobRequest, jobId)

        then: "An exception is thrown"
        1 * command.getClusterCriteria() >> commandClusterCriteria
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion0, true) >> Sets.newHashSet()
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion1, true) >> Sets.newHashSet()
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion2, true) >> Sets.newHashSet()
        thrown(GenieJobResolutionException)

        when: "Only a single cluster is found"
        def resolvedCluster = this.service.resolveCluster(command, jobRequest, jobId)

        then: "It is returned"
        1 * command.getClusterCriteria() >> commandClusterCriteria
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion0, true) >> Sets.newHashSet()
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion1, true) >> Sets.newHashSet(cluster2)
        0 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion2, true)
        resolvedCluster == cluster2

        when: "Multiple clusters are found matching the criterion and the selectors don't select anything"
        this.service.resolveCluster(command, jobRequest, jobId)

        then: "An exception is thrown"
        1 * command.getClusterCriteria() >> commandClusterCriteria
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion0, true) >> Sets.newHashSet()
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion1, true) >> Sets.newHashSet()
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion2, true) >> allClusters
        1 * this.clusterSelector.select(allClusters, jobRequest, jobId) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.empty()
        thrown(GenieJobResolutionException)

        when: "Multiple clusters are found matching the criterion and the selectors throw exception"
        this.service.resolveCluster(command, jobRequest, jobId)

        then: "An exception is thrown"
        1 * command.getClusterCriteria() >> commandClusterCriteria
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion0, true) >> Sets.newHashSet()
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion1, true) >> Sets.newHashSet()
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion2, true) >> allClusters
        1 * this.clusterSelector.select(allClusters, jobRequest, jobId) >> { throw new ResourceSelectionException() }
        0 * selectionResult.getSelectedResource()
        thrown(GenieJobResolutionException)

        when: "Multiple clusters are found matching the criterion and the selectors select a cluster"
        resolvedCluster = this.service.resolveCluster(command, jobRequest, jobId)

        then: "That cluster is returned to the caller"
        1 * command.getClusterCriteria() >> commandClusterCriteria
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion0, true) >> Sets.newHashSet()
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion1, true) >> Sets.newHashSet()
        1 * this.persistenceService.findClustersMatchingCriterion(mergedCriterion2, true) >> allClusters
        1 * this.clusterSelector.select(allClusters, jobRequest, jobId) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.of(cluster2)
        resolvedCluster == cluster2
    }

    def "can select proper resolution algorithm"() {
        when: "An invalid property value is provided"
        def useV4 = this.service.useV4ResourceSelection()

        then: "V3 is used"
        1 * this.environment.getProperty(
            JobResolverServiceImpl.V4_PROBABILITY_PROPERTY_KEY,
            Double.class,
            JobResolverServiceImpl.DEFAULT_V4_PROBABILITY
        ) >> { throw new IllegalStateException() }
        !useV4

        when: "A value over 1.0 is provided"
        useV4 = this.service.useV4ResourceSelection()

        then: "It is reset to 1.0 and V4 algorithm is used"
        1 * this.environment.getProperty(
            JobResolverServiceImpl.V4_PROBABILITY_PROPERTY_KEY,
            Double.class,
            JobResolverServiceImpl.DEFAULT_V4_PROBABILITY
        ) >> JobResolverServiceImpl.MAX_V4_PROBABILITY + 0.1
        useV4

        when: "A value under 0.0 is provided"
        useV4 = this.service.useV4ResourceSelection()

        then: "It is reset to 0.0 and V3 algorithm is used"
        1 * this.environment.getProperty(
            JobResolverServiceImpl.V4_PROBABILITY_PROPERTY_KEY,
            Double.class,
            JobResolverServiceImpl.DEFAULT_V4_PROBABILITY
        ) >> JobResolverServiceImpl.MIN_V4_PROBABILITY - 0.1
        !useV4
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
            100L,
            Lists.newArrayList(
                new Criterion.Builder().withTags(Sets.newHashSet(UUID.randomUUID().toString())).build(),
                new Criterion.Builder().withTags(Sets.newHashSet(UUID.randomUUID().toString())).build()
            )
        )
    }

    private static JobRequest createJobRequest(
        List<String> commandArgs,
        @Nullable String requestedArchiveLocationPrefix,
        @Nullable Integer requestedMemory,
        @Nullable Integer requestedTimeout
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
            requestedTimeout == null
                ? null
                : new AgentConfigRequest.Builder().withTimeoutRequested(requestedTimeout).build(),
            requestedArchiveLocationPrefix == null
                ? null
                : new JobArchivalDataRequest.Builder()
                .withRequestedArchiveLocationPrefix(requestedArchiveLocationPrefix)
                .build()
        )
    }
}
