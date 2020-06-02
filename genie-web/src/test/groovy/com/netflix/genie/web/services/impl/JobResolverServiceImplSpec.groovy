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
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.common.external.dtos.v4.AgentConfigRequest
import com.netflix.genie.common.external.dtos.v4.Application
import com.netflix.genie.common.external.dtos.v4.ApplicationMetadata
import com.netflix.genie.common.external.dtos.v4.ApplicationStatus
import com.netflix.genie.common.external.dtos.v4.Cluster
import com.netflix.genie.common.external.dtos.v4.ClusterMetadata
import com.netflix.genie.common.external.dtos.v4.ClusterStatus
import com.netflix.genie.common.external.dtos.v4.Command
import com.netflix.genie.common.external.dtos.v4.CommandMetadata
import com.netflix.genie.common.external.dtos.v4.CommandStatus
import com.netflix.genie.common.external.dtos.v4.Criterion
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment
import com.netflix.genie.common.external.dtos.v4.ExecutionResourceCriteria
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
import com.netflix.genie.web.selectors.ClusterSelectionContext
import com.netflix.genie.web.selectors.ClusterSelector
import com.netflix.genie.web.selectors.CommandSelectionContext
import com.netflix.genie.web.selectors.CommandSelector
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.commons.lang3.StringUtils
import org.springframework.core.env.Environment
import spock.lang.Specification

import javax.annotation.Nullable
import java.time.Instant
import java.util.stream.Collectors
import java.util.stream.Stream
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

        def expectedCommandArgs = executable
        def expectedJobArgs = arguments

        Map<Cluster, String> clusterCommandMap = ImmutableMap.of(cluster1, commandId, cluster2, commandId)
        def jobRequest = createJobRequest(arguments, null, null)
        def jobRequestNoArchivalData = createJobRequest(arguments, null, 5_002)
        def requestedMemory = 6_323
        def savedJobRequest = createJobRequest(arguments, requestedMemory, null)

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
        1 * this.clusterSelector.select(
            {
                verifyAll(it, ClusterSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == jobRequest
                    it.getResources() == clusters
                }
            }
        ) >> clusterSelectionResult
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
        jobSpec.getArchiveLocation() ==
            Optional.of(
                StringUtils.endsWith(this.jobsProperties.getLocations().getArchives().toString(), File.separator)
                    ? this.jobsProperties.getLocations().getArchives().toString() + jobId
                    : this.jobsProperties.getLocations().getArchives().toString() + File.separator + jobId
            )
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
        1 * this.clusterSelector.select(
            {
                verifyAll(it, ClusterSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == jobRequestNoArchivalData
                    it.getResources() == clusters
                }
            }
        ) >> clusterSelectionResult
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
        1 * this.clusterSelector.select(
            {
                verifyAll(it, ClusterSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == savedJobRequest
                    it.getResources() == clusters
                }
            }
        ) >> clusterSelectionResult
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

        def expectedCommandArgs = executable
        def expectedJobArgs = arguments

        def jobRequest0 = createJobRequest(arguments, null, null)
        def jobRequest1 = createJobRequest(arguments, null, 5_002)
        def requestedMemory = 6_323
        def jobRequest2 = createJobRequest(arguments, requestedMemory, null)

        def command0JobRequest0Clusters = createClustersBasedOnCriteria(2, command0, jobRequest0)
        def command1JobRequest0Clusters = createClustersBasedOnCriteria(3, command1, jobRequest0)
        def jobRequest0Clusters = Sets.newHashSet(command0JobRequest0Clusters)
        jobRequest0Clusters.addAll(command1JobRequest0Clusters)
        def jobRequest0SelectedCluster = command0JobRequest0Clusters.head()

        def command0JobRequest1Clusters = createClustersBasedOnCriteria(1, command0, jobRequest1)
        def command1JobRequest1Clusters = createClustersBasedOnCriteria(4, command1, jobRequest1)
        def jobRequest1Clusters = Sets.newHashSet(command0JobRequest1Clusters)
        jobRequest1Clusters.addAll(command1JobRequest1Clusters)
        def jobRequest1SelectedCluster = command1JobRequest1Clusters.head()

        def command0JobRequest2Clusters = createClustersBasedOnCriteria(4, command0, jobRequest2)
        def command1JobRequest2Clusters = createClustersBasedOnCriteria(2, command1, jobRequest2)
        def jobRequest2Clusters = Sets.newHashSet(command0JobRequest2Clusters)
        jobRequest2Clusters.addAll(command1JobRequest2Clusters)
        def jobRequest2SelectedCluster = command1JobRequest2Clusters.head()

        ResourceSelectionResult<Cluster> clusterSelectionResult = Mock(ResourceSelectionResult) {
            getSelectorClass() >> this.getClass()
            getSelectionRationale() >> Optional.empty()
        }

        when: "A non-persisting api is called"
        def resolvedJob = this.service.resolveJob(jobId, jobRequest0, true)
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
        1 * this.persistenceService.findCommandsMatchingCriterion(jobRequest0.getCriteria().getCommandCriterion(), true) >> commands
        1 * this.persistenceService.findClustersMatchingAnyCriterion(_ as Set<Criterion>, true) >> jobRequest0Clusters
        1 * this.commandSelector.select(_ as CommandSelectionContext) >> commandSelectionResult
        1 * commandSelectionResult.getSelectedResource() >> Optional.of(command0)
        1 * this.clusterSelector.select(
            {
                verifyAll(it, ClusterSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == jobRequest0
                    it.getResources() == command0JobRequest0Clusters
                }
            }
        ) >> clusterSelectionResult
        1 * clusterSelectionResult.getSelectedResource() >> Optional.of(jobRequest0SelectedCluster)
        1 * this.persistenceService.getApplicationsForCommand(command0Id) >> Lists.newArrayList()
        jobSpec.getExecutableArgs() == expectedCommandArgs
        jobSpec.getJobArgs() == expectedJobArgs
        jobSpec.getJob().getId() == jobId
        jobSpec.getCluster().getId() == jobRequest0SelectedCluster.getId()
        jobSpec.getCommand().getId() == command0Id
        jobSpec.getApplications().isEmpty()
        !jobSpec.isInteractive()
        jobSpec.getEnvironmentVariables().size() == 19
        jobSpec.getArchiveLocation() ==
            Optional.of(
                StringUtils.endsWith(this.jobsProperties.getLocations().getArchives().toString(), File.separator)
                    ? this.jobsProperties.getLocations().getArchives().toString() + jobId
                    : this.jobsProperties.getLocations().getArchives().toString() + File.separator + jobId
            )
        jobEnvironment.getEnvironmentVariables() == jobSpec.getEnvironmentVariables()
        jobEnvironment.getMemory() == this.jobsProperties.getMemory().getDefaultJobMemory()
        jobEnvironment.getCpu() == 1
        !jobEnvironment.getExt().isPresent()
        jobSpec.getTimeout().orElse(null) == com.netflix.genie.common.dto.JobRequest.DEFAULT_TIMEOUT_DURATION

        when: "Job request with archive data is submitted to non-persisting API"
        resolvedJob = this.service.resolveJob(jobId, jobRequest1, true)
        jobSpec = resolvedJob.getJobSpecification()
        jobEnvironment = resolvedJob.getJobEnvironment()

        then: "It is resolved with archive information but not saved"
        0 * this.persistenceService.saveResolvedJob(_ as String, _ as ResolvedJob)
        1 * this.environment.getProperty(
            JobResolverServiceImpl.V4_PROBABILITY_PROPERTY_KEY,
            Double.class,
            JobResolverServiceImpl.DEFAULT_V4_PROBABILITY
        ) >> 1.0d
        0 * this.environment.getProperty(JobResolverServiceImpl.DUAL_RESOLVE_PROPERTY_KEY, Boolean.class, false) >> false
        1 * this.persistenceService.findCommandsMatchingCriterion(jobRequest1.getCriteria().getCommandCriterion(), true) >> commands
        1 * this.persistenceService.findClustersMatchingAnyCriterion(_ as Set<Criterion>, true) >> jobRequest1Clusters
        1 * this.commandSelector.select(_ as CommandSelectionContext) >> commandSelectionResult
        1 * commandSelectionResult.getSelectedResource() >> Optional.of(command1)
        1 * this.clusterSelector.select(
            {
                verifyAll(it, ClusterSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == jobRequest1
                    it.getCommand().orElse(null) == command1
                    it.getResources() == command1JobRequest1Clusters
                }
            }
        ) >> clusterSelectionResult
        1 * clusterSelectionResult.getSelectedResource() >> Optional.of(jobRequest1SelectedCluster)
        1 * this.persistenceService.getApplicationsForCommand(command1Id) >> Lists.newArrayList()
        jobSpec.getExecutableArgs() == expectedCommandArgs
        jobSpec.getJobArgs() == expectedJobArgs
        jobSpec.getJob().getId() == jobId
        jobSpec.getCluster().getId() == jobRequest1SelectedCluster.getId()
        jobSpec.getCommand().getId() == command1Id
        jobSpec.getApplications().isEmpty()
        !jobSpec.isInteractive()
        jobSpec.getEnvironmentVariables().size() == 19
        jobSpec.getArchiveLocation() ==
            Optional.of(
                StringUtils.endsWith(this.jobsProperties.getLocations().getArchives().toString(), File.separator)
                    ? this.jobsProperties.getLocations().getArchives().toString() + jobId
                    : this.jobsProperties.getLocations().getArchives().toString() + File.separator + jobId
            )
        jobEnvironment.getEnvironmentVariables() == jobSpec.getEnvironmentVariables()
        jobEnvironment.getMemory() == this.jobsProperties.getMemory().getDefaultJobMemory()
        jobEnvironment.getCpu() == 1
        !jobEnvironment.getExt().isPresent()
        jobSpec.getTimeout().orElse(null) == 5_002

        when: "Request to resolve a saved job"
        resolvedJob = this.service.resolveJob(jobId)
        jobSpec = resolvedJob.getJobSpecification()
        jobEnvironment = resolvedJob.getJobEnvironment()

        then: "The job is resolved and the resolution is saved"
        1 * this.environment.getProperty(
            JobResolverServiceImpl.V4_PROBABILITY_PROPERTY_KEY,
            Double.class,
            JobResolverServiceImpl.DEFAULT_V4_PROBABILITY
        ) >> 1.0d
        0 * this.environment.getProperty(JobResolverServiceImpl.DUAL_RESOLVE_PROPERTY_KEY, Boolean.class, false) >> false
        1 * this.persistenceService.getJobStatus(jobId) >> JobStatus.RESERVED
        1 * this.persistenceService.isApiJob(jobId) >> false
        1 * this.persistenceService.getJobRequest(jobId) >> jobRequest2
        1 * this.persistenceService.findCommandsMatchingCriterion(jobRequest2.getCriteria().getCommandCriterion(), true) >> commands
        1 * this.persistenceService.findClustersMatchingAnyCriterion(_ as Set<Criterion>, true) >> jobRequest2Clusters
        1 * this.commandSelector.select(_ as CommandSelectionContext) >> commandSelectionResult
        1 * commandSelectionResult.getSelectedResource() >> Optional.of(command1)
        1 * this.clusterSelector.select(
            {
                verifyAll(it, ClusterSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == jobRequest2
                    it.getCommand().orElse(null) == command1
                    it.getResources() == command1JobRequest2Clusters
                }
            }
        ) >> clusterSelectionResult
        1 * clusterSelectionResult.getSelectedResource() >> Optional.of(jobRequest2SelectedCluster)
        1 * this.persistenceService.getApplicationsForCommand(command1Id) >> Lists.newArrayList()
        1 * this.persistenceService.saveResolvedJob(jobId, _ as ResolvedJob)
        jobSpec.getExecutableArgs() == expectedCommandArgs
        jobSpec.getJobArgs() == expectedJobArgs
        jobSpec.getJob().getId() == jobId
        jobSpec.getCluster().getId() == jobRequest2SelectedCluster.getId()
        jobSpec.getCommand().getId() == command1Id
        jobSpec.getApplications().isEmpty()
        !jobSpec.isInteractive()
        jobSpec.getEnvironmentVariables().size() == 19
        jobSpec.getArchiveLocation() ==
            Optional.of(
                StringUtils.endsWith(this.jobsProperties.getLocations().getArchives().toString(), File.separator)
                    ? this.jobsProperties.getLocations().getArchives().toString() + jobId
                    : this.jobsProperties.getLocations().getArchives().toString() + File.separator + jobId
            )
        jobEnvironment.getEnvironmentVariables() == jobSpec.getEnvironmentVariables()
        jobEnvironment.getMemory() == requestedMemory
        jobEnvironment.getCpu() == 1
        !jobEnvironment.getExt().isPresent()
        !jobSpec.getTimeout().isPresent()
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

    def "Can resolve correct environment variables"() {
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
        def context = new JobResolverServiceImpl.JobResolutionContext(jobId, jobRequest, true)
        context.setCluster(cluster)
        context.setCommand(command)
        context.setJobMemory(this.jobsProperties.getMemory().getDefaultJobMemory())

        when:
        this.service.resolveEnvironmentVariables(context)
        def envVariables = context.getEnvironmentVariables().orElseThrow({ new IllegalStateException() })

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
        def jobRequest = createJobRequest(Lists.newArrayList(UUID.randomUUID().toString()), null, null)
        def jobId = UUID.randomUUID().toString()
        def command0 = createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))
        def command1 = createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))
        def command1Set = Sets.newHashSet(command1)
        def command2 = createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))
        def allCommands = Sets.newHashSet(command0, command1, command2)
        def commandCriterion = jobRequest.getCriteria().getCommandCriterion()
        ResourceSelectionResult<Command> selectionResult = Mock(ResourceSelectionResult)
        def command1UniqueCriteria = this.service.flattenClusterCriteriaPermutations(
            this.service.generateClusterCriteriaPermutations(
                command1Set,
                jobRequest
            )
        )
        def command0Clusters = createClustersBasedOnCriteria(2, command0, jobRequest)
        def command1Clusters = createClustersBasedOnCriteria(1, command1, jobRequest)
        def command2Clusters = createClustersBasedOnCriteria(3, command2, jobRequest)

        def allCommandsUniqueCriteria = this.service.flattenClusterCriteriaPermutations(
            this.service.generateClusterCriteriaPermutations(
                allCommands,
                jobRequest
            )
        )
        def allClusters = Sets.newHashSet()
        allClusters.addAll(command0Clusters)
        allClusters.addAll(command1Clusters)
        allClusters.addAll(command2Clusters)

        when: "No commands are found in the database which match the users criterion"
        def context = new JobResolverServiceImpl.JobResolutionContext(jobId, jobRequest, true)
        this.service.resolveCommand(context)

        then: "An exception is thrown"
        1 * this.persistenceService.findCommandsMatchingCriterion(commandCriterion, true) >> Sets.newHashSet()
        0 * this.persistenceService.findClustersMatchingAnyCriterion(_ as Set<Cluster>, _ as boolean)
        0 * this.commandSelector.select(_ as CommandSelectionContext)
        thrown(GenieJobResolutionException)

        when: "Only a single command is found which matches the criterion but it has no clusters"
        context = new JobResolverServiceImpl.JobResolutionContext(jobId, jobRequest, false)
        this.service.resolveCommand(context)

        then: "The selector is not invoked as no command is selected"
        1 * this.persistenceService.findCommandsMatchingCriterion(commandCriterion, true) >> command1Set
        1 * this.persistenceService.findClustersMatchingAnyCriterion(command1UniqueCriteria, true) >> Sets.newHashSet()
        0 * this.commandSelector.select(_ as CommandSelectionContext)
        thrown(GenieJobResolutionException)

        when: "Only a single command is found but it is filtered out by in memory cluster matching"
        context = new JobResolverServiceImpl.JobResolutionContext(jobId, jobRequest, true)
        this.service.resolveCommand(context)

        then: "The selector is not invoked as no command is selected"
        1 * this.persistenceService.findCommandsMatchingCriterion(commandCriterion, true) >> command1Set
        1 * this.persistenceService.findClustersMatchingAnyCriterion(command1UniqueCriteria, true) >> Sets.newHashSet(
            createCluster(UUID.randomUUID().toString()) // this cluster will never match
        )
        0 * this.commandSelector.select(_ as CommandSelectionContext)
        thrown(GenieJobResolutionException)

        when: "Only a single command is found"
        context = new JobResolverServiceImpl.JobResolutionContext(jobId, jobRequest, true)
        this.service.resolveCommand(context)
        def resolvedCommand = context.getCommand().orElseThrow({ new IllegalStateException() })

        then: "The selector is invoked"
        1 * this.persistenceService.findCommandsMatchingCriterion(commandCriterion, true) >> command1Set
        1 * this.persistenceService.findClustersMatchingAnyCriterion(command1UniqueCriteria, true) >> command1Clusters
        1 * this.commandSelector.select(
            {
                verifyAll(it, CommandSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == jobRequest
                    it.getResources() == command1Set
                    it.getCommandToClusters() == [
                        (command1): command1Clusters
                    ]
                }
            }
        ) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.of(command1)
        1 * selectionResult.getSelectorClass() >> this.getClass()
        1 * selectionResult.getSelectionRationale() >> Optional.empty()
        resolvedCommand == command1

        when: "Many commands are found which match the criterion but nothing is selected by the selectors"
        context = new JobResolverServiceImpl.JobResolutionContext(jobId, jobRequest, false)
        this.service.resolveCommand(context)

        then: "An exception is thrown"
        1 * this.persistenceService.findCommandsMatchingCriterion(commandCriterion, true) >> allCommands
        1 * this.persistenceService.findClustersMatchingAnyCriterion(allCommandsUniqueCriteria, true) >> allClusters
        1 * this.commandSelector.select(
            {
                verifyAll(it, CommandSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == jobRequest
                    it.getResources() == allCommands
                    it.getCommandToClusters() == [
                        (command0): command0Clusters,
                        (command1): command1Clusters,
                        (command2): command2Clusters
                    ]
                }
            }
        ) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.empty()
        1 * selectionResult.getSelectorClass() >> this.getClass()
        1 * selectionResult.getSelectionRationale() >> Optional.empty()
        thrown(GenieJobResolutionException)

        when: "The selectors throw an exception"
        context = new JobResolverServiceImpl.JobResolutionContext(jobId, jobRequest, true)
        this.service.resolveCommand(context)

        then: "It is propagated"
        1 * this.persistenceService.findCommandsMatchingCriterion(commandCriterion, true) >> allCommands
        1 * this.persistenceService.findClustersMatchingAnyCriterion(allCommandsUniqueCriteria, true) >> allClusters
        1 * this.commandSelector.select(
            {
                verifyAll(it, CommandSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == jobRequest
                    it.getResources() == allCommands
                    it.getCommandToClusters() == [
                        (command0): command0Clusters,
                        (command1): command1Clusters,
                        (command2): command2Clusters
                    ]
                }
            }
        ) >> { throw new ResourceSelectionException() }
        thrown(GenieJobResolutionException)

        when: "The selectors select a command"
        context = new JobResolverServiceImpl.JobResolutionContext(jobId, jobRequest, true)
        this.service.resolveCommand(context)
        resolvedCommand = context.getCommand().orElseThrow({ new IllegalStateException() })

        then: "It is returned"
        1 * this.persistenceService.findCommandsMatchingCriterion(commandCriterion, true) >> allCommands
        1 * this.persistenceService.findClustersMatchingAnyCriterion(allCommandsUniqueCriteria, true) >> allClusters
        1 * this.commandSelector.select(
            {
                verifyAll(it, CommandSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == jobRequest
                    it.getResources() == allCommands
                    it.getCommandToClusters() == [
                        (command0): command0Clusters,
                        (command1): command1Clusters,
                        (command2): command2Clusters
                    ]
                }
            }
        ) >> selectionResult
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
        def clusters = Sets.newHashSet(cluster0, cluster1, cluster2)
        def jobId = UUID.randomUUID().toString()
        def jobRequest = createJobRequest(Lists.newArrayList(UUID.randomUUID().toString()), null, null)
        def commandClusters = [
            (command)      : clusters,
            (Mock(Command)): Sets.newHashSet(Mock(Cluster), Mock(Cluster))
        ]
        def context = Mock(JobResolverServiceImpl.JobResolutionContext) {
            getJobRequest() >> jobRequest
            getJobId() >> jobId
            isApiJob() >> true
        }

        ResourceSelectionResult<Cluster> selectionResult = Mock(ResourceSelectionResult) {
            getSelectionRationale() >> Optional.empty()
        }

        when: "No command was set in the context"
        this.service.resolveCluster(context)

        then: "An exception is thrown"
        1 * context.getCommand() >> Optional.empty()
        0 * context.setCluster(_ as Cluster)
        thrown(GenieJobResolutionException)

        when: "No command -> cluster map was stored in the context"
        this.service.resolveCluster(context)

        then: "An exception is thrown"
        1 * context.getCommand() >> Optional.of(command)
        1 * context.getCommandClusters() >> Optional.empty()
        0 * context.setCluster(_ as Cluster)
        thrown(GenieJobResolutionException)

        when: "No command -> cluster mapping is found"
        this.service.resolveCluster(context)

        then: "An exception is thrown"
        1 * context.getCommand() >> Optional.of(command)
        1 * context.getCommandClusters() >> Optional.of([:])
        0 * context.setCluster(_ as Cluster)
        thrown(GenieJobResolutionException)

        when: "No command -> cluster mapping is empty"
        this.service.resolveCluster(context)

        then: "An exception is thrown"
        1 * context.getCommand() >> Optional.of(command)
        1 * context.getCommandClusters() >> Optional.of([(command): Sets.newHashSet()])
        0 * context.setCluster(_ as Cluster)
        thrown(GenieJobResolutionException)

        when: "Selector has no preference and there are no more selectors"
        this.service.resolveCluster(context)

        then: "Exception is thrown"
        1 * context.getCommand() >> Optional.of(command)
        1 * context.getCommandClusters() >> Optional.of(commandClusters)
        1 * this.clusterSelector.select(
            {
                verifyAll(it, ClusterSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == jobRequest
                    it.isApiJob()
                    it.getCommand().orElse(null) == command
                    it.getClusters() == clusters
                }
            }
        ) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.empty()
        0 * context.setCluster(_ as Cluster)
        thrown(GenieJobResolutionException)

        when: "Selector has throws exception"
        this.service.resolveCluster(context)

        then: "Exception is thrown when no more selectors are available"
        1 * context.getCommand() >> Optional.of(command)
        1 * context.getCommandClusters() >> Optional.of(commandClusters)
        1 * this.clusterSelector.select(
            {
                verifyAll(it, ClusterSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == jobRequest
                    it.isApiJob()
                    it.getCommand().orElse(null) == command
                    it.getClusters() == clusters
                }
            }
        ) >> { throw new ResourceSelectionException("whoops") }
        0 * context.setCluster(_ as Cluster)
        thrown(GenieJobResolutionException)

        when: "Selector successfully selects a cluster"
        this.service.resolveCluster(context)

        then: "The cluster is added to the context"
        1 * context.getCommand() >> Optional.of(command)
        1 * context.getCommandClusters() >> Optional.of(commandClusters)
        1 * this.clusterSelector.select(
            {
                verifyAll(it, ClusterSelectionContext) {
                    it.getJobId() == jobId
                    it.getJobRequest() == jobRequest
                    it.isApiJob()
                    it.getCommand().orElse(null) == command
                    it.getClusters() == clusters
                }
            }
        ) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.of(cluster1)
        1 * context.setCluster(
            {
                verifyAll(it, Cluster) {
                    it == cluster1
                }
            }
        )
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

    def "can generate cluster criteria permutations"() {
        def commands = Sets.newHashSet(
            createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString())),
            createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString())),
            createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))
        )
        def jobRequest = createJobRequest(Lists.newArrayList(UUID.randomUUID().toString()), null, null)

        // build expected map
        final Map<Command, List<Criterion>> expectedMap = [:]
        for (def command : commands) {
            final List<Criterion> criteria = []
            for (def commandClusterCriteria : command.getClusterCriteria()) {
                for (def jobClusterCriteria : jobRequest.getCriteria().getClusterCriteria()) {
                    try {
                        criteria.add(this.service.mergeCriteria(commandClusterCriteria, jobClusterCriteria))
                    } catch (final IllegalArgumentException ignored) {
                        // swallow
                    }
                }
            }
            expectedMap[command] = criteria
        }

        when:
        def actualMap = this.service.generateClusterCriteriaPermutations(commands, jobRequest)

        then:
        actualMap == expectedMap
    }

    def "can flatten cluster criteria permutations"() {
        def commands = [
            createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString())),
            createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString())),
            createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))
        ] as Set
        Map<Command, List<Criterion>> permutations = commands.collectEntries { command ->
            [command, command.getClusterCriteria()]
        }

        def expectedSet = [] as Set
        for (def command : commands) {
            expectedSet.addAll(command.getClusterCriteria())
        }

        when:
        def actualSet = this.service.flattenClusterCriteriaPermutations(permutations)

        then:
        actualSet == expectedSet
    }

    def "can check if cluster matches criterion"() {
        def cluster = createCluster(UUID.randomUUID().toString())

        expect:
        !this.service.clusterMatchesCriterion(
            cluster,
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build()
        )
        !this.service.clusterMatchesCriterion(
            cluster,
            new Criterion.Builder().withName(UUID.randomUUID().toString()).build()
        )
        !this.service.clusterMatchesCriterion(
            cluster,
            new Criterion.Builder().withVersion(UUID.randomUUID().toString()).build()
        )
        !this.service.clusterMatchesCriterion(
            cluster,
            new Criterion.Builder().withStatus(UUID.randomUUID().toString()).build()
        )
        !this.service.clusterMatchesCriterion(
            cluster,
            new Criterion.Builder().withTags(Sets.newHashSet(UUID.randomUUID().toString())).build()
        )
        def builder = new Criterion.Builder()
        def metadata = cluster.getMetadata()
        this.service.clusterMatchesCriterion(cluster, builder.withId(cluster.getId()).build())
        this.service.clusterMatchesCriterion(cluster, builder.withName(metadata.getName()).build())
        this.service.clusterMatchesCriterion(cluster, builder.withVersion(metadata.getVersion()).build())
        this.service.clusterMatchesCriterion(cluster, builder.withStatus(metadata.getStatus().name()).build())
        this.service.clusterMatchesCriterion(
            cluster,
            builder.withTags(ImmutableSet.copyOf(Iterables.limit(metadata.getTags(), 1))).build()
        )
    }

    def "can generate command to cluster matrix"() {
        def cluster0 = createCluster(UUID.randomUUID().toString())
        def cluster1 = createCluster(UUID.randomUUID().toString())
        def cluster2 = createCluster(UUID.randomUUID().toString())
        def cluster3 = createCluster(UUID.randomUUID().toString())
        def cluster4Tags = Sets.newHashSet(cluster3.getMetadata().getTags())
        cluster4Tags.add(UUID.randomUUID().toString())
        def cluster4 = createCluster(UUID.randomUUID().toString(), cluster4Tags)
        def candidateClusters = Sets.newHashSet(cluster0, cluster1, cluster2, cluster3, cluster4)

        def command0 = createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))
        def command1 = createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))
        def command2 = createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))
        def command3 = createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))

        Map<Command, List<Criterion>> commandClusterCriteria = [:]
        commandClusterCriteria[command0] = Lists.newArrayList(
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build(), // no match
            new Criterion.Builder().withName(UUID.randomUUID().toString()).build(), // no match
            new Criterion.Builder().withName(cluster2.getMetadata().getName()).build(), // cluster2
            new Criterion.Builder().withTags(cluster3.getMetadata().getTags()).build(), // would be cluster3
        )
        // command1 should be filtered out as it won't match anything
        commandClusterCriteria[command1] = Lists.newArrayList(
            new Criterion.Builder().withTags(Sets.newHashSet(UUID.randomUUID().toString())).build()
        )
        commandClusterCriteria[command2] = Lists.newArrayList(
            new Criterion.Builder().withVersion(UUID.randomUUID().toString()).build(), // no match
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build(), // no match
            new Criterion.Builder().withTags(cluster3.getMetadata().getTags()).build(), // cluster3, cluster4
        )
        commandClusterCriteria[command3] = Lists.newArrayList(
            new Criterion.Builder().withVersion(cluster1.getMetadata().getVersion()).build(), // cluster1
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build(), // no match
            new Criterion.Builder().withTags(cluster3.getMetadata().getTags()).build(), // cluster3, cluster4
        )

        when:
        def result = this.service.generateCommandClustersMap(commandClusterCriteria, candidateClusters)

        then:
        result.size() == 3 // command1 should have been filtered out
        result[command0] == Sets.newHashSet(cluster2)
        result[command2] == Sets.newHashSet(cluster3, cluster4)
        result[command3] == Sets.newHashSet(cluster1)
    }

    def "JobResolutionContext behaves as expected"() {
        def jobId = UUID.randomUUID().toString()
        def jobRequest = createJobRequest(
            Lists.newArrayList(UUID.randomUUID().toString()),
            null,
            null
        )
        def apiJob = true
        def command = createCommand(UUID.randomUUID().toString(), Lists.newArrayList(UUID.randomUUID().toString()))
        def cluster = createCluster(UUID.randomUUID().toString())
        def applications = Lists.newArrayList(
            new Application(
                UUID.randomUUID().toString(),
                Instant.now(),
                Instant.now(),
                null,
                new ApplicationMetadata.Builder(
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    ApplicationStatus.ACTIVE
                ).build()
            )
        )
        def jobMemory = 1_234
        def environmentVariables = [
            "one": "two"
        ]
        def archiveLocation = UUID.randomUUID().toString()
        def jobDirectory = Mock(File)
        def timeout = 13_349_388
        def commandClusters = [
            (Mock(Command)): Sets.newHashSet(Mock(Cluster)),
            (Mock(Command)): Sets.newHashSet(Mock(Cluster), Mock(Cluster))
        ]

        when:
        def context = new JobResolverServiceImpl.JobResolutionContext(jobId, jobRequest, apiJob)

        then:
        context.getJobId() == jobId
        context.getJobRequest() == jobRequest
        context.isApiJob()
        !context.getCommand().isPresent()
        !context.getCluster().isPresent()
        !context.getApplications().isPresent()
        !context.getJobMemory().isPresent()
        !context.getEnvironmentVariables().isPresent()
        !context.getTimeout().isPresent()
        !context.getArchiveLocation().isPresent()
        !context.getJobDirectory().isPresent()
        !context.getCommandClusters().isPresent()

        when:
        context.build()

        then:
        thrown(IllegalStateException)

        when:
        context.setCommand(command)
        context.build()

        then:
        context.getCommand().orElse(null) == command
        thrown(IllegalStateException)

        when:
        context.setCluster(cluster)
        context.build()

        then:
        context.getCluster().orElse(null) == cluster
        thrown(IllegalStateException)

        when:
        context.setApplications(applications)
        context.build()

        then:
        context.getApplications().orElse(null) == applications
        thrown(IllegalStateException)

        when:
        context.setJobMemory(jobMemory)
        context.build()

        then:
        context.getJobMemory().orElse(null) == jobMemory
        thrown(IllegalStateException)

        when:
        context.setEnvironmentVariables(environmentVariables)
        context.build()

        then:
        context.getEnvironmentVariables().orElse(null) == environmentVariables
        thrown(IllegalStateException)

        when:
        context.setArchiveLocation(archiveLocation)
        context.build()

        then:
        context.getArchiveLocation().orElse(null) == archiveLocation
        thrown(IllegalStateException)

        when:
        context.setJobDirectory(jobDirectory)
        context.build()

        then:
        context.getJobDirectory().orElse(null) == jobDirectory
        noExceptionThrown()
        // could validate resolved job here?

        when:
        context.setTimeout(timeout)
        context.build()

        then:
        context.getTimeout().orElse(null) == timeout
        noExceptionThrown()

        when:
        context.toString()

        then:
        noExceptionThrown()

        when:
        context.setCommandClusters(commandClusters)

        then:
        context.getCommandClusters().orElse(null) == commandClusters
    }

    //region Helper Methods
    private static Cluster createCluster(String id) {
        return createCluster(id, null)
    }

    private static Cluster createCluster(String id, @Nullable Set<String> tags) {
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
            )
                .withTags(
                    tags != null
                        ? tags
                        : Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
                )
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
                : new AgentConfigRequest.Builder().withTimeoutRequested(requestedTimeout).build()
        )
    }

    /*
     * Helper method which creates clusters based on the criteria of the inputted command/job request.
     * Necessary to ensure in memory matching of criterion to clusters succeeds.
     */
    private static Set<Cluster> createClustersBasedOnCriteria(int numClusters, Command command, JobRequest jobRequest) {
        final Set<String> tags = Stream.concat(
            jobRequest
                .getCriteria()
                .getClusterCriteria()
                .stream()
                .flatMap { it.getTags().stream() },
            command
                .getClusterCriteria()
                .stream()
                .flatMap { it.getTags().stream() }
        ).collect(Collectors.toSet())
        Set<Cluster> clusters = Sets.newHashSet()
        for (def i = 0; i < numClusters; i++) {
            clusters.add(createCluster(UUID.randomUUID().toString(), tags))
        }
        return clusters
    }
    //endregion
}
