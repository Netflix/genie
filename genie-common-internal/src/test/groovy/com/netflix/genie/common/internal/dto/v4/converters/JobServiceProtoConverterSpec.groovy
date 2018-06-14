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
package com.netflix.genie.common.internal.dto.v4.converters

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata
import com.netflix.genie.common.internal.dto.v4.AgentConfigRequest
import com.netflix.genie.common.internal.dto.v4.AgentJobRequest
import com.netflix.genie.common.internal.dto.v4.Criterion
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment
import com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria
import com.netflix.genie.common.internal.dto.v4.JobMetadata
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.proto.AgentConfig
import com.netflix.genie.proto.AgentMetadata
import com.netflix.genie.proto.DryRunJobSpecificationRequest
import com.netflix.genie.proto.ExecutionResource
import com.netflix.genie.proto.JobSpecificationResponse
import com.netflix.genie.proto.ReserveJobIdRequest
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification
/**
 * Specifications for the {@link JobServiceProtoConverter} utility class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class JobServiceProtoConverterSpec extends Specification {

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

    def applicationIds = Lists.newArrayList(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
    )

    def clusterId = UUID.randomUUID().toString()
    def clusterConfigs = Sets.newHashSet(
            UUID.randomUUID().toString()
    )
    def clusterDependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
    )
    def clusterSetupFile = UUID.randomUUID().toString()

    def commandId = UUID.randomUUID().toString()
    def commandConfigs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
    )
    def commandDependencies = Sets.newHashSet(
            UUID.randomUUID().toString()
    )
    def commandSetupFile = UUID.randomUUID().toString()

    def application0Id = UUID.randomUUID().toString()
    def application0Configs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
    )
    def application0Dependencies = Sets.newHashSet(
            UUID.randomUUID().toString()
    )
    def application0SetupFile = UUID.randomUUID().toString()

    def application1Id = UUID.randomUUID().toString()
    def application1Configs = Sets.newHashSet(
            UUID.randomUUID().toString()
    )
    def application1Dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
    )
    def application1SetupFile = UUID.randomUUID().toString()

    def environmentVariables = ImmutableMap.of(UUID.randomUUID().toString(), UUID.randomUUID().toString())

    def agentHostname = UUID.randomUUID().toString()
    def agentVersion = UUID.randomUUID().toString()
    def agentPid = 21_031

    def converter = new JobServiceProtoConverter()

    def "Can convert JobRequest to ReserveJobIdRequest and vice versa"() {
        def jobRequest = createJobRequest(id)
        def agentClientMetadata = createAgentClientMetadata()
        def reserveJobIdRequest = createReserveJobIdRequest()

        when:
        def reserveJobIdRequest1 = converter.toProtoReserveJobIdRequest(jobRequest, agentClientMetadata)
        def jobRequest1 = converter.toJobRequestDTO(reserveJobIdRequest1)

        then:
        jobRequest1 == jobRequest


        when:
        def jobRequest2 = converter.toJobRequestDTO(reserveJobIdRequest)

        then:
        jobRequest2 == jobRequest
    }

    def "Can convert JobRequest to DryRunJobSpecificationRequest and vice versa"() {
        def jobRequest = createJobRequest(id)
        def dryRunJobSpecificationRequest = createDryRunJobSpecificationRequest()

        when:
        def dryRunJobSpecificationRequest1 = converter.toProtoDryRunJobSpecificationRequest(jobRequest)
        def jobRequest1 = converter.toJobRequestDTO(dryRunJobSpecificationRequest1)

        then:
        jobRequest1 == jobRequest


        when:
        def jobRequest2 = converter.toJobRequestDTO(dryRunJobSpecificationRequest)

        then:
        jobRequest2 == jobRequest
    }

    def "Can create a JobSpecificationRequest"() {
        when:
        def request = converter.toProtoJobSpecificationRequest(id)

        then:
        request.getId() == id
    }

    def "Can convert AgentMetadata to AgentClientMetadata"() {
        def agentMetadata = createReserveJobIdRequest().getAgentMetadata()
        def agentClientMetadata = createAgentClientMetadata()

        when:
        def agentClientMetadata1 = converter.toAgentClientMetadataDTO(agentMetadata)

        then:
        agentClientMetadata1 == agentClientMetadata
    }

    def "Can convert JobSpecification with default values"() {
        setup:
        def originalSpecification = new JobSpecification(
                ["echo"].asList(),
                new JobSpecification.ExecutionResource(
                        "my-job",
                        new ExecutionEnvironment(null, null, null)
                ),
                new JobSpecification.ExecutionResource(
                        "my-cluster",
                        new ExecutionEnvironment(null, null, null)
                ),
                new JobSpecification.ExecutionResource(
                        "my-command",
                        new ExecutionEnvironment(null, null, null)
                ),
                null,
                null,
                false,
                new File("/tmp/jobs")
        )

        when:
        def specResponseProto = converter.toProtoJobSpecificationResponse(originalSpecification)
        def convertedSpecification = converter.toJobSpecificationDTO(specResponseProto.getSpecification())

        then:
        originalSpecification == convertedSpecification
    }

    def "Can convert JobSpecification to JobSpecificationResponse and vice versa"() {
        def jobSpecification = createJobSpecification()
        def jobSpecificationResponse = createJobSpecificationResponse()

        when:
        def jobSpecificationResponse2 = converter.toProtoJobSpecificationResponse(jobSpecification)
        def jobSpecification2 = converter.toJobSpecificationDTO(jobSpecificationResponse2.getSpecification())

        then:
        jobSpecificationResponse2.hasSpecification()
        !jobSpecificationResponse2.hasError()
        jobSpecification2 == jobSpecification


        when:
        def jobSpecification3 = converter.toJobSpecificationDTO(jobSpecificationResponse.getSpecification())

        then:
        jobSpecification3 == jobSpecification
    }

    def "Can convert id and AgentMetadata to ClaimJobRequest"() {
        def agentClientMetadata = createAgentClientMetadata()

        when:
        def request = converter.toProtoClaimJobRequest(id, agentClientMetadata)
        def agentClientMetadata2 = converter.toAgentClientMetadataDTO(request.getAgentMetadata())

        then:
        request.getId() == id
        agentClientMetadata == agentClientMetadata2
    }


    def "Can convert JobRequest to ReserveJobIdRequest with null id"() {
        def jobRequest = createJobRequest(null)
        def agentClientMetadata = createAgentClientMetadata()

        when:
        def reserveJobIdRequest = converter.toProtoReserveJobIdRequest(jobRequest, agentClientMetadata)
        def jobRequest1 = converter.toJobRequestDTO(reserveJobIdRequest)

        then:
        jobRequest1 == jobRequest

        when:
        def resolveSpecDryRunRequest = converter.toProtoDryRunJobSpecificationRequest(jobRequest)
        def jobRequest2 = converter.toJobRequestDTO(resolveSpecDryRunRequest)

        then:
        jobRequest2 == jobRequest

    }

    def "Can convert parameters to ChangeJobStatusRequest"() {
        def currentStatus = JobStatus.INIT
        def newStatus = JobStatus.RUNNING
        def message = "..."

        when:
        def changeJobStatusRequest = converter.toProtoChangeJobStatusRequest(
                id,
                currentStatus,
                newStatus,
                message
        )

        then:
        id == changeJobStatusRequest.getId()
        currentStatus == JobStatus.parse(changeJobStatusRequest.getCurrentStatus())
        newStatus == JobStatus.parse(changeJobStatusRequest.getNewStatus())
        message == changeJobStatusRequest.getNewStatusMessage()
    }

    AgentJobRequest createJobRequest(String id) {
        def jobMetadata = new JobMetadata.Builder(name, user, version)
                .withDescription(description)
                .withMetadata(metadata)
                .withTags(tags)
                .withGrouping(grouping)
                .withGroupingInstance(groupingInstance)
                .withEmail(email)
                .build()

        def executionResourceCriteria = new ExecutionResourceCriteria(
                Lists.newArrayList(
                        clusterCriterion0,
                        clusterCriterion1,
                        clusterCriterion2
                ),
                commandCriterion,
                applicationIds
        )

        def agentConfigRequest = new AgentConfigRequest.Builder()
                .withInteractive(interactive)
                .withRequestedJobDirectoryLocation(jobDirectoryLocation)
                .build()

        return new AgentJobRequest.Builder(jobMetadata, executionResourceCriteria, agentConfigRequest)
                .withRequestedId(id)
                .withCommandArgs(commandArgs)
                .withResources(new ExecutionEnvironment(configs, dependencies, setupFile))
                .build()
    }

    AgentClientMetadata createAgentClientMetadata() {
        return new AgentClientMetadata(agentHostname, agentVersion, agentPid)
    }

    ReserveJobIdRequest createReserveJobIdRequest() {
        def jobMetadataProto = createJobMetadataProto()

        def executionResourceCriteriaProto = createExecutionResourceCriteriaProto()

        def agentConfigProto = createAgentConfig()

        def agentMetadataProto = AgentMetadata
                .newBuilder()
                .setAgentHostname(agentHostname)
                .setAgentVersion(agentVersion)
                .setAgentPid(agentPid)
                .build()

        return ReserveJobIdRequest
                .newBuilder()
                .setMetadata(jobMetadataProto)
                .setCriteria(executionResourceCriteriaProto)
                .setAgentConfig(agentConfigProto)
                .setAgentMetadata(agentMetadataProto)
                .build()
    }

    DryRunJobSpecificationRequest createDryRunJobSpecificationRequest() {
        def jobMetadataProto = createJobMetadataProto()
        def executionResourceCriteriaProto = createExecutionResourceCriteriaProto()
        def agentConfigProto = createAgentConfig()

        return DryRunJobSpecificationRequest
                .newBuilder()
                .setMetadata(jobMetadataProto)
                .setCriteria(executionResourceCriteriaProto)
                .setAgentConfig(agentConfigProto)
                .build()
    }

    JobSpecification createJobSpecification() {
        return new JobSpecification(
                commandArgs,
                new JobSpecification.ExecutionResource(
                        id,
                        new ExecutionEnvironment(configs, dependencies, setupFile)
                ),
                new JobSpecification.ExecutionResource(
                        clusterId,
                        new ExecutionEnvironment(clusterConfigs, clusterDependencies, clusterSetupFile)
                ),
                new JobSpecification.ExecutionResource(
                        commandId,
                        new ExecutionEnvironment(commandConfigs, commandDependencies, commandSetupFile)
                ),
                Lists.newArrayList(
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
                ),
                environmentVariables,
                interactive,
                new File(jobDirectoryLocation)
        )
    }

    JobSpecificationResponse createJobSpecificationResponse() {
        def job = ExecutionResource
                .newBuilder()
                .setId(id)
                .addAllConfigs(configs)
                .addAllDependencies(dependencies)
                .setSetupFile(setupFile)
                .build()

        def cluster = ExecutionResource
                .newBuilder()
                .setId(clusterId)
                .addAllConfigs(clusterConfigs)
                .addAllDependencies(clusterDependencies)
                .setSetupFile(clusterSetupFile)
                .build()

        def command = ExecutionResource
                .newBuilder()
                .setId(commandId)
                .addAllConfigs(commandConfigs)
                .addAllDependencies(commandDependencies)
                .setSetupFile(commandSetupFile)
                .build()

        def application0 = ExecutionResource
                .newBuilder()
                .setId(application0Id)
                .addAllConfigs(application0Configs)
                .addAllDependencies(application0Dependencies)
                .setSetupFile(application0SetupFile)
                .build()

        def application1 = ExecutionResource
                .newBuilder()
                .setId(application1Id)
                .addAllConfigs(application1Configs)
                .addAllDependencies(application1Dependencies)
                .setSetupFile(application1SetupFile)
                .build()

        def jobSpecification = com.netflix.genie.proto.JobSpecification
                .newBuilder()
                .setIsInteractive(interactive)
                .setJobDirectoryLocation(jobDirectoryLocation)
                .putAllEnvironmentVariables(environmentVariables)
                .addAllCommandArgs(commandArgs)
                .setJob(job)
                .setCluster(cluster)
                .setCommand(command)
                .addAllApplications(Lists.newArrayList(application0, application1))
                .build()

        return JobSpecificationResponse.newBuilder().setSpecification(jobSpecification).build()
    }

    com.netflix.genie.proto.JobMetadata createJobMetadataProto() {
        return com.netflix.genie.proto.JobMetadata
                .newBuilder()
                .setId(id)
                .setName(name)
                .setUser(user)
                .setVersion(version)
                .setDescription(description)
                .addAllTags(tags)
                .setMetadata(metadataString)
                .setEmail(email)
                .setGrouping(grouping)
                .setGroupingInstance(groupingInstance)
                .setSetupFile(setupFile)
                .addAllConfigs(configs)
                .addAllDependencies(dependencies)
                .addAllCommandArgs(commandArgs)
                .build()
    }

    com.netflix.genie.proto.ExecutionResourceCriteria createExecutionResourceCriteriaProto() {
        return com.netflix.genie.proto.ExecutionResourceCriteria.newBuilder()
                .addAllClusterCriteria(
                Lists.newArrayList(
                        com.netflix.genie.proto.Criterion
                                .newBuilder()
                                .setId(clusterCriterion0Id)
                                .setName(clusterCriterion0Name)
                                .setVersion(clusterCriterion0Version)
                                .setStatus(clusterCriterion0Status)
                                .addAllTags(clusterCriterion0Tags)
                                .build(),
                        com.netflix.genie.proto.Criterion
                                .newBuilder()
                                .setId(clusterCriterion1Id)
                                .setName(clusterCriterion1Name)
                                .setVersion(clusterCriterion1Version)
                                .setStatus(clusterCriterion1Status)
                                .addAllTags(clusterCriterion1Tags)
                                .build(),
                        com.netflix.genie.proto.Criterion
                                .newBuilder()
                                .setId(clusterCriterion2Id)
                                .setName(clusterCriterion2Name)
                                .setVersion(clusterCriterion2Version)
                                .setStatus(clusterCriterion2Status)
                                .addAllTags(clusterCriterion2Tags)
                                .build()
                )
        ).setCommandCriterion(
                com.netflix.genie.proto.Criterion
                        .newBuilder()
                        .setId(commandCriterionId)
                        .setName(commandCriterionName)
                        .setVersion(commandCriterionVersion)
                        .setStatus(commandCriterionStatus)
                        .addAllTags(commandCriterionTags)
                        .build()
        ).addAllRequestedApplicationIdOverrides(applicationIds)
                .build()
    }

    AgentConfig createAgentConfig() {
        return AgentConfig
                .newBuilder()
                .setIsInteractive(interactive)
                .setJobDirectoryLocation(jobDirectoryLocation)
                .build()
    }
}
