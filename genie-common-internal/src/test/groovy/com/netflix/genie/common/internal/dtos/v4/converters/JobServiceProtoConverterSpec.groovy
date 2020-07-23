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

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.google.protobuf.Int32Value
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata
import com.netflix.genie.common.external.dtos.v4.AgentConfigRequest
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus
import com.netflix.genie.common.external.dtos.v4.Criterion
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment
import com.netflix.genie.common.external.dtos.v4.ExecutionResourceCriteria
import com.netflix.genie.common.external.dtos.v4.JobMetadata
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.external.util.GenieObjectMapper
import com.netflix.genie.proto.AgentConfig
import com.netflix.genie.proto.AgentMetadata
import com.netflix.genie.proto.ChangeJobArchiveStatusRequest
import com.netflix.genie.proto.ConfigureRequest
import com.netflix.genie.proto.DryRunJobSpecificationRequest
import com.netflix.genie.proto.ExecutionResource
import com.netflix.genie.proto.GetJobStatusRequest
import com.netflix.genie.proto.HandshakeRequest
import com.netflix.genie.proto.JobSpecificationResponse
import com.netflix.genie.proto.ReserveJobIdRequest
import spock.lang.Specification

/**
 * Specifications for the {@link JobServiceProtoConverter} utility class.
 *
 * @author tgianos
 */
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
    def archiveLocation = UUID.randomUUID().toString()
    def executableArgs = Lists.newArrayList(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString()
    )
    def jobArgs = Lists.newArrayList(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString()
    )
    def jobDirectoryLocation = "/tmp"
    def timeout = 23_382

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

    def archivingDisabled = true

    def "Can convert JobRequest to ReserveJobIdRequest and vice versa"() {
        def jobRequest = createJobRequest(id)
        def agentClientMetadata = createAgentClientMetadata()
        def reserveJobIdRequest = createReserveJobIdRequest()

        when:
        def reserveJobIdRequest1 = converter.toReserveJobIdRequestProto(jobRequest, agentClientMetadata)
        def jobRequest1 = converter.toJobRequestDto(reserveJobIdRequest1)

        then:
        jobRequest1 == jobRequest

        when:
        def jobRequest2 = converter.toJobRequestDto(reserveJobIdRequest)

        then:
        jobRequest2 == jobRequest

        when:
        def jobRequest3 = createJobRequest(id)
        def reserveJobIdRequest3 = converter.toReserveJobIdRequestProto(jobRequest3, agentClientMetadata)
        def jobRequest4 = converter.toJobRequestDto(reserveJobIdRequest3)

        then:
        jobRequest4 == jobRequest3
    }

    def "Can convert JobRequest to DryRunJobSpecificationRequest and vice versa"() {
        def jobRequest = createJobRequest(id)
        def dryRunJobSpecificationRequest = createDryRunJobSpecificationRequest()

        when:
        def dryRunJobSpecificationRequest1 = converter.toDryRunJobSpecificationRequestProto(jobRequest)
        def jobRequest1 = converter.toJobRequestDto(dryRunJobSpecificationRequest1)

        then:
        jobRequest1 == jobRequest

        when:
        def jobRequest2 = converter.toJobRequestDto(dryRunJobSpecificationRequest)

        then:
        jobRequest2 == jobRequest

        when:
        def jobRequest3 = createJobRequest(id)
        def dryRunJobSpecificationRequest3 = converter.toDryRunJobSpecificationRequestProto(jobRequest3)
        def jobRequest4 = converter.toJobRequestDto(dryRunJobSpecificationRequest3)

        then:
        jobRequest3 == jobRequest4

    }

    def "Can create a JobSpecificationRequest"() {
        when:
        def request = converter.toJobSpecificationRequestProto(id)

        then:
        request.getId() == id
    }

    def "Can convert AgentMetadata to AgentClientMetadata"() {
        def agentMetadata = createReserveJobIdRequest().getAgentMetadata()
        def agentClientMetadata = createAgentClientMetadata()

        when:
        def agentClientMetadata1 = converter.toAgentClientMetadataDto(agentMetadata)

        then:
        agentClientMetadata1 == agentClientMetadata
    }

    def "Can convert JobSpecification with default values"() {
        setup:
        def originalSpecification = new JobSpecification(
            ["echo"].asList(),
            new ArrayList<String>(0),
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
            new File("/tmp/jobs"),
            null,
            null
        )

        when:
        def specProto = converter.toJobSpecificationProto(originalSpecification)
        def convertedSpecification = converter.toJobSpecificationDto(specProto)

        then:
        originalSpecification == convertedSpecification
    }

    def "Can convert JobSpecification to JobSpecificationResponse and vice versa"() {
        def jobSpecification = createJobSpecification()
        def jobSpecificationResponse = createJobSpecificationResponse()

        when:
        def jobSpecificationResponse2 = converter.toJobSpecificationResponseProto(jobSpecification)
        def jobSpecification2 = converter.toJobSpecificationDto(jobSpecificationResponse2.getSpecification())

        then:
        jobSpecificationResponse2.hasSpecification()
        !jobSpecificationResponse2.hasError()
        jobSpecification2 == jobSpecification

        when:
        def jobSpecification3 = converter.toJobSpecificationDto(jobSpecificationResponse.getSpecification())

        then:
        jobSpecification3 == jobSpecification
    }

    def "Can convert id and AgentMetadata to ClaimJobRequest"() {
        def agentClientMetadata = createAgentClientMetadata()

        when:
        def request = converter.toClaimJobRequestProto(id, agentClientMetadata)
        def agentClientMetadata2 = converter.toAgentClientMetadataDto(request.getAgentMetadata())

        then:
        request.getId() == id
        agentClientMetadata == agentClientMetadata2
    }

    def "Can convert JobRequest to ReserveJobIdRequest with null id"() {
        def jobRequest = createJobRequest(null)
        def agentClientMetadata = createAgentClientMetadata()

        when:
        def reserveJobIdRequest = converter.toReserveJobIdRequestProto(jobRequest, agentClientMetadata)
        def jobRequest1 = converter.toJobRequestDto(reserveJobIdRequest)

        then:
        jobRequest1 == jobRequest

        when:
        def resolveSpecDryRunRequest = converter.toDryRunJobSpecificationRequestProto(jobRequest)
        def jobRequest2 = converter.toJobRequestDto(resolveSpecDryRunRequest)

        then:
        jobRequest2 == jobRequest

        when:
        def jobRequest3 = createJobRequest(id)
        def reserveJobIdRequest3 = converter.toReserveJobIdRequestProto(jobRequest3, agentClientMetadata)
        def jobRequest4 = converter.toJobRequestDto(reserveJobIdRequest3)

        then:
        jobRequest4 == jobRequest3
    }

    def "Can convert parameters to ChangeJobStatusRequest"() {
        def currentStatus = JobStatus.INIT
        def newStatus = JobStatus.RUNNING
        def message = "..."

        when:
        def changeJobStatusRequest = converter.toChangeJobStatusRequestProto(
            id,
            currentStatus,
            newStatus,
            message
        )

        then:
        id == changeJobStatusRequest.getId()
        currentStatus == JobStatus.valueOf(changeJobStatusRequest.getCurrentStatus())
        newStatus == JobStatus.valueOf(changeJobStatusRequest.getNewStatus())
        message == changeJobStatusRequest.getNewStatusMessage()
    }

    def "Can convert AgentClientMetadata to HandshakeRequest"() {
        AgentClientMetadata agentClientMetadata = createAgentClientMetadata()

        when:
        HandshakeRequest handshakeRequest = converter.toHandshakeRequestProto(agentClientMetadata)

        then:
        AgentMetadata agentMetadata = handshakeRequest.getAgentMetadata()
        agentMetadata != null
        agentMetadata.getAgentHostname() == agentHostname
        agentMetadata.getAgentVersion() == agentVersion
        agentMetadata.getAgentPid() == agentPid
    }

    def "Can convert AgentClientMetadata to ConfigureRequest"() {
        AgentClientMetadata agentClientMetadata = createAgentClientMetadata()

        when:
        ConfigureRequest configureRequest = converter.toConfigureRequestProto(agentClientMetadata)

        then:
        AgentMetadata agentMetadata = configureRequest.getAgentMetadata()
        agentMetadata != null
        agentMetadata.getAgentHostname() == agentHostname
        agentMetadata.getAgentVersion() == agentVersion
        agentMetadata.getAgentPid() == agentPid
    }

    def "Can convert AgentConfigRequest DTO to AgentConfig Proto and vice versa"() {
        def agentConfigRequestWithOptionals = new AgentConfigRequest.Builder()
            .withInteractive(interactive)
            .withRequestedJobDirectoryLocation(jobDirectoryLocation)
            .withTimeoutRequested(timeout)
            .withArchivingDisabled(archivingDisabled)
            .build()

        def agentConfigRequestWithoutOptionals = new AgentConfigRequest.Builder()
            .withInteractive(false)
            .build()

        when:
        def protoWithOptionals = converter.toAgentConfigProto(agentConfigRequestWithOptionals)

        then:
        protoWithOptionals.getIsInteractive()
        protoWithOptionals.getJobDirectoryLocation() == jobDirectoryLocation
        protoWithOptionals.hasTimeout()
        protoWithOptionals.getTimeout().getValue() == timeout
        protoWithOptionals.getArchivingDisabled() == archivingDisabled

        when:
        def protoWithoutOptionals = converter.toAgentConfigProto(agentConfigRequestWithoutOptionals)

        then:
        !protoWithoutOptionals.getIsInteractive()
        protoWithoutOptionals.getJobDirectoryLocation() == ""
        !protoWithoutOptionals.hasTimeout()
        !protoWithoutOptionals.getArchivingDisabled()

        when:
        def dtoWithOptionals = converter.toAgentConfigRequestDto(protoWithOptionals)
        def dtoWithoutOptionals = converter.toAgentConfigRequestDto(protoWithoutOptionals)

        then:
        agentConfigRequestWithOptionals == dtoWithOptionals
        agentConfigRequestWithoutOptionals == dtoWithoutOptionals
    }

    def "Can create GetJobStatusRequest"() {
        when:
        GetJobStatusRequest proto = converter.toGetJobStatusRequestProto(id)

        then:
        proto != null
        proto.getId() == id
    }

    def "Can create ChangeJobArchiveStatusRequest"() {
        when:
        ChangeJobArchiveStatusRequest proto = converter.toChangeJobStatusArchiveRequestProto(id, ArchiveStatus.ARCHIVED)

        then:
        proto != null
        proto.getId() == id
        proto.getNewStatus() == ArchiveStatus.ARCHIVED.name()
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
            .withTimeoutRequested(timeout)
            .build()

        return new AgentJobRequest.Builder(jobMetadata, executionResourceCriteria, agentConfigRequest)
            .withRequestedId(id)
            .withCommandArgs(executableArgs)
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
            executableArgs,
            jobArgs,
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
            new File(jobDirectoryLocation),
            archiveLocation,
            timeout
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
            .addAllExecutableAndArgs(executableArgs)
            .addAllJobArgs(jobArgs)
            .addAllCommandArgs(executableArgs)
            .addAllCommandArgs(jobArgs)
            .setJob(job)
            .setCluster(cluster)
            .setCommand(command)
            .addAllApplications(Lists.newArrayList(application0, application1))
            .setArchiveLocation(archiveLocation)
            .setTimeout(Int32Value.of(timeout))
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
            .addAllCommandArgs(executableArgs)
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
            .setTimeout(Int32Value.of(timeout))
            .build()
    }
}
