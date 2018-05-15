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
package com.netflix.genie.web.rpc.grpc.services.impl.v4

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment
import com.netflix.genie.common.internal.dto.v4.JobRequest
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobSpecificationNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import com.netflix.genie.proto.AgentConfig
import com.netflix.genie.proto.AgentMetadata
import com.netflix.genie.proto.ClaimJobError
import com.netflix.genie.proto.ClaimJobRequest
import com.netflix.genie.proto.ClaimJobResponse
import com.netflix.genie.proto.Criterion
import com.netflix.genie.proto.DryRunJobSpecificationRequest
import com.netflix.genie.proto.ExecutionResourceCriteria
import com.netflix.genie.proto.JobMetadata
import com.netflix.genie.proto.JobSpecificationRequest
import com.netflix.genie.proto.JobSpecificationResponse
import com.netflix.genie.proto.ReserveJobIdRequest
import com.netflix.genie.proto.ReserveJobIdResponse
import com.netflix.genie.web.services.AgentJobService
import io.grpc.stub.StreamObserver
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for the {@link GRpcJobServiceImpl} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
class GRpcJobServiceImplSpec extends Specification {

    // TODO: explore using GrpcServerRule

    def id = UUID.randomUUID().toString()
    def name = UUID.randomUUID().toString()
    def user = UUID.randomUUID().toString()
    def description = UUID.randomUUID().toString()
    def version = UUID.randomUUID().toString()
    def metadataString = "{\"" + UUID.randomUUID().toString() + "\":\"" + UUID.randomUUID().toString() + "\"}"
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
    def jobDirectoryLocation = new File("/tmp").getAbsolutePath()

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
    def commandCriterionStatus = UUID.randomUUID().toString()
    def commandCriterionTags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
    )
    def clusterCriterion0Id = UUID.randomUUID().toString()
    def clusterCriterion0Name = UUID.randomUUID().toString()
    def clusterCriterion0Status = UUID.randomUUID().toString()
    def clusterCriterion0Tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
    )
    def clusterCriterion1Id = UUID.randomUUID().toString()
    def clusterCriterion1Name = UUID.randomUUID().toString()
    def clusterCriterion1Status = UUID.randomUUID().toString()
    def clusterCriterion1Tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
    )
    def clusterCriterion2Id = UUID.randomUUID().toString()
    def clusterCriterion2Name = UUID.randomUUID().toString()
    def clusterCriterion2Status = UUID.randomUUID().toString()
    def clusterCriterion2Tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
    )
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
    def agentPid = 12_345

    def "Can reserve job id"() {
        def reserveJobIdRequest = createReserveJobIdRequest()
        def agentJobService = Mock(AgentJobService)
        def service = new GRpcJobServiceImpl(agentJobService)
        StreamObserver<ReserveJobIdResponse> responseObserver = Mock()

        when:
        service.reserveJobId(reserveJobIdRequest, responseObserver)

        then:
        1 * agentJobService.reserveJobId(_ as JobRequest, _ as AgentClientMetadata) >> id
        1 * responseObserver.onNext(_ as ReserveJobIdResponse)
        1 * responseObserver.onCompleted()

        when:
        service.reserveJobId(reserveJobIdRequest, responseObserver)

        then:
        1 * agentJobService.reserveJobId(_ as JobRequest, _ as AgentClientMetadata) >> {
            throw new RuntimeException("uh oh")
        }
        1 * responseObserver.onNext(_ as ReserveJobIdResponse)
        1 * responseObserver.onCompleted()
    }

    def "Can resolve job specification"() {
        def jobSpecification = createJobSpecification()
        def agentJobService = Mock(AgentJobService)
        def service = new GRpcJobServiceImpl(agentJobService)
        StreamObserver<JobSpecificationResponse> responseObserver = Mock()

        when:
        service.resolveJobSpecification(createJobSpecificationRequest(), responseObserver)

        then:
        1 * agentJobService.resolveJobSpecification(id) >> jobSpecification
        1 * responseObserver.onNext(_ as JobSpecificationResponse)
        1 * responseObserver.onCompleted()
    }

    def "Can't resolve job specification"() {
        def agentJobService = Mock(AgentJobService)
        def service = new GRpcJobServiceImpl(agentJobService)
        StreamObserver<JobSpecificationResponse> responseObserver = Mock()

        when:
        service.resolveJobSpecification(createJobSpecificationRequest(), responseObserver)

        then:
        1 * agentJobService.resolveJobSpecification(id) >> {
            throw new RuntimeException(UUID.randomUUID().toString())
        }
        1 * responseObserver.onNext(_ as JobSpecificationResponse)
        1 * responseObserver.onCompleted()
    }

    def "Can get a job specification that has already been resolved"() {
        def jobSpecification = createJobSpecification()
        def agentJobService = Mock(AgentJobService)
        def service = new GRpcJobServiceImpl(agentJobService)
        StreamObserver<JobSpecificationResponse> responseObserver = Mock()

        when:
        service.getJobSpecification(createJobSpecificationRequest(), responseObserver)

        then:
        1 * agentJobService.getJobSpecification(id) >> jobSpecification
        1 * responseObserver.onNext(_ as JobSpecificationResponse)
        1 * responseObserver.onCompleted()
    }

    def "Can not get a job specification that hasn't already been resolved"() {
        def agentJobService = Mock(AgentJobService)
        def service = new GRpcJobServiceImpl(agentJobService)
        StreamObserver<JobSpecificationResponse> responseObserver = Mock()

        when:
        service.getJobSpecification(createJobSpecificationRequest(), responseObserver)

        then:
        1 * agentJobService.getJobSpecification(id) >> {
            throw new GenieJobSpecificationNotFoundException()
        }
        1 * responseObserver.onNext(_ as JobSpecificationResponse)
        1 * responseObserver.onCompleted()
    }

    def "Can dry run job specification resolution"() {
        def agentJobService = Mock(AgentJobService)
        def service = new GRpcJobServiceImpl(agentJobService)
        StreamObserver<JobSpecificationResponse> responseObserver = Mock()
        def jobSpecification = createJobSpecification()

        when:
        service.resolveJobSpecificationDryRun(createDryRunSpecificationRequest(), responseObserver)

        then:
        1 * agentJobService.dryRunJobSpecificationResolution(_ as JobRequest) >> jobSpecification
        1 * responseObserver.onNext(_ as JobSpecificationResponse)
        1 * responseObserver.onCompleted()
    }

    def "Can not dry run job specification resolution if resolution throws an error"() {
        def agentJobService = Mock(AgentJobService)
        def service = new GRpcJobServiceImpl(agentJobService)
        StreamObserver<JobSpecificationResponse> responseObserver = Mock()

        when:
        service.resolveJobSpecificationDryRun(createDryRunSpecificationRequest(), responseObserver)

        then:
        1 * agentJobService.dryRunJobSpecificationResolution(_ as JobRequest) >> {
            throw new GenieRuntimeException()
        }
        1 * responseObserver.onNext(_ as JobSpecificationResponse)
        1 * responseObserver.onCompleted()
    }

    def "Can't claim job for agent if id isn't present"() {
        def agentJobService = Mock(AgentJobService)
        def service = new GRpcJobServiceImpl(agentJobService)
        StreamObserver<ClaimJobResponse> responseObserver = Mock()
        ClaimJobResponse response

        when:
        service.claimJob(ClaimJobRequest.newBuilder().setId("").build(), responseObserver)

        then:
        1 * responseObserver.onNext(_ as ClaimJobResponse) >> {
            arguments -> response = (ClaimJobResponse) arguments[0]
        }
        0 * agentJobService.claimJob(_ as String, _ as AgentClientMetadata)
        response != null
        !response.getSuccessful()
        response.getError().getType() == ClaimJobError.Type.NO_ID_SUPPLIED
        response.getError().getMessage() != null
    }

    @Unroll
    def "Can't claim job for agent when #error.class is thrown"() {
        def agentJobService = Mock(AgentJobService)
        def service = new GRpcJobServiceImpl(agentJobService)
        StreamObserver<ClaimJobResponse> responseObserver = Mock()
        ClaimJobResponse response

        when:
        service.claimJob(createClaimJobRequest(), responseObserver)

        then:
        1 * responseObserver.onNext(_ as ClaimJobResponse) >> {
            arguments -> response = (ClaimJobResponse) arguments[0]
        }
        1 * agentJobService.claimJob(_ as String, _ as AgentClientMetadata) >> {
            throw error
        }
        response != null
        !response.getSuccessful()
        response.getError().getType() == type
        response.getError().getMessage() == error.getMessage()

        where:
        error                                                             | type
        new GenieJobAlreadyClaimedException(UUID.randomUUID().toString()) | ClaimJobError.Type.ALREADY_CLAIMED
        new GenieJobNotFoundException(UUID.randomUUID().toString())       | ClaimJobError.Type.NO_SUCH_JOB
        new GenieInvalidStatusException(UUID.randomUUID().toString())     | ClaimJobError.Type.INVALID_STATUS
        new RuntimeException(UUID.randomUUID().toString())                | ClaimJobError.Type.UNKNOWN
    }

    def "Can claim job for agent"() {
        def agentJobService = Mock(AgentJobService)
        def service = new GRpcJobServiceImpl(agentJobService)
        StreamObserver<ClaimJobResponse> responseObserver = Mock()
        ClaimJobResponse response

        when:
        service.claimJob(createClaimJobRequest(), responseObserver)

        then:
        1 * responseObserver.onNext(_ as ClaimJobResponse) >> {
            arguments -> response = (ClaimJobResponse) arguments[0]
        }
        1 * agentJobService.claimJob(_ as String, _ as AgentClientMetadata)
        response != null
        response.getSuccessful()
    }

    JobSpecificationRequest createJobSpecificationRequest() {
        return JobSpecificationRequest.newBuilder().setId(id).build()
    }

    ReserveJobIdRequest createReserveJobIdRequest() {
        def jobMetadataProto = createJobMetadataProto()
        def executionResourceCriteriaProto = createExecutionResourceCriteriaProto()
        def agentConfigProto = createAgentConfigProto()
        def agentMetadata = createAgentMetadataProto()

        return ReserveJobIdRequest
                .newBuilder()
                .setMetadata(jobMetadataProto)
                .setCriteria(executionResourceCriteriaProto)
                .setAgentConfig(agentConfigProto)
                .setAgentMetadata(agentMetadata)
                .build()
    }

    DryRunJobSpecificationRequest createDryRunSpecificationRequest() {
        def jobMetadataProto = createJobMetadataProto()
        def executionResourceCriteriaProto = createExecutionResourceCriteriaProto()
        def agentConfigProto = createAgentConfigProto()

        return DryRunJobSpecificationRequest
                .newBuilder()
                .setMetadata(jobMetadataProto)
                .setCriteria(executionResourceCriteriaProto)
                .setAgentConfig(agentConfigProto)
                .build()
    }

    ClaimJobRequest createClaimJobRequest() {
        def id = UUID.randomUUID().toString()
        def agentMetadataProto = createAgentMetadataProto()

        return ClaimJobRequest
                .newBuilder()
                .setId(id)
                .setAgentMetadata(agentMetadataProto)
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

    JobMetadata createJobMetadataProto() {
        return JobMetadata
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

    ExecutionResourceCriteria createExecutionResourceCriteriaProto() {
        ExecutionResourceCriteria.newBuilder()
                .addAllClusterCriteria(
                Lists.newArrayList(
                        Criterion
                                .newBuilder()
                                .setId(clusterCriterion0Id)
                                .setName(clusterCriterion0Name)
                                .setStatus(clusterCriterion0Status)
                                .addAllTags(clusterCriterion0Tags)
                                .build(),
                        Criterion
                                .newBuilder()
                                .setId(clusterCriterion1Id)
                                .setName(clusterCriterion1Name)
                                .setStatus(clusterCriterion1Status)
                                .addAllTags(clusterCriterion1Tags)
                                .build(),
                        Criterion
                                .newBuilder()
                                .setId(clusterCriterion2Id)
                                .setName(clusterCriterion2Name)
                                .setStatus(clusterCriterion2Status)
                                .addAllTags(clusterCriterion2Tags)
                                .build()
                )
        ).setCommandCriterion(
                Criterion
                        .newBuilder()
                        .setId(commandCriterionId)
                        .setName(commandCriterionName)
                        .setStatus(commandCriterionStatus)
                        .addAllTags(commandCriterionTags)
                        .build()
        ).addAllRequestedApplicationIdOverrides(applicationIds)
                .build()
    }

    AgentConfig createAgentConfigProto() {
        return AgentConfig
                .newBuilder()
                .setIsInteractive(interactive)
                .setJobDirectoryLocation(jobDirectoryLocation)
                .build()
    }

    AgentMetadata createAgentMetadataProto() {
        return AgentMetadata
                .newBuilder()
                .setAgentHostname(agentHostname)
                .setAgentVersion(agentVersion)
                .setAgentPid(agentPid)
                .build()
    }
}
