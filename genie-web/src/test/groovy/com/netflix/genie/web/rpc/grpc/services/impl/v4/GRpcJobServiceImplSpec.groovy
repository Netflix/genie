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
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.proto.*
import com.netflix.genie.web.services.JobPersistenceService
import com.netflix.genie.web.services.JobSpecificationService
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.MeterRegistry
import spock.lang.Specification

/**
 * Specifications for the {@link GRpcJobServiceImpl} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
class GRpcJobServiceImplSpec extends Specification {

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

    def "Can resolve job specification"() {
        def jobSpecification = createJobSpecification()
        def jobPersistenceService = Mock(JobPersistenceService)
        def jobSpecificationService = Mock(JobSpecificationService)
        def registry = Mock(MeterRegistry)
        def service = new GRpcJobServiceImpl(jobPersistenceService, jobSpecificationService, registry)
        StreamObserver<JobSpecificationResponse> responseObserver = Mock()

        when:
        service.resolveJobSpecification(createJobSpecificationRequest(), responseObserver)

        then:
        0 * jobSpecificationService.resolveJobSpecification(
                _ as String,
                _ as com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria
        )
        // TODO: Fix once reimplemented
//        1 * jobSpecificationService.resolveJobSpecification(
//                id as String,
//                _ as com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria
//        ) >> jobSpecification
//        1 * responseObserver.onNext(_ as JobSpecificationResponse)
//        1 * responseObserver.onCompleted()
    }

    def "Can't resolve job specification"() {
        def jobPersistenceService = Mock(JobPersistenceService)
        def jobSpecificationService = Mock(JobSpecificationService)
        def registry = Mock(MeterRegistry)
        def service = new GRpcJobServiceImpl(jobPersistenceService, jobSpecificationService, registry)
        StreamObserver<JobSpecificationResponse> responseObserver = Mock()

        when:
        service.resolveJobSpecification(createJobSpecificationRequest(), responseObserver)

        then:
        0 * jobSpecificationService.resolveJobSpecification(
                _ as String,
                _ as com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria
        )
        // TODO: Fix once reimplemented
//        1 * jobSpecificationService.resolveJobSpecification(
//                id as String,
//                _ as com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria
//        ) >> {
//            throw new RuntimeException(UUID.randomUUID().toString())
//        }
//        1 * responseObserver.onNext(_ as JobSpecificationResponse)
//        1 * responseObserver.onCompleted()
    }

    JobSpecificationRequest createJobSpecificationRequest() {
        return JobSpecificationRequest.newBuilder().setId(id).build()
    }

    ReserveJobIdRequest createReserveJobIdRequest() {
        def jobMetadataProto = JobMetadata
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

        def executionResourceCriteriaProto = ExecutionResourceCriteria.newBuilder()
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
        ).addAllApplicationIds(applicationIds)
                .build()

        def agentConfigProto = AgentConfig
                .newBuilder()
                .setIsInteractive(interactive)
                .setJobDirectoryLocation(jobDirectoryLocation)
                .build()

        return ReserveJobIdRequest
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
}
