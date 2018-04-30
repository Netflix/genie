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

package com.netflix.genie.agent.execution.services.impl.grpc

import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.proto.*
import com.netflix.genie.test.categories.UnitTest
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.junit.Rule
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class GRpcAgentJobServiceImplSpec extends Specification {

    AgentJobService service
    JobServiceGrpc.JobServiceFutureStub client

    @Rule
    GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()
    JobSpecificationRequest capturedResolveRequest
    JobSpecificationRequest capturedGetRequest
    Exception serverException
    Throwable serverError
    JobSpecificationResponse serverResponse

    void setup() {
        this.grpcServerRule.getServiceRegistry().addService(new JobServiceGrpc.JobServiceImplBase() {
            @Override
            void resolveJobSpecification(JobSpecificationRequest request, StreamObserver<JobSpecificationResponse> responseObserver) {
                capturedResolveRequest = request
                if (serverException != null) {
                    throw serverException
                } else if (serverError != null) {
                    responseObserver.onError(serverError)
                } else if (serverResponse != null) {
                    responseObserver.onNext(serverResponse)
                    responseObserver.onCompleted()
                }
            }

            @Override
            void getJobSpecification(JobSpecificationRequest request, StreamObserver<JobSpecificationResponse> responseObserver) {
                capturedGetRequest = request
                if (serverException != null) {
                    throw serverException
                } else if (serverError != null) {
                    responseObserver.onError(serverError)
                } else if (serverResponse != null) {
                    responseObserver.onNext(serverResponse)
                    responseObserver.onCompleted()
                }
            }
        })
        this.client = JobServiceGrpc.newFutureStub(grpcServerRule.getChannel())
        this.capturedResolveRequest = null
        this.serverException = null
        this.serverError = null
        this.serverResponse = null
        this.service = new GRpcAgentJobServiceImpl(client)
    }

    void cleanup() {
    }

    def "ResolveJobSpecification"() {
        setup:
//        AgentJobRequest request = new AgentJobRequest.Builder(
//                new JobMetadata.Builder("job x", "jdoe").build(),
//                new ExecutionResourceCriteria(
//                        Lists.newArrayList(),
//                        new Criterion.Builder().withName("foo").build(),
//                        Lists.newArrayList()
//                ),
//                "/tmp/jobs"
//        ).build()
        def id = UUID.randomUUID().toString()

        serverResponse = JobSpecificationResponse.newBuilder()
                .setSpecification(com.netflix.genie.proto.JobSpecification.newBuilder().build())
                .build()

        when:
        JobSpecification response = service.resolveJobSpecification(id)

        then:
        response != null
    }

    def "ResolveJobSpecification server error"() {
        setup:
//        AgentJobRequest request = new AgentJobRequest.Builder(
//                new JobMetadata.Builder("job x", "jdoe").build(),
//                new ExecutionResourceCriteria(
//                        Lists.newArrayList(),
//                        new Criterion.Builder().withName("foo").build(),
//                        Lists.newArrayList()
//                ),
//                "/tmp/jobs"
//        ).build()
        def id = UUID.randomUUID().toString()

        serverResponse = JobSpecificationResponse.newBuilder()
                .setError(
                JobSpecificationError.newBuilder()
                        .setMessage("error")
                        .setType(JobSpecificationError.Type.NO_CLUSTER_FOUND)
                        .build()
        ).build()

        when:
        service.resolveJobSpecification(id)

        then:
        thrown(JobSpecificationResolutionException)
    }

    def "ResolveJobSpecification server no response"() {
        setup:
//        AgentJobRequest request = new AgentJobRequest.Builder(
//                new JobMetadata.Builder("job x", "jdoe").build(),
//                new ExecutionResourceCriteria(
//                        Lists.newArrayList(),
//                        new Criterion.Builder().withName("foo").build(),
//                        Lists.newArrayList()
//                ),
//                "/tmp/jobs"
//        ).build()
        def id = UUID.randomUUID().toString()

        serverResponse = JobSpecificationResponse.newBuilder().build()

        when:
        service.resolveJobSpecification(id)

        then:
        thrown(JobSpecificationResolutionException)
    }

    def "GetJobSpecification"() {

        setup:
        String id = "123456789"

        serverResponse = JobSpecificationResponse.newBuilder()
                .setSpecification(com.netflix.genie.proto.JobSpecification.newBuilder().build())
                .build()

        when:
        JobSpecification response = service.getJobSpecification(id)

        then:
        response != null

    }
}
