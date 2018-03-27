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
import com.netflix.genie.agent.execution.services.AgentJobSpecificationService
import com.netflix.genie.common.dto.v4.AgentJobRequest
import com.netflix.genie.common.dto.v4.Criterion
import com.netflix.genie.common.dto.v4.ExecutionResourceCriteria
import com.netflix.genie.common.dto.v4.JobMetadata
import com.netflix.genie.common.dto.v4.JobSpecification
import com.netflix.genie.proto.GetJobSpecificationRequest
import com.netflix.genie.proto.JobSpecificationResponse
import com.netflix.genie.proto.JobSpecificationServiceError
import com.netflix.genie.proto.JobSpecificationServiceGrpc
import com.netflix.genie.proto.ResolveJobSpecificationRequest
import com.netflix.genie.test.categories.UnitTest
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.assertj.core.util.Lists
import org.junit.Rule
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class GRpcAgentJobSpecificationServiceImplSpec extends Specification {

    AgentJobSpecificationService service
    JobSpecificationServiceGrpc.JobSpecificationServiceFutureStub client

    @Rule
    GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()
    ResolveJobSpecificationRequest capturedResolveRequest
    GetJobSpecificationRequest capturedGetRequest
    Exception serverException
    Throwable serverError
    JobSpecificationResponse serverResponse

    void setup() {
        this.grpcServerRule.getServiceRegistry().addService(new JobSpecificationServiceGrpc.JobSpecificationServiceImplBase() {
            @Override
            void resolveJobSpecification(ResolveJobSpecificationRequest request, StreamObserver<JobSpecificationResponse> responseObserver) {
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
            void getJobSpecification(GetJobSpecificationRequest request, StreamObserver<JobSpecificationResponse> responseObserver) {
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
        this.client = JobSpecificationServiceGrpc.newFutureStub(grpcServerRule.getChannel())
        this.capturedResolveRequest = null
        this.serverException = null
        this.serverError = null
        this.serverResponse = null
        this.service = new GRpcAgentJobSpecificationServiceImpl(client)
    }

    void cleanup() {
    }

    def "ResolveJobSpecification"() {
        setup:
        AgentJobRequest request = new AgentJobRequest.Builder(
                new JobMetadata.Builder("job x", "jdoe").build(),
                new ExecutionResourceCriteria(
                        Lists.newArrayList(),
                        new Criterion.Builder().withName("foo").build(),
                        Lists.newArrayList()
                ),
                "/tmp/jobs"
        ).build()

        serverResponse = JobSpecificationResponse.newBuilder()
            .setSpecification(
                new com.netflix.genie.proto.JobSpecification.Builder()
                .build()
            )
            .build()

        when:
        JobSpecification response = service.resolveJobSpecification(request)

        then:
        response != null
    }

    def "ResolveJobSpecification server error"() {
        setup:
        AgentJobRequest request = new AgentJobRequest.Builder(
                new JobMetadata.Builder("job x", "jdoe").build(),
                new ExecutionResourceCriteria(
                        Lists.newArrayList(),
                        new Criterion.Builder().withName("foo").build(),
                        Lists.newArrayList()
                ),
                "/tmp/jobs"
        ).build()

        serverResponse = JobSpecificationResponse.newBuilder()
            .setError(
                JobSpecificationServiceError.newBuilder()
                   .setMessage("error")
                    .setType(JobSpecificationServiceError.Type.NO_CLUSTER_FOUND)
                    .build()
            ).build()

        when:
        service.resolveJobSpecification(request)

        then:
        thrown(JobSpecificationResolutionException)
    }

    def "ResolveJobSpecification server no response"() {
        setup:
        AgentJobRequest request = new AgentJobRequest.Builder(
                new JobMetadata.Builder("job x", "jdoe").build(),
                new ExecutionResourceCriteria(
                        Lists.newArrayList(),
                        new Criterion.Builder().withName("foo").build(),
                        Lists.newArrayList()
                ),
                "/tmp/jobs"
        ).build()

        serverResponse = JobSpecificationResponse.newBuilder().build()

        when:
        service.resolveJobSpecification(request)

        then:
        thrown(JobSpecificationResolutionException)
    }

    def "GetJobSpecification"() {

        setup:
        String id = "123456789"

        serverResponse = JobSpecificationResponse.newBuilder()
                .setSpecification(
                new com.netflix.genie.proto.JobSpecification.Builder()
                        .build()
        ).build()

        when:
        JobSpecification response = service.getJobSpecification(id)

        then:
        response != null

    }
}
