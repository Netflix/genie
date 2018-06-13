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

import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException
import com.netflix.genie.agent.execution.exceptions.JobIdUnavailableException
import com.netflix.genie.agent.execution.exceptions.JobReservationException
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.internal.exceptions.GenieConversionException
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata
import com.netflix.genie.common.internal.dto.v4.AgentJobRequest
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.common.internal.dto.v4.converters.JobServiceProtoConverter
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import com.netflix.genie.proto.*
import com.netflix.genie.test.categories.UnitTest
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.junit.Rule
import org.junit.experimental.categories.Category
import spock.lang.Specification
import spock.lang.Unroll

@Category(UnitTest.class)
class GRpcAgentJobServiceImplSpec extends Specification {

    @Rule
    GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()
    String id
    JobServiceProtoConverter protoConverter
    JobServiceGrpc.JobServiceFutureStub client
    AgentJobService service

    // Set during setup of individual tests as needed.
    ReserveJobIdResponse reserveJobIdResponse
    JobSpecificationResponse jobSpecificationResponse
    ClaimJobResponse claimJobResponse
    ChangeJobStatusResponse changeJobStatusResponse

    void setup() {
        this.grpcServerRule.getServiceRegistry().addService(new TestService())
        this.id = UUID.randomUUID().toString()
        this.protoConverter = Mock(JobServiceProtoConverter)
        this.client = JobServiceGrpc.newFutureStub(grpcServerRule.getChannel())
        this.service = new GRpcAgentJobServiceImpl(client, protoConverter)
    }

    def "Reserve job id -- successful"() {
        AgentJobRequest agentJobRequest = Mock()
        AgentClientMetadata agentClientMetadata = Mock()
        ReserveJobIdRequest request = ReserveJobIdRequest.getDefaultInstance()
        this.reserveJobIdResponse = ReserveJobIdResponse.newBuilder().setId(id).build()

        when:
        String reservedId = service.reserveJobId(agentJobRequest, agentClientMetadata)

        then:
        1 * protoConverter.toProtoReserveJobIdRequest(agentJobRequest, agentClientMetadata) >> request

        expect:
        reservedId == id
    }

    @Unroll
    def "Reserve job id -- handle error #errorType"() {
        AgentJobRequest agentJobRequest = Mock()
        AgentClientMetadata agentClientMetadata = Mock()
        ReserveJobIdRequest request = ReserveJobIdRequest.getDefaultInstance()
        this.reserveJobIdResponse = ReserveJobIdResponse.newBuilder().setError(
                ReserveJobIdError.newBuilder()
                    .setType(errorType)
                    .setMessage("error message")
        ).build()

        when:
        service.reserveJobId(agentJobRequest, agentClientMetadata)

        then:
        1 * protoConverter.toProtoReserveJobIdRequest(agentJobRequest, agentClientMetadata) >> request

        thrown(expectedException)

        where:
        errorType                               | expectedException
        ReserveJobIdError.Type.ID_NOT_AVAILABLE | JobIdUnavailableException
        ReserveJobIdError.Type.SERVER_ERROR     | JobReservationException
        ReserveJobIdError.Type.INVALID_REQUEST  | JobReservationException
        ReserveJobIdError.Type.UNKNOWN          | GenieRuntimeException
    }

    def "Reserve job id -- invalid response"() {
        AgentJobRequest agentJobRequest = Mock()
        AgentClientMetadata agentClientMetadata = Mock()
        ReserveJobIdRequest request = ReserveJobIdRequest.getDefaultInstance()
        this.reserveJobIdResponse = ReserveJobIdResponse.getDefaultInstance()

        when:
        service.reserveJobId(agentJobRequest, agentClientMetadata)

        then:
        1 * protoConverter.toProtoReserveJobIdRequest(agentJobRequest, agentClientMetadata) >> request

        thrown(GenieRuntimeException)
    }

    def "Reserve job id -- conversion error"() {
        AgentJobRequest agentJobRequest = Mock()
        AgentClientMetadata agentClientMetadata = Mock()
        this.reserveJobIdResponse = ReserveJobIdResponse.getDefaultInstance()
        Exception exception = new GenieConversionException("...")

        when:
        service.reserveJobId(agentJobRequest, agentClientMetadata)

        then:
        1 * protoConverter.toProtoReserveJobIdRequest(agentJobRequest, agentClientMetadata) >> {throw exception}

        thrown(JobReservationException)
    }

    def "Resolve job specification -- successful"() {
        com.netflix.genie.proto.JobSpecification jobSpecificationProto = com.netflix.genie.proto.JobSpecification.getDefaultInstance()
        this.jobSpecificationResponse = JobSpecificationResponse.newBuilder().setSpecification(jobSpecificationProto).build()
        JobSpecificationRequest request = JobSpecificationRequest.getDefaultInstance()
        JobSpecification jobSpecification = Mock()

        when:
        JobSpecification resolvedJobSpecification = service.resolveJobSpecification(id)

        then:
        1 * protoConverter.toProtoJobSpecificationRequest(id) >> request
        1 * protoConverter.toJobSpecificationDTO(jobSpecificationProto) >> jobSpecification

        expect:
        resolvedJobSpecification == jobSpecification
    }

    @Unroll
    def "Resolve job specification -- handle error #errorType"() {
        this.jobSpecificationResponse = JobSpecificationResponse.newBuilder()
            .setError(
                JobSpecificationError.newBuilder()
                    .setType(errorType)
                    .setMessage("error message")
            )
            .build()
        JobSpecificationRequest request = JobSpecificationRequest.getDefaultInstance()

        when:
        service.resolveJobSpecification(id)

        then:
        1 * protoConverter.toProtoJobSpecificationRequest(id) >> request

        thrown(expectedException)

        where:
        errorType                                       | expectedException
        JobSpecificationError.Type.NO_APPLICATION_FOUND | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_CLUSTER_FOUND     | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_JOB_FOUND         | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_COMMAND_FOUND     | JobSpecificationResolutionException
        JobSpecificationError.Type.UNKNOWN              | GenieRuntimeException
    }

    def "Resolve job specification -- invalid response"() {
        com.netflix.genie.proto.JobSpecification jobSpecificationProto = com.netflix.genie.proto.JobSpecification.getDefaultInstance()
        JobSpecificationRequest request = JobSpecificationRequest.getDefaultInstance()
        this.jobSpecificationResponse = JobSpecificationResponse.getDefaultInstance()

        when:
        service.resolveJobSpecification(id)

        then:
        1 * protoConverter.toProtoJobSpecificationRequest(id) >> request

        thrown(GenieRuntimeException)
    }

    def "Get job specification -- successful"() {
        com.netflix.genie.proto.JobSpecification jobSpecificationProto = com.netflix.genie.proto.JobSpecification.getDefaultInstance()
        this.jobSpecificationResponse = JobSpecificationResponse.newBuilder().setSpecification(jobSpecificationProto).build()
        JobSpecificationRequest request = JobSpecificationRequest.getDefaultInstance()
        JobSpecification jobSpecification = Mock()

        when:
        JobSpecification resolvedJobSpecification = service.getJobSpecification(id)

        then:
        1 * protoConverter.toProtoJobSpecificationRequest(id) >> request
        1 * protoConverter.toJobSpecificationDTO(jobSpecificationProto) >> jobSpecification

        expect:
        resolvedJobSpecification == jobSpecification
    }

    @Unroll
    def "Get job specification -- handle error #errorType"() {
        this.jobSpecificationResponse = JobSpecificationResponse.newBuilder()
                .setError(
                JobSpecificationError.newBuilder()
                        .setType(errorType)
                        .setMessage("error message")
        )
                .build()
        JobSpecificationRequest request = JobSpecificationRequest.getDefaultInstance()

        when:
        service.getJobSpecification(id)

        then:
        1 * protoConverter.toProtoJobSpecificationRequest(id) >> request

        thrown(expectedException)

        where:
        errorType                                       | expectedException
        JobSpecificationError.Type.NO_APPLICATION_FOUND | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_CLUSTER_FOUND     | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_JOB_FOUND         | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_COMMAND_FOUND     | JobSpecificationResolutionException
        JobSpecificationError.Type.UNKNOWN              | GenieRuntimeException
    }

    def "Get job specification -- invalid response"() {
        com.netflix.genie.proto.JobSpecification jobSpecificationProto = com.netflix.genie.proto.JobSpecification.getDefaultInstance()
        JobSpecificationRequest request = JobSpecificationRequest.getDefaultInstance()
        this.jobSpecificationResponse = JobSpecificationResponse.getDefaultInstance()

        when:
        service.getJobSpecification(id)

        then:
        1 * protoConverter.toProtoJobSpecificationRequest(id) >> request

        thrown(GenieRuntimeException)
    }

    def "Resolve job specification dry run -- successful"() {
        com.netflix.genie.proto.JobSpecification jobSpecificationProto = com.netflix.genie.proto.JobSpecification.getDefaultInstance()
        this.jobSpecificationResponse = JobSpecificationResponse.newBuilder().setSpecification(jobSpecificationProto).build()
        DryRunJobSpecificationRequest request = DryRunJobSpecificationRequest.getDefaultInstance()
        JobSpecification jobSpecification = Mock()
        AgentJobRequest agentJobRequest = Mock()

        when:
        JobSpecification resolvedJobSpecification = service.resolveJobSpecificationDryRun(agentJobRequest)

        then:
        1 * protoConverter.toProtoDryRunJobSpecificationRequest(agentJobRequest) >> request
        1 * protoConverter.toJobSpecificationDTO(jobSpecificationProto) >> jobSpecification

        expect:
        resolvedJobSpecification == jobSpecification
    }

    def "Resolve job specification dry run -- conversion error"() {
        com.netflix.genie.proto.JobSpecification jobSpecificationProto = com.netflix.genie.proto.JobSpecification.getDefaultInstance()
        this.jobSpecificationResponse = JobSpecificationResponse.newBuilder().setSpecification(jobSpecificationProto).build()
        AgentJobRequest agentJobRequest = Mock()
        Exception exception = new GenieConversionException("...")

        when:
        service.resolveJobSpecificationDryRun(agentJobRequest)

        then:
        1 * protoConverter.toProtoDryRunJobSpecificationRequest(agentJobRequest) >> { throw exception }

        thrown(JobSpecificationResolutionException)
    }

    @Unroll
    def "Resolve job specification dry run -- handle error #errorType"() {
        this.jobSpecificationResponse = JobSpecificationResponse.newBuilder()
                .setError(
                JobSpecificationError.newBuilder()
                        .setType(errorType)
                        .setMessage("error message")
        )
                .build()
        DryRunJobSpecificationRequest request = DryRunJobSpecificationRequest.getDefaultInstance()
        AgentJobRequest agentJobRequest = Mock()

        when:
        service.resolveJobSpecificationDryRun(agentJobRequest)

        then:
        1 * protoConverter.toProtoDryRunJobSpecificationRequest(agentJobRequest) >> request

        thrown(expectedException)

        where:
        errorType                                       | expectedException
        JobSpecificationError.Type.NO_APPLICATION_FOUND | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_CLUSTER_FOUND     | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_JOB_FOUND         | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_COMMAND_FOUND     | JobSpecificationResolutionException
        JobSpecificationError.Type.UNKNOWN              | GenieRuntimeException
    }

    def "Resolve job specification dry run -- invalid response"() {
        DryRunJobSpecificationRequest request = DryRunJobSpecificationRequest.getDefaultInstance()
        this.jobSpecificationResponse = JobSpecificationResponse.getDefaultInstance()
        AgentJobRequest agentJobRequest = Mock()

        when:
        service.resolveJobSpecificationDryRun(agentJobRequest)

        then:
        1 * protoConverter.toProtoDryRunJobSpecificationRequest(agentJobRequest) >> request

        thrown(GenieRuntimeException)
    }

    def "Claim job -- successful"() {
        this.claimJobResponse = ClaimJobResponse.newBuilder().setSuccessful(true).build()
        ClaimJobRequest request = ClaimJobRequest.getDefaultInstance()
        AgentClientMetadata agentClientMetadata = Mock()

        when:
        service.claimJob(id, agentClientMetadata)

        then:
        1 * protoConverter.toProtoClaimJobRequest(id, agentClientMetadata) >> request
    }

    @Unroll
    def "Claim job -- handle error #errorType"() {
        this.claimJobResponse = ClaimJobResponse.newBuilder()
                .setError(
                ClaimJobError.newBuilder()
                        .setType(errorType)
                        .setMessage("error message")
        )
        .build()
        AgentClientMetadata agentClientMetadata = Mock()
        ClaimJobRequest request = ClaimJobRequest.getDefaultInstance()

        when:
        service.claimJob(id, agentClientMetadata)

        then:
        1 * protoConverter.toProtoClaimJobRequest(id, agentClientMetadata) >> request

        thrown(expectedException)

        where:
        errorType                          | expectedException
        ClaimJobError.Type.NO_SUCH_JOB     | JobReservationException
        ClaimJobError.Type.INVALID_STATUS  | JobReservationException
        ClaimJobError.Type.INVALID_REQUEST | JobReservationException
        ClaimJobError.Type.ALREADY_CLAIMED | JobReservationException
        ClaimJobError.Type.UNKNOWN         | GenieRuntimeException
    }

    def "Claim job -- invalid response"() {
        this.claimJobResponse = ClaimJobResponse.getDefaultInstance()
        AgentClientMetadata agentClientMetadata = Mock()
        ClaimJobRequest request = ClaimJobRequest.getDefaultInstance()

        when:
        service.claimJob(id, agentClientMetadata)

        then:
        1 * protoConverter.toProtoClaimJobRequest(id, agentClientMetadata) >> request

        thrown(GenieRuntimeException)
    }

    def "Change job status -- successful"() {
        this.changeJobStatusResponse = ChangeJobStatusResponse.newBuilder().setSuccessful(true).build()
        ChangeJobStatusRequest request = ChangeJobStatusRequest.getDefaultInstance()
        JobStatus currentStatus = JobStatus.INIT
        JobStatus newStatus = JobStatus.RUNNING

        when:
        service.changeJobStatus(id, currentStatus, newStatus, null)

        then:
        1 * protoConverter.toProtoChangeJobStatusRequest(id, currentStatus, newStatus, _ as String) >> request
    }

    @Unroll
    def "Change job status -- handle error #errorType"() {
        this.changeJobStatusResponse = ChangeJobStatusResponse.newBuilder()
                .setSuccessful(false)
                .setError(
                    ChangeJobStatusError.newBuilder()
                        .setType(errorType)
                        .setMessage("error message")
                )
                .build()
        JobStatus currentStatus = JobStatus.INIT
        JobStatus newStatus = JobStatus.RUNNING
        String message = "..."
        ChangeJobStatusRequest request = ChangeJobStatusRequest.getDefaultInstance()

        when:
        service.changeJobStatus(id, currentStatus, newStatus, message)

        then:
        1 * protoConverter.toProtoChangeJobStatusRequest(id, currentStatus, newStatus, message) >> request

        thrown(expectedException)

        where:
        errorType                                          | expectedException
        ChangeJobStatusError.Type.INVALID_REQUEST          | ChangeJobStatusException
        ChangeJobStatusError.Type.NO_SUCH_JOB              | ChangeJobStatusException
        ChangeJobStatusError.Type.INCORRECT_CURRENT_STATUS | ChangeJobStatusException
        ChangeJobStatusError.Type.UNKNOWN                  | GenieRuntimeException
    }

    def "Change job status -- invalid response"() {
        this.changeJobStatusResponse = ChangeJobStatusResponse.getDefaultInstance()
        JobStatus currentStatus = JobStatus.INIT
        JobStatus newStatus = JobStatus.RUNNING
        String message = "..."
        ChangeJobStatusRequest request = ChangeJobStatusRequest.getDefaultInstance()

        when:
        service.changeJobStatus(id, currentStatus, newStatus, message)

        then:
        1 * protoConverter.toProtoChangeJobStatusRequest(id, currentStatus, newStatus, message) >> request

        thrown(GenieRuntimeException)
    }

    private class TestService extends JobServiceGrpc.JobServiceImplBase {
        @Override
        void reserveJobId(
                final ReserveJobIdRequest request,
                final StreamObserver<ReserveJobIdResponse> responseObserver
        ) {
            sendResponse(responseObserver, reserveJobIdResponse)
        }

        @Override
        void resolveJobSpecification(
                final JobSpecificationRequest request,
                final StreamObserver<JobSpecificationResponse> responseObserver
        ) {
            sendResponse(responseObserver, jobSpecificationResponse)
        }

        @Override
        void getJobSpecification(
                final JobSpecificationRequest request,
                final StreamObserver<JobSpecificationResponse> responseObserver
        ) {
            sendResponse(responseObserver, jobSpecificationResponse)
        }

        @Override
        void resolveJobSpecificationDryRun(
                final DryRunJobSpecificationRequest request,
                final StreamObserver<JobSpecificationResponse> responseObserver
        ) {
            sendResponse(responseObserver, jobSpecificationResponse)
        }

        @Override
        void claimJob(
                final ClaimJobRequest request,
                final StreamObserver<ClaimJobResponse> responseObserver
        ) {
            sendResponse(responseObserver, claimJobResponse)
        }

        @Override
        void changeJobStatus(
                final ChangeJobStatusRequest request,
                final StreamObserver<ChangeJobStatusResponse> responseObserver
        ) {
            sendResponse(responseObserver, changeJobStatusResponse)
        }

        private <ResponseType> void  sendResponse(
                StreamObserver<ResponseType> observer,
                ResponseType response
        ) {
            assert response != null
            observer.onNext(response)
            observer.onCompleted()
        }
    }
}
