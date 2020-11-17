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

import com.netflix.genie.agent.execution.exceptions.ChangeJobArchiveStatusException
import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException
import com.netflix.genie.agent.execution.exceptions.ConfigureException
import com.netflix.genie.agent.execution.exceptions.GetJobStatusException
import com.netflix.genie.agent.execution.exceptions.HandshakeException
import com.netflix.genie.agent.execution.exceptions.JobIdUnavailableException
import com.netflix.genie.agent.execution.exceptions.JobReservationException
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.dtos.v4.converters.JobServiceProtoConverter
import com.netflix.genie.common.internal.exceptions.checked.GenieConversionException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import com.netflix.genie.proto.ChangeJobArchiveStatusRequest
import com.netflix.genie.proto.ChangeJobArchiveStatusResponse
import com.netflix.genie.proto.ChangeJobStatusError
import com.netflix.genie.proto.ChangeJobStatusRequest
import com.netflix.genie.proto.ChangeJobStatusResponse
import com.netflix.genie.proto.ClaimJobError
import com.netflix.genie.proto.ClaimJobRequest
import com.netflix.genie.proto.ClaimJobResponse
import com.netflix.genie.proto.ConfigureRequest
import com.netflix.genie.proto.ConfigureResponse
import com.netflix.genie.proto.DryRunJobSpecificationRequest
import com.netflix.genie.proto.GetJobStatusRequest
import com.netflix.genie.proto.GetJobStatusResponse
import com.netflix.genie.proto.HandshakeRequest
import com.netflix.genie.proto.HandshakeResponse
import com.netflix.genie.proto.JobServiceGrpc
import com.netflix.genie.proto.JobSpecificationError
import com.netflix.genie.proto.JobSpecificationRequest
import com.netflix.genie.proto.JobSpecificationResponse
import com.netflix.genie.proto.ReserveJobIdError
import com.netflix.genie.proto.ReserveJobIdRequest
import com.netflix.genie.proto.ReserveJobIdResponse
import io.grpc.Status
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.junit.Rule
import spock.lang.Specification
import spock.lang.Unroll

class GRpcAgentJobServiceImplSpec extends Specification {

    @Rule
    GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()
    String id
    JobServiceProtoConverter protoConverter
    JobServiceGrpc.JobServiceFutureStub client
    AgentJobService service

    // Set during setup of individual tests as needed.
    HandshakeResponse handshakeResponse
    ConfigureResponse configureResponse
    ReserveJobIdResponse reserveJobIdResponse
    JobSpecificationResponse jobSpecificationResponse
    ClaimJobResponse claimJobResponse
    ChangeJobStatusResponse changeJobStatusResponse
    GetJobStatusResponse getJobStatusResponse
    ChangeJobArchiveStatusResponse changeJobArchiveStatusResponse
    Throwable serverError

    void setup() {
        this.serverError = null
        this.grpcServerRule.getServiceRegistry().addService(new TestService())
        this.id = UUID.randomUUID().toString()
        this.protoConverter = Mock(JobServiceProtoConverter)
        this.client = JobServiceGrpc.newFutureStub(grpcServerRule.getChannel())
        this.service = new GRpcAgentJobServiceImpl(client, protoConverter)
    }

    def "Handshake -- successful"() {
        AgentClientMetadata agentClientMetadata = Mock()
        HandshakeRequest request = HandshakeRequest.getDefaultInstance()
        this.handshakeResponse = HandshakeResponse.newBuilder()
            .setType(HandshakeResponse.Type.ALLOWED)
            .build()

        when:
        service.handshake(agentClientMetadata)

        then:
        1 * protoConverter.toHandshakeRequestProto(agentClientMetadata) >> request
    }

    def "Handshake -- rejected"() {
        AgentClientMetadata agentClientMetadata = Mock()
        HandshakeRequest request = HandshakeRequest.getDefaultInstance()
        this.handshakeResponse = HandshakeResponse.newBuilder()
            .setType(HandshakeResponse.Type.REJECTED)
            .setMessage("Thou shall not pass")
            .build()

        when:
        service.handshake(agentClientMetadata)

        then:
        1 * protoConverter.toHandshakeRequestProto(agentClientMetadata) >> request
        thrown(HandshakeException)
    }

    @Unroll
    def "Handshake -- error #error"(HandshakeResponse.Type error) {
        AgentClientMetadata agentClientMetadata = Mock()
        HandshakeRequest request = HandshakeRequest.getDefaultInstance()
        this.handshakeResponse = HandshakeResponse.newBuilder()
            .setType(error)
            .setMessage("Some server error")
            .build()

        when:
        service.handshake(agentClientMetadata)

        then:
        1 * protoConverter.toHandshakeRequestProto(agentClientMetadata) >> request
        Exception e = thrown(GenieRuntimeException)
        e.getMessage().contains(error.name())

        where:
        error                                  | _
        HandshakeResponse.Type.UNKNOWN         | _
        HandshakeResponse.Type.SERVER_ERROR    | _
        HandshakeResponse.Type.INVALID_REQUEST | _
    }

    def "Configure -- successful"() {
        AgentClientMetadata agentClientMetadata = Mock()
        ConfigureRequest request = ConfigureRequest.getDefaultInstance()
        this.configureResponse = ConfigureResponse.newBuilder()
            .build()

        when:
        Map<String, String> agentProperties = service.configure(agentClientMetadata)

        then:
        1 * protoConverter.toConfigureRequestProto(agentClientMetadata) >> request
        agentProperties != null
    }

    def "Configure -- conversion error"() {
        AgentClientMetadata agentClientMetadata = Mock()
        ConfigureRequest request = ConfigureRequest.getDefaultInstance()
        Exception exception = new GenieConversionException("...")

        when:
        service.configure(agentClientMetadata)

        then:
        1 * protoConverter.toConfigureRequestProto(agentClientMetadata) >> { throw exception }
        thrown(ConfigureException)
    }

    def "Reserve job id -- successful"() {
        AgentJobRequest agentJobRequest = Mock()
        AgentClientMetadata agentClientMetadata = Mock()
        ReserveJobIdRequest request = ReserveJobIdRequest.getDefaultInstance()
        this.reserveJobIdResponse = ReserveJobIdResponse.newBuilder().setId(id).build()

        when:
        String reservedId = service.reserveJobId(agentJobRequest, agentClientMetadata)

        then:
        1 * protoConverter.toReserveJobIdRequestProto(agentJobRequest, agentClientMetadata) >> request

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
        1 * protoConverter.toReserveJobIdRequestProto(agentJobRequest, agentClientMetadata) >> request
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
        1 * protoConverter.toReserveJobIdRequestProto(agentJobRequest, agentClientMetadata) >> request

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
        1 * protoConverter.toReserveJobIdRequestProto(agentJobRequest, agentClientMetadata) >> { throw exception }

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
        1 * protoConverter.toJobSpecificationRequestProto(id) >> request
        1 * protoConverter.toJobSpecificationDto(jobSpecificationProto) >> jobSpecification

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
        1 * protoConverter.toJobSpecificationRequestProto(id) >> request

        thrown(expectedException)

        where:
        errorType                                       | expectedException
        JobSpecificationError.Type.NO_APPLICATION_FOUND | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_CLUSTER_FOUND     | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_JOB_FOUND         | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_COMMAND_FOUND     | JobSpecificationResolutionException
        JobSpecificationError.Type.RESOLUTION_FAILED    | JobSpecificationResolutionException
        JobSpecificationError.Type.RUNTIME_ERROR        | GenieRuntimeException
        JobSpecificationError.Type.UNKNOWN              | GenieRuntimeException
    }

    def "Resolve job specification -- invalid response"() {
        com.netflix.genie.proto.JobSpecification jobSpecificationProto = com.netflix.genie.proto.JobSpecification.getDefaultInstance()
        JobSpecificationRequest request = JobSpecificationRequest.getDefaultInstance()
        this.jobSpecificationResponse = JobSpecificationResponse.getDefaultInstance()

        when:
        service.resolveJobSpecification(id)

        then:
        1 * protoConverter.toJobSpecificationRequestProto(id) >> request

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
        1 * protoConverter.toJobSpecificationRequestProto(id) >> request
        1 * protoConverter.toJobSpecificationDto(jobSpecificationProto) >> jobSpecification

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
        1 * protoConverter.toJobSpecificationRequestProto(id) >> request

        thrown(expectedException)

        where:
        errorType                                       | expectedException
        JobSpecificationError.Type.NO_APPLICATION_FOUND | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_CLUSTER_FOUND     | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_JOB_FOUND         | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_COMMAND_FOUND     | JobSpecificationResolutionException
        JobSpecificationError.Type.RESOLUTION_FAILED    | JobSpecificationResolutionException
        JobSpecificationError.Type.UNKNOWN              | GenieRuntimeException
    }

    def "Get job specification -- invalid response"() {
        com.netflix.genie.proto.JobSpecification jobSpecificationProto = com.netflix.genie.proto.JobSpecification.getDefaultInstance()
        JobSpecificationRequest request = JobSpecificationRequest.getDefaultInstance()
        this.jobSpecificationResponse = JobSpecificationResponse.getDefaultInstance()

        when:
        service.getJobSpecification(id)

        then:
        1 * protoConverter.toJobSpecificationRequestProto(id) >> request

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
        1 * protoConverter.toDryRunJobSpecificationRequestProto(agentJobRequest) >> request
        1 * protoConverter.toJobSpecificationDto(jobSpecificationProto) >> jobSpecification

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
        1 * protoConverter.toDryRunJobSpecificationRequestProto(agentJobRequest) >> { throw exception }

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
        1 * protoConverter.toDryRunJobSpecificationRequestProto(agentJobRequest) >> request

        thrown(expectedException)

        where:
        errorType                                       | expectedException
        JobSpecificationError.Type.NO_APPLICATION_FOUND | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_CLUSTER_FOUND     | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_JOB_FOUND         | JobSpecificationResolutionException
        JobSpecificationError.Type.NO_COMMAND_FOUND     | JobSpecificationResolutionException
        JobSpecificationError.Type.RESOLUTION_FAILED    | JobSpecificationResolutionException
        JobSpecificationError.Type.UNKNOWN              | GenieRuntimeException
    }

    def "Resolve job specification dry run -- invalid response"() {
        DryRunJobSpecificationRequest request = DryRunJobSpecificationRequest.getDefaultInstance()
        this.jobSpecificationResponse = JobSpecificationResponse.getDefaultInstance()
        AgentJobRequest agentJobRequest = Mock()

        when:
        service.resolveJobSpecificationDryRun(agentJobRequest)

        then:
        1 * protoConverter.toDryRunJobSpecificationRequestProto(agentJobRequest) >> request

        thrown(GenieRuntimeException)
    }

    def "Claim job -- successful"() {
        this.claimJobResponse = ClaimJobResponse.newBuilder().setSuccessful(true).build()
        ClaimJobRequest request = ClaimJobRequest.getDefaultInstance()
        AgentClientMetadata agentClientMetadata = Mock()

        when:
        service.claimJob(id, agentClientMetadata)

        then:
        1 * protoConverter.toClaimJobRequestProto(id, agentClientMetadata) >> request
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
        1 * protoConverter.toClaimJobRequestProto(id, agentClientMetadata) >> request

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
        1 * protoConverter.toClaimJobRequestProto(id, agentClientMetadata) >> request

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
        1 * protoConverter.toChangeJobStatusRequestProto(id, currentStatus, newStatus, _ as String) >> request
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
        1 * protoConverter.toChangeJobStatusRequestProto(id, currentStatus, newStatus, message) >> request

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
        1 * protoConverter.toChangeJobStatusRequestProto(id, currentStatus, newStatus, message) >> request

        thrown(GenieRuntimeException)
    }

    def "Get job status -- successful"() {
        this.getJobStatusResponse = GetJobStatusResponse.newBuilder().setStatus(JobStatus.RUNNING.name()).build()
        GetJobStatusRequest request = GetJobStatusRequest.getDefaultInstance()

        when:
        service.getJobStatus(id)

        then:
        1 * protoConverter.toGetJobStatusRequestProto(id) >> request
    }

    def "Get job status -- invalid response"() {
        this.getJobStatusResponse = GetJobStatusResponse.getDefaultInstance()
        GetJobStatusRequest request = GetJobStatusRequest.getDefaultInstance()

        when:
        service.getJobStatus(id)

        then:
        1 * protoConverter.toGetJobStatusRequestProto(id) >> request

        thrown(GetJobStatusException)
    }

    def "Update job archive status -- successful"() {
        this.changeJobArchiveStatusResponse = ChangeJobArchiveStatusResponse.getDefaultInstance()
        ChangeJobArchiveStatusRequest request = ChangeJobArchiveStatusRequest.getDefaultInstance()

        when:
        service.changeJobArchiveStatus(id, ArchiveStatus.ARCHIVED)

        then:
        1 * protoConverter.toChangeJobStatusArchiveRequestProto(id, ArchiveStatus.ARCHIVED) >> request
    }

    def "Update job archive status -- invalid response"() {
        this.serverError = Status.NOT_FOUND.asException()
        ChangeJobArchiveStatusRequest request = ChangeJobArchiveStatusRequest.getDefaultInstance()

        when:
        service.changeJobArchiveStatus(id, ArchiveStatus.ARCHIVED)

        then:
        1 * protoConverter.toChangeJobStatusArchiveRequestProto(id, ArchiveStatus.ARCHIVED) >> request
        thrown(ChangeJobArchiveStatusException)
    }

    private class TestService extends JobServiceGrpc.JobServiceImplBase {
        @Override
        void handshake(final HandshakeRequest request, final StreamObserver<HandshakeResponse> responseObserver) {
            sendResponse(responseObserver, handshakeResponse)
        }

        @Override
        void configure(final ConfigureRequest request, final StreamObserver<ConfigureResponse> responseObserver) {
            sendResponse(responseObserver, configureResponse)
        }

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

        @Override
        void getJobStatus(
            final GetJobStatusRequest getJobStatusRequest,
            final StreamObserver<GetJobStatusResponse> responseObserver
        ) {
            sendResponse(responseObserver, getJobStatusResponse)
        }

        @Override
        void changeJobArchiveStatus(
            final ChangeJobArchiveStatusRequest request,
            final StreamObserver<ChangeJobArchiveStatusResponse> responseObserver
        ) {
            sendResponse(responseObserver, changeJobArchiveStatusResponse)
        }

        private <ResponseType> void sendResponse(
            StreamObserver<ResponseType> observer,
            ResponseType response
        ) {
            if (serverError != null) {
                observer.onError(serverError)
            } else {
                assert response != null
                observer.onNext(response)
                observer.onCompleted()
            }
        }
    }
}
