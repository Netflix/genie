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

import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.exceptions.GeniePreconditionException
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata
import com.netflix.genie.common.internal.dto.v4.JobRequest
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.common.internal.dto.v4.converters.JobServiceProtoConverter
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobSpecificationNotFoundException
import com.netflix.genie.proto.*
import com.netflix.genie.web.services.AgentJobService
import io.grpc.stub.StreamObserver
import spock.lang.Specification

/**
 * Specifications for the {@link GRpcJobServiceImpl} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
class GRpcJobServiceImplSpec extends Specification {

    String id
    JobServiceProtoErrorComposer errorMessageComposer
    AgentJobService agentJobService
    GRpcJobServiceImpl gRpcJobService
    StreamObserver<ReserveJobIdResponse> reserveJobIdResponseObserver
    StreamObserver<JobSpecificationResponse> jobSpecificationResponseObserver
    StreamObserver<ClaimJobResponse> claimJobResponseObserver
    StreamObserver<ChangeJobStatusResponse> changeJobStatusResponseObserver
    JobServiceProtoConverter jobServiceProtoConverter

    def setup() {
        this.id = UUID.randomUUID().toString()
        this.errorMessageComposer = Mock(JobServiceProtoErrorComposer)
        this.jobServiceProtoConverter = Mock(JobServiceProtoConverter)
        this.agentJobService = Mock(AgentJobService)
        this.gRpcJobService = new GRpcJobServiceImpl(agentJobService, jobServiceProtoConverter, errorMessageComposer)
        this.reserveJobIdResponseObserver = Mock(StreamObserver)
        this.jobSpecificationResponseObserver = Mock(StreamObserver)
        this.claimJobResponseObserver = Mock(StreamObserver)
        this.changeJobStatusResponseObserver = Mock(StreamObserver)
    }

    def "Reserve job id -- successful"() {
        ReserveJobIdRequest request = ReserveJobIdRequest.newBuilder().build()
        JobRequest jobRequest = Mock(JobRequest)
        AgentClientMetadata agentClientMetadata = Mock(AgentClientMetadata)
        ReserveJobIdResponse expectedResponse = ReserveJobIdResponse.newBuilder().setId(id).build()

        when:
        gRpcJobService.reserveJobId(request, reserveJobIdResponseObserver)

        then:
        1 * jobServiceProtoConverter.toJobRequestDTO(request) >> jobRequest
        1 * jobServiceProtoConverter.toAgentClientMetadataDTO(request.getAgentMetadata()) >> agentClientMetadata
        1 * agentJobService.reserveJobId(jobRequest, agentClientMetadata) >> id
        1 * reserveJobIdResponseObserver.onNext(expectedResponse)
        1 * reserveJobIdResponseObserver.onCompleted()
    }

    def "Reserve job id -- service exception"() {
        ReserveJobIdRequest request = ReserveJobIdRequest.newBuilder().build()
        JobRequest jobRequest = Mock(JobRequest)
        AgentClientMetadata agentClientMetadata = Mock(AgentClientMetadata)
        Exception e = new RuntimeException()
        ReserveJobIdResponse errorResponse = ReserveJobIdResponse.newBuilder().build()

        when:
        gRpcJobService.reserveJobId(request, reserveJobIdResponseObserver)

        then:
        1 * jobServiceProtoConverter.toJobRequestDTO(request) >> jobRequest
        1 * jobServiceProtoConverter.toAgentClientMetadataDTO(request.getAgentMetadata()) >> agentClientMetadata
        1 * agentJobService.reserveJobId(_ as JobRequest, _ as AgentClientMetadata) >> {
            throw e
        }
        1 * errorMessageComposer.toProtoReserveJobIdResponse(e) >> errorResponse
        1 * reserveJobIdResponseObserver.onNext(errorResponse)
        1 * reserveJobIdResponseObserver.onCompleted()
    }

    def "Resolve specification -- successful"() {
        JobSpecificationRequest request = JobSpecificationRequest.newBuilder().setId(id).build()
        JobSpecification jobSpecification = Mock(JobSpecification)
        JobSpecificationResponse specificationResponse = JobSpecificationResponse.newBuilder().build()

        when:
        gRpcJobService.resolveJobSpecification(request, jobSpecificationResponseObserver)

        then:
        1 * agentJobService.resolveJobSpecification(id) >> jobSpecification
        1 * jobServiceProtoConverter.toProtoJobSpecificationResponse(jobSpecification) >> specificationResponse
        1 * jobSpecificationResponseObserver.onNext(specificationResponse)
        1 * jobSpecificationResponseObserver.onCompleted()
    }


    def "Resolve job specification -- successful"() {
        JobSpecificationRequest request = JobSpecificationRequest.newBuilder().setId(id).build()
        JobSpecification jobSpecification = Mock(JobSpecification)
        JobSpecificationResponse response = JobSpecificationResponse.newBuilder().build()

        when:
        gRpcJobService.resolveJobSpecification(request, jobSpecificationResponseObserver)

        then:
        1 * agentJobService.resolveJobSpecification(id) >> jobSpecification
        1 * jobServiceProtoConverter.toProtoJobSpecificationResponse(jobSpecification) >> response
        1 * jobSpecificationResponseObserver.onNext(response)
        1 * jobSpecificationResponseObserver.onCompleted()
    }

    def "Resolve job specification -- service exception"() {
        JobSpecificationRequest request = JobSpecificationRequest.newBuilder().setId(id).build()
        Exception exception = new GenieJobNotFoundException()
        JobSpecificationResponse response = JobSpecificationResponse.newBuilder().build()

        when:
        gRpcJobService.resolveJobSpecification(request, jobSpecificationResponseObserver)

        then:
        1 * agentJobService.resolveJobSpecification(id) >> { throw exception }
        1 * errorMessageComposer.toProtoJobSpecificationResponse(exception) >> response
        1 * jobSpecificationResponseObserver.onNext(response)
        1 * jobSpecificationResponseObserver.onCompleted()
    }

    def "Get job specification -- successful"() {
        JobSpecificationRequest request = JobSpecificationRequest.newBuilder().setId(id).build()
        JobSpecification jobSpecification = Mock(JobSpecification)
        JobSpecificationResponse response = JobSpecificationResponse.newBuilder().build()

        when:
        gRpcJobService.getJobSpecification(request, jobSpecificationResponseObserver)

        then:
        1 * agentJobService.getJobSpecification(id) >> jobSpecification
        1 * jobServiceProtoConverter.toProtoJobSpecificationResponse(jobSpecification) >> response
        1 * jobSpecificationResponseObserver.onNext(response)
        1 * jobSpecificationResponseObserver.onCompleted()
    }

    def "Get job specification -- service exception"() {
        JobSpecificationRequest request = JobSpecificationRequest.newBuilder().setId(id).build()
        Exception exception = new GenieJobSpecificationNotFoundException()
        JobSpecificationResponse response = JobSpecificationResponse.newBuilder().build()

        when:
        gRpcJobService.getJobSpecification(request, jobSpecificationResponseObserver)

        then:
        1 * agentJobService.getJobSpecification(id) >> { throw exception }
        1 * errorMessageComposer.toProtoJobSpecificationResponse(exception) >> response
        1 * jobSpecificationResponseObserver.onNext(response)
        1 * jobSpecificationResponseObserver.onCompleted()
    }

    def "Dry run resolve job specification -- successful"() {
        DryRunJobSpecificationRequest request = DryRunJobSpecificationRequest.newBuilder().build()
        JobRequest jobRequest = Mock(JobRequest)
        JobSpecification jobSpecification = Mock(JobSpecification)
        JobSpecificationResponse response = JobSpecificationResponse.newBuilder().build()

        when:
        gRpcJobService.resolveJobSpecificationDryRun(request, jobSpecificationResponseObserver)

        then:
        1 * jobServiceProtoConverter.toJobRequestDTO(request) >> jobRequest
        1 * agentJobService.dryRunJobSpecificationResolution(jobRequest) >> jobSpecification
        1 * jobServiceProtoConverter.toProtoJobSpecificationResponse(jobSpecification) >> response
        1 * jobSpecificationResponseObserver.onNext(response)
        1 * jobSpecificationResponseObserver.onCompleted()
    }

    def "Dry run resolve job specification -- service exception"() {
        DryRunJobSpecificationRequest request = DryRunJobSpecificationRequest.newBuilder().build()
        JobRequest jobRequest = Mock(JobRequest)
        Exception exception = new GenieJobAlreadyClaimedException()
        JobSpecificationResponse response = JobSpecificationResponse.newBuilder().build()

        when:
        gRpcJobService.resolveJobSpecificationDryRun(request, jobSpecificationResponseObserver)

        then:
        1 * jobServiceProtoConverter.toJobRequestDTO(request) >> jobRequest
        1 * agentJobService.dryRunJobSpecificationResolution(jobRequest) >> { throw exception }
        1 * errorMessageComposer.toProtoJobSpecificationResponse(exception) >> response
        1 * jobSpecificationResponseObserver.onNext(response)
        1 * jobSpecificationResponseObserver.onCompleted()
    }

    def "Claim job -- successful"() {
        ClaimJobRequest request = ClaimJobRequest.newBuilder().setId(id).build()
        AgentClientMetadata clientMetadata = Mock(AgentClientMetadata)
        ClaimJobResponse responseCapture

        when:
        gRpcJobService.claimJob(request, claimJobResponseObserver)

        then:
        1 * jobServiceProtoConverter.toAgentClientMetadataDTO(request.getAgentMetadata()) >> clientMetadata
        1 * agentJobService.claimJob(id, clientMetadata)
        1 * claimJobResponseObserver.onNext(_ as ClaimJobResponse) >> {
            args ->
                responseCapture = args[0] as ClaimJobResponse
        }
        1 * claimJobResponseObserver.onCompleted()

        expect:
        responseCapture != null
        responseCapture.getSuccessful()
    }

    def "Claim job -- service exception"() {
        ClaimJobRequest request = ClaimJobRequest.newBuilder().setId(id).build()
        AgentClientMetadata clientMetadata = Mock(AgentClientMetadata)
        Exception exception = new GenieJobAlreadyClaimedException()
        ClaimJobResponse response = ClaimJobResponse.newBuilder().build()


        when:
        gRpcJobService.claimJob(request, claimJobResponseObserver)

        then:
        1 * jobServiceProtoConverter.toAgentClientMetadataDTO(request.getAgentMetadata()) >> clientMetadata
        1 * agentJobService.claimJob(id, clientMetadata) >> { throw exception }
        1 * errorMessageComposer.toProtoClaimJobResponse(exception) >> response
        1 * claimJobResponseObserver.onNext(response)
        1 * claimJobResponseObserver.onCompleted()
    }

    def "Change job status -- successful"() {
        JobStatus currentStatus = JobStatus.INIT
        JobStatus newStatus = JobStatus.RUNNING
        String message = "..."
        ChangeJobStatusRequest request = ChangeJobStatusRequest.newBuilder()
                .setCurrentStatus(currentStatus.name())
                .setNewStatus(newStatus.name())
                .setId(id)
                .setNewStatusMessage(message)
                .build()
        ChangeJobStatusResponse responseCapture

        when:
        gRpcJobService.changeJobStatus(request, changeJobStatusResponseObserver)

        then:
        1 * agentJobService.updateJobStatus(id, currentStatus, newStatus, message)
        1 * changeJobStatusResponseObserver.onNext(_ as ChangeJobStatusResponse) >> {
            args -> responseCapture = args[0] as ChangeJobStatusResponse
        }
        1 * changeJobStatusResponseObserver.onCompleted()

        expect:
        responseCapture != null
        responseCapture.getSuccessful()
    }

    def "Change job status -- service exception"() {
        JobStatus currentStatus = JobStatus.INIT
        JobStatus newStatus = JobStatus.RUNNING
        String message = "..."
        ChangeJobStatusRequest request = ChangeJobStatusRequest.newBuilder()
                .setCurrentStatus(currentStatus.name())
                .setNewStatus(newStatus.name())
                .setId(id)
                .setNewStatusMessage(message)
                .build()
        Exception exception = new GenieInvalidStatusException()
        ChangeJobStatusResponse response = ChangeJobStatusResponse.newBuilder().build()

        when:
        gRpcJobService.changeJobStatus(request, changeJobStatusResponseObserver)

        then:
        1 * agentJobService.updateJobStatus(id, currentStatus, newStatus, message) >> {
            throw exception
        }
        1 * errorMessageComposer.toProtoChangeJobStatusResponse(exception) >> response
        1 * changeJobStatusResponseObserver.onNext(response)
        1 * changeJobStatusResponseObserver.onCompleted()
    }

    def "Change job status -- unrecognized state"() {
        String invalidStatusName = "Foo"
        JobStatus newStatus = JobStatus.RUNNING
        String message = "..."
        ChangeJobStatusRequest request = ChangeJobStatusRequest.newBuilder()
                .setCurrentStatus(invalidStatusName)
                .setNewStatus(newStatus.name())
                .setId(id)
                .setNewStatusMessage(message)
                .build()
        ChangeJobStatusResponse response = ChangeJobStatusResponse.newBuilder().build()

        when:
        gRpcJobService.changeJobStatus(request, changeJobStatusResponseObserver)

        then:
        0 * agentJobService.updateJobStatus(_, _, _, _)
        1 * errorMessageComposer.toProtoChangeJobStatusResponse(_ as GeniePreconditionException) >> response
        1 * changeJobStatusResponseObserver.onNext(response)
        1 * changeJobStatusResponseObserver.onCompleted()
    }
}
