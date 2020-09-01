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
package com.netflix.genie.web.agent.apis.rpc.v4.endpoints

import com.google.common.collect.Maps
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.dtos.v4.converters.JobServiceProtoConverter
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobSpecificationNotFoundException
import com.netflix.genie.proto.AgentMetadata
import com.netflix.genie.proto.ChangeJobArchiveStatusRequest
import com.netflix.genie.proto.ChangeJobArchiveStatusResponse
import com.netflix.genie.proto.ChangeJobStatusRequest
import com.netflix.genie.proto.ChangeJobStatusResponse
import com.netflix.genie.proto.ClaimJobRequest
import com.netflix.genie.proto.ClaimJobResponse
import com.netflix.genie.proto.ConfigureRequest
import com.netflix.genie.proto.ConfigureResponse
import com.netflix.genie.proto.DryRunJobSpecificationRequest
import com.netflix.genie.proto.GetJobStatusRequest
import com.netflix.genie.proto.GetJobStatusResponse
import com.netflix.genie.proto.HandshakeRequest
import com.netflix.genie.proto.HandshakeResponse
import com.netflix.genie.proto.JobSpecificationRequest
import com.netflix.genie.proto.JobSpecificationResponse
import com.netflix.genie.proto.ReserveJobIdRequest
import com.netflix.genie.proto.ReserveJobIdResponse
import com.netflix.genie.web.agent.services.AgentJobService
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.apache.commons.lang3.StringUtils
import org.assertj.core.util.Sets
import spock.lang.Specification
import spock.lang.Unroll

import javax.validation.ConstraintViolationException
import java.util.concurrent.TimeUnit

/**
 * Specifications for the {@link GRpcJobServiceImpl} class.
 *
 * @author tgianos
 */
class GRpcJobServiceImplSpec extends Specification {

    String id
    JobServiceProtoErrorComposer errorMessageComposer
    AgentJobService agentJobService
    GRpcJobServiceImpl gRpcJobService
    StreamObserver<HandshakeResponse> handshakeResponseObserver
    StreamObserver<ConfigureResponse> configureResponseObserver
    StreamObserver<ReserveJobIdResponse> reserveJobIdResponseObserver
    StreamObserver<JobSpecificationResponse> jobSpecificationResponseObserver
    StreamObserver<ClaimJobResponse> claimJobResponseObserver
    StreamObserver<ChangeJobStatusResponse> changeJobStatusResponseObserver
    StreamObserver<GetJobStatusResponse> getJobStatusResponseObserver
    StreamObserver<ChangeJobArchiveStatusResponse> changeJobArchiveStatusObserver
    JobServiceProtoConverter jobServiceProtoConverter
    MeterRegistry meterRegistry
    Timer timer
    AgentClientMetadata agentClientMetadata

    def setup() {
        this.id = UUID.randomUUID().toString()
        this.errorMessageComposer = Mock(JobServiceProtoErrorComposer)
        this.jobServiceProtoConverter = Mock(JobServiceProtoConverter)
        this.agentJobService = Mock(AgentJobService)
        this.meterRegistry = Mock(MeterRegistry)
        this.gRpcJobService = new GRpcJobServiceImpl(agentJobService, jobServiceProtoConverter, errorMessageComposer, meterRegistry)
        this.handshakeResponseObserver = Mock(StreamObserver)
        this.configureResponseObserver = Mock(StreamObserver)
        this.reserveJobIdResponseObserver = Mock(StreamObserver)
        this.jobSpecificationResponseObserver = Mock(StreamObserver)
        this.claimJobResponseObserver = Mock(StreamObserver)
        this.changeJobStatusResponseObserver = Mock(StreamObserver)
        this.getJobStatusResponseObserver = Mock(StreamObserver)
        this.changeJobArchiveStatusObserver = Mock(StreamObserver)
        this.timer = Mock(Timer)
        this.agentClientMetadata = Mock(AgentClientMetadata) {
            getVersion() >> Optional.of("1.2.3")
        }
    }

    def "Handshake -- successful"() {
        AgentMetadata agentMetadata = AgentMetadata.newBuilder().build()
        HandshakeRequest request = HandshakeRequest.newBuilder()
            .setAgentMetadata(agentMetadata)
            .build()
        HandshakeResponse responseCapture

        when:
        gRpcJobService.handshake(request, handshakeResponseObserver)

        then:
        1 * jobServiceProtoConverter.toAgentClientMetadataDto(agentMetadata) >> agentClientMetadata
        1 * agentJobService.handshake(agentClientMetadata)
        1 * handshakeResponseObserver.onNext(_ as HandshakeResponse) >> {
            args -> responseCapture = args[0]
        }
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * handshakeResponseObserver.onCompleted()
        responseCapture != null
        StringUtils.isNotBlank(responseCapture.getMessage())
        responseCapture.getType() == HandshakeResponse.Type.ALLOWED
    }

    def "Handshake -- service exception"() {
        AgentMetadata agentMetadata = AgentMetadata.newBuilder().build()
        HandshakeRequest request = HandshakeRequest.newBuilder()
            .setAgentMetadata(agentMetadata)
            .build()
        Exception e = new RuntimeException("Some error")
        HandshakeResponse response = HandshakeResponse.newBuilder().build()

        when:
        gRpcJobService.handshake(request, handshakeResponseObserver)

        then:
        1 * jobServiceProtoConverter.toAgentClientMetadataDto(agentMetadata) >> agentClientMetadata
        1 * agentJobService.handshake(agentClientMetadata) >> { throw e }
        1 * errorMessageComposer.toProtoHandshakeResponse(e) >> response
        1 * handshakeResponseObserver.onNext(_ as HandshakeResponse) >> {
            args -> assert response == args[0]
        }
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * handshakeResponseObserver.onCompleted()
    }

    def "Configure -- successful"() {
        AgentMetadata agentMetadata = AgentMetadata.newBuilder().build()
        ConfigureRequest request = ConfigureRequest.newBuilder().build()
        Map<String, String> agentProperties = Maps.newHashMap()
        agentProperties.put("foo", "bar")
        ConfigureResponse responseCapture

        when:
        gRpcJobService.configure(request, configureResponseObserver)

        then:
        1 * jobServiceProtoConverter.toAgentClientMetadataDto(agentMetadata) >> agentClientMetadata
        1 * agentJobService.getAgentProperties(agentClientMetadata) >> agentProperties
        1 * configureResponseObserver.onNext(_ as ConfigureResponse) >> {
            args -> responseCapture = args[0]
        }
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * configureResponseObserver.onCompleted()
        responseCapture != null
        responseCapture.getPropertiesMap().get("foo") == "bar"
    }

    def "Reserve job id -- successful"() {
        ReserveJobIdRequest request = ReserveJobIdRequest.newBuilder().build()
        JobRequest jobRequest = Mock(JobRequest)
        ReserveJobIdResponse expectedResponse = ReserveJobIdResponse.newBuilder().setId(id).build()

        when:
        gRpcJobService.reserveJobId(request, reserveJobIdResponseObserver)

        then:
        1 * jobServiceProtoConverter.toJobRequestDto(request) >> jobRequest
        1 * jobServiceProtoConverter.toAgentClientMetadataDto(request.getAgentMetadata()) >> agentClientMetadata
        1 * agentJobService.reserveJobId(jobRequest, agentClientMetadata) >> id
        1 * reserveJobIdResponseObserver.onNext(expectedResponse)
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * reserveJobIdResponseObserver.onCompleted()
    }

    def "Reserve job id -- service exception"() {
        ReserveJobIdRequest request = ReserveJobIdRequest.newBuilder().build()
        JobRequest jobRequest = Mock(JobRequest)
        Exception e = new RuntimeException()
        ReserveJobIdResponse errorResponse = ReserveJobIdResponse.newBuilder().build()

        when:
        gRpcJobService.reserveJobId(request, reserveJobIdResponseObserver)

        then:
        1 * jobServiceProtoConverter.toJobRequestDto(request) >> jobRequest
        1 * jobServiceProtoConverter.toAgentClientMetadataDto(request.getAgentMetadata()) >> agentClientMetadata
        1 * agentJobService.reserveJobId(_ as JobRequest, _ as AgentClientMetadata) >> {
            throw e
        }
        1 * errorMessageComposer.toProtoReserveJobIdResponse(e) >> errorResponse
        1 * reserveJobIdResponseObserver.onNext(errorResponse)
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
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
        1 * jobServiceProtoConverter.toJobSpecificationResponseProto(jobSpecification) >> specificationResponse
        1 * jobSpecificationResponseObserver.onNext(specificationResponse)
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
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
        1 * jobServiceProtoConverter.toJobSpecificationResponseProto(jobSpecification) >> response
        1 * jobSpecificationResponseObserver.onNext(response)
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
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
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
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
        1 * jobServiceProtoConverter.toJobSpecificationResponseProto(jobSpecification) >> response
        1 * jobSpecificationResponseObserver.onNext(response)
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
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
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
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
        1 * jobServiceProtoConverter.toJobRequestDto(request) >> jobRequest
        1 * agentJobService.dryRunJobSpecificationResolution(jobRequest) >> jobSpecification
        1 * jobServiceProtoConverter.toJobSpecificationResponseProto(jobSpecification) >> response
        1 * jobSpecificationResponseObserver.onNext(response)
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
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
        1 * jobServiceProtoConverter.toJobRequestDto(request) >> jobRequest
        1 * agentJobService.dryRunJobSpecificationResolution(jobRequest) >> { throw exception }
        1 * errorMessageComposer.toProtoJobSpecificationResponse(exception) >> response
        1 * jobSpecificationResponseObserver.onNext(response)
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * jobSpecificationResponseObserver.onCompleted()
    }

    def "Claim job -- successful"() {
        ClaimJobRequest request = ClaimJobRequest.newBuilder().setId(id).build()
        ClaimJobResponse responseCapture

        when:
        gRpcJobService.claimJob(request, claimJobResponseObserver)

        then:
        1 * jobServiceProtoConverter.toAgentClientMetadataDto(request.getAgentMetadata()) >> agentClientMetadata
        1 * agentJobService.claimJob(id, agentClientMetadata)
        1 * claimJobResponseObserver.onNext(_ as ClaimJobResponse) >> {
            args ->
                responseCapture = args[0] as ClaimJobResponse
        }
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * claimJobResponseObserver.onCompleted()

        expect:
        responseCapture != null
        responseCapture.getSuccessful()
    }

    def "Claim job -- service exception"() {
        ClaimJobRequest request = ClaimJobRequest.newBuilder().setId(id).build()
        Exception exception = new GenieJobAlreadyClaimedException()
        ClaimJobResponse response = ClaimJobResponse.newBuilder().build()

        when:
        gRpcJobService.claimJob(request, claimJobResponseObserver)

        then:
        1 * jobServiceProtoConverter.toAgentClientMetadataDto(request.getAgentMetadata()) >> agentClientMetadata
        1 * agentJobService.claimJob(id, agentClientMetadata) >> { throw exception }
        1 * errorMessageComposer.toProtoClaimJobResponse(exception) >> response
        1 * claimJobResponseObserver.onNext(response)
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
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
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
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
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
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
        1 * errorMessageComposer.toProtoChangeJobStatusResponse(_ as IllegalArgumentException) >> response
        1 * changeJobStatusResponseObserver.onNext(response)
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * changeJobStatusResponseObserver.onCompleted()
    }

    def "Get job status -- successful"() {
        JobStatus currentStatus = JobStatus.INIT
        GetJobStatusRequest request = GetJobStatusRequest.newBuilder()
            .setId(id)
            .build()
        GetJobStatusResponse responseCapture

        when:
        gRpcJobService.getJobStatus(request, getJobStatusResponseObserver)

        then:
        1 * agentJobService.getJobStatus(id) >> currentStatus
        1 * getJobStatusResponseObserver.onNext(_ as GetJobStatusResponse) >> {
            GetJobStatusResponse response ->
                responseCapture = response
        }
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * getJobStatusResponseObserver.onCompleted()
        responseCapture != null

        expect:
        responseCapture.getStatus() == currentStatus.name()
    }

    def "Get job status -- service exception"() {
        Exception e = new GenieJobNotFoundException("...")
        GetJobStatusRequest request = GetJobStatusRequest.newBuilder()
            .setId(id)
            .build()

        when:
        gRpcJobService.getJobStatus(request, getJobStatusResponseObserver)

        then:
        1 * agentJobService.getJobStatus(id) >> {
            throw e
        }
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * getJobStatusResponseObserver.onError(e)
    }

    def "Update archive status -- successful"() {
        ArchiveStatus archiveStatus = ArchiveStatus.ARCHIVED
        ChangeJobArchiveStatusRequest request = ChangeJobArchiveStatusRequest.newBuilder()
            .setId(id)
            .setNewStatus(archiveStatus.name())
            .build()

        when:
        gRpcJobService.changeJobArchiveStatus(request, changeJobArchiveStatusObserver)

        then:
        1 * agentJobService.updateJobArchiveStatus(id, archiveStatus)
        1 * changeJobArchiveStatusObserver.onNext(_ as ChangeJobArchiveStatusResponse)
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * changeJobArchiveStatusObserver.onCompleted()
    }

    @Unroll
    def "Update archive status -- service exception #exception"() {
        ArchiveStatus archiveStatus = ArchiveStatus.ARCHIVED
        ChangeJobArchiveStatusRequest request = ChangeJobArchiveStatusRequest.newBuilder()
            .setId(id)
            .setNewStatus(archiveStatus.name())
            .build()

        when:
        gRpcJobService.changeJobArchiveStatus(request, changeJobArchiveStatusObserver)

        then:
        1 * agentJobService.updateJobArchiveStatus(id, archiveStatus) >> {
            throw exception
        }
        1 * meterRegistry.timer(_, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * changeJobArchiveStatusObserver.onError(_ as Exception)

        where:
        exception                                                  | _
        new GenieJobNotFoundException("...")                       | _
        new ConstraintViolationException("...", Sets.newHashSet()) | _
    }
}
