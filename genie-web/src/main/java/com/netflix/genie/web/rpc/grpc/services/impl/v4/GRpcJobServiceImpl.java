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
package com.netflix.genie.web.rpc.grpc.services.impl.v4;

import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.dto.v4.JobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.dto.v4.converters.JobServiceProtoConverter;
import com.netflix.genie.proto.ChangeJobStatusRequest;
import com.netflix.genie.proto.ChangeJobStatusResponse;
import com.netflix.genie.proto.ClaimJobRequest;
import com.netflix.genie.proto.ClaimJobResponse;
import com.netflix.genie.proto.DryRunJobSpecificationRequest;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.JobSpecificationRequest;
import com.netflix.genie.proto.JobSpecificationResponse;
import com.netflix.genie.proto.ReserveJobIdRequest;
import com.netflix.genie.proto.ReserveJobIdResponse;
import com.netflix.genie.web.properties.GRpcServerProperties;
import com.netflix.genie.web.rpc.grpc.interceptors.SimpleLoggingInterceptor;
import com.netflix.genie.web.services.AgentJobService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * Extension of {@link JobServiceGrpc.JobServiceImplBase} to provide
 * functionality for resolving and fetching specifications for jobs to be run by the Genie Agent.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ConditionalOnProperty(value = GRpcServerProperties.ENABLED_PROPERTY, havingValue = "true")
@GrpcService(
    value = JobServiceGrpc.class,
    interceptors = {
        SimpleLoggingInterceptor.class,
    }
)
@Slf4j
public class GRpcJobServiceImpl extends JobServiceGrpc.JobServiceImplBase {
    private final AgentJobService agentJobService;
    private final JobServiceProtoConverter jobServiceProtoConverter;
    private final JobServiceProtoErrorComposer protoErrorComposer;

    // TODO: Metrics which I believe can be captured by an interceptor

    /**
     * Constructor.
     *
     * @param agentJobService The implementation of the {@link AgentJobService} to use
     * @param jobServiceProtoConverter DTO/Proto converter
     * @param protoErrorComposer proto error message composer
     */
    public GRpcJobServiceImpl(
        final AgentJobService agentJobService,
        final JobServiceProtoConverter jobServiceProtoConverter,
        final JobServiceProtoErrorComposer protoErrorComposer
    ) {
        this.agentJobService = agentJobService;
        this.jobServiceProtoConverter = jobServiceProtoConverter;
        this.protoErrorComposer = protoErrorComposer;
    }

    /**
     * This API will reserve a job id using the supplied metadata in the request and return the reserved job id.
     *
     * @param request          The request containing all the metadata necessary to reserve a job id in the system
     * @param responseObserver To send the response
     */
    @Override
    public void reserveJobId(
        final ReserveJobIdRequest request,
        final StreamObserver<ReserveJobIdResponse> responseObserver
    ) {
        try {
            final JobRequest jobRequest = jobServiceProtoConverter.toJobRequestDTO(request);
            final AgentClientMetadata agentClientMetadata
                = jobServiceProtoConverter.toAgentClientMetadataDTO(request.getAgentMetadata());
            final String jobId =  this.agentJobService.reserveJobId(jobRequest, agentClientMetadata);
            responseObserver.onNext(
                ReserveJobIdResponse.newBuilder()
                    .setId(jobId)
                    .build()
                );
        } catch (final Exception e) {
            responseObserver.onNext(protoErrorComposer.toProtoReserveJobIdResponse(e));
        }
        responseObserver.onCompleted();
    }

    /**
     * This API will take a request to resolve a Job Specification and given the inputs figure out all the details
     * needed to flesh out a job specification for the Agent to run. The request parameters will be stored in the
     * database.
     *
     * @param request          The request information
     * @param responseObserver How to send a response
     */
    @Override
    public void resolveJobSpecification(
        final JobSpecificationRequest request,
        final StreamObserver<JobSpecificationResponse> responseObserver
    ) {
        try {
            final String id = request.getId();
            final JobSpecification jobSpec = this.agentJobService.resolveJobSpecification(id);
            responseObserver.onNext(jobServiceProtoConverter.toProtoJobSpecificationResponse(jobSpec));
        } catch (final Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onNext(protoErrorComposer.toProtoJobSpecificationResponse(e));
        }
        responseObserver.onCompleted();
    }

    /**
     * Assuming a specification has already been resolved the agent will call this API with a job id to fetch the
     * specification.
     *
     * @param request          The request containing the job id to return the specification for
     * @param responseObserver How to send a response
     */
    @Override
    public void getJobSpecification(
        final JobSpecificationRequest request,
        final StreamObserver<JobSpecificationResponse> responseObserver
    ) {
        try {
            final String id = request.getId();
            final JobSpecification jobSpecification = this.agentJobService.getJobSpecification(id);
            responseObserver.onNext(jobServiceProtoConverter.toProtoJobSpecificationResponse(jobSpecification));
        } catch (final Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onNext(protoErrorComposer.toProtoJobSpecificationResponse(e));
        }
        responseObserver.onCompleted();
    }

    /**
     * The agent requests that a job specification be resolved without impacting any state in the database. This
     * operation is completely transient and just reflects what the job specification would look like given the
     * current state of the system and the input parameters.
     *
     * @param request          The request containing all the metadata required to resolve a job specification
     * @param responseObserver The observer to send a response with
     */
    @Override
    public void resolveJobSpecificationDryRun(
        final DryRunJobSpecificationRequest request,
        final StreamObserver<JobSpecificationResponse> responseObserver
    ) {
        try {
            final JobRequest jobRequest = jobServiceProtoConverter.toJobRequestDTO(request);
            final JobSpecification jobSpecification = this.agentJobService.dryRunJobSpecificationResolution(jobRequest);
            responseObserver.onNext(jobServiceProtoConverter.toProtoJobSpecificationResponse(jobSpecification));
        } catch (final Exception e) {
            log.error("Error resolving job specification for request " + request, e);
            responseObserver.onNext(protoErrorComposer.toProtoJobSpecificationResponse(e));
        }
        responseObserver.onCompleted();
    }

    /**
     * When an agent is claiming responsibility and ownership for a job this API is called.
     *
     * @param request          The request containing the job id being claimed and other pertinent metadata
     * @param responseObserver The observer to send a response with
     */
    @Override
    public void claimJob(
        final ClaimJobRequest request,
        final StreamObserver<ClaimJobResponse> responseObserver
    ) {
        try {
            final String id = request.getId();
            final AgentClientMetadata clientMetadata
                = jobServiceProtoConverter.toAgentClientMetadataDTO(request.getAgentMetadata());
            this.agentJobService.claimJob(id, clientMetadata);
            responseObserver.onNext(ClaimJobResponse.newBuilder().setSuccessful(true).build());
        } catch (final Exception e) {
            log.error("Error claiming job for request " + request, e);
            responseObserver.onNext(protoErrorComposer.toProtoClaimJobResponse(e));

        }
        responseObserver.onCompleted();
    }

    /**
     * When the agent wants to tell the system that the status of a job is changed this API is called.
     *
     * @param request          The request containing the necessary metadata to change job status for a given job
     * @param responseObserver The observer to send a response with
     */
    @Override
    public void changeJobStatus(
        final ChangeJobStatusRequest request,
        final StreamObserver<ChangeJobStatusResponse> responseObserver
    ) {
        try {
            final String id = request.getId();
            final JobStatus currentStatus = JobStatus.parse(request.getCurrentStatus());
            final JobStatus newStatus = JobStatus.parse(request.getNewStatus());
            final String newStatusMessage = request.getNewStatusMessage();
            this.agentJobService.updateJobStatus(id, currentStatus, newStatus, newStatusMessage);
            responseObserver.onNext(ChangeJobStatusResponse.newBuilder().setSuccessful(true).build());
        } catch (Exception e) {
            log.error("Error changing job status for request " + request, e);
            responseObserver.onNext(protoErrorComposer.toProtoChangeJobStatusResponse(e));
        }
        responseObserver.onCompleted();
    }
}
