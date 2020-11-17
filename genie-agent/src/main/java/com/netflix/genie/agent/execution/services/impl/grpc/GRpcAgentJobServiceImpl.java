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
package com.netflix.genie.agent.execution.services.impl.grpc;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.genie.agent.execution.exceptions.ChangeJobArchiveStatusException;
import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException;
import com.netflix.genie.agent.execution.exceptions.ConfigureException;
import com.netflix.genie.agent.execution.exceptions.GetJobStatusException;
import com.netflix.genie.agent.execution.exceptions.HandshakeException;
import com.netflix.genie.agent.execution.exceptions.JobIdUnavailableException;
import com.netflix.genie.agent.execution.exceptions.JobReservationException;
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.dtos.v4.converters.JobServiceProtoConverter;
import com.netflix.genie.common.internal.exceptions.checked.GenieConversionException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import com.netflix.genie.proto.ChangeJobArchiveStatusRequest;
import com.netflix.genie.proto.ChangeJobStatusError;
import com.netflix.genie.proto.ChangeJobStatusRequest;
import com.netflix.genie.proto.ChangeJobStatusResponse;
import com.netflix.genie.proto.ClaimJobError;
import com.netflix.genie.proto.ClaimJobRequest;
import com.netflix.genie.proto.ClaimJobResponse;
import com.netflix.genie.proto.ConfigureRequest;
import com.netflix.genie.proto.ConfigureResponse;
import com.netflix.genie.proto.DryRunJobSpecificationRequest;
import com.netflix.genie.proto.GetJobStatusRequest;
import com.netflix.genie.proto.GetJobStatusResponse;
import com.netflix.genie.proto.HandshakeRequest;
import com.netflix.genie.proto.HandshakeResponse;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.JobSpecificationError;
import com.netflix.genie.proto.JobSpecificationRequest;
import com.netflix.genie.proto.JobSpecificationResponse;
import com.netflix.genie.proto.ReserveJobIdError;
import com.netflix.genie.proto.ReserveJobIdRequest;
import com.netflix.genie.proto.ReserveJobIdResponse;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Client-side implementation of the job service used to obtain job id, specification and update job state.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
class GRpcAgentJobServiceImpl implements AgentJobService {

    private static final String NO_MESSAGE = "No message";
    private final JobServiceGrpc.JobServiceFutureStub client;
    private final JobServiceProtoConverter jobServiceProtoConverter;

    /**
     * Constructor.
     *
     * @param client                   The gRPC client to use to call the server.
     *                                 Asynchronous version to allow timeouts.
     * @param jobServiceProtoConverter The proto/DTO converter utility
     */
    GRpcAgentJobServiceImpl(
        final JobServiceGrpc.JobServiceFutureStub client,
        final JobServiceProtoConverter jobServiceProtoConverter
    ) {
        this.client = client;
        this.jobServiceProtoConverter = jobServiceProtoConverter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handshake(
        final AgentClientMetadata agentClientMetadata
    ) throws HandshakeException {
        final HandshakeRequest request;
        try {
            request = jobServiceProtoConverter.toHandshakeRequestProto(agentClientMetadata);
        } catch (final GenieConversionException e) {
            throw new HandshakeException("Failed to construct request from parameters", e);
        }

        final HandshakeResponse response = handleResponseFuture(this.client.handshake(request));

        switch (response.getType()) {
            case ALLOWED:
                log.info("Successfully shook hand with server");
                break;

            case REJECTED:
                log.warn("Server rejected handshake with this agent");
                throw new HandshakeException("Server rejected client: " + response.getMessage());

            default:
                throw new GenieRuntimeException(
                    "Error during handshake: "
                        + response.getType().name()
                        + " - "
                        + response.getMessage()
                );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> configure(
        final AgentClientMetadata agentClientMetadata
    ) throws ConfigureException {
        final ConfigureRequest request;
        try {
            request = jobServiceProtoConverter.toConfigureRequestProto(agentClientMetadata);
        } catch (final GenieConversionException e) {
            throw new ConfigureException("Failed to construct request from parameters", e);
        }

        final ConfigureResponse response = handleResponseFuture(this.client.configure(request));

        return ImmutableMap.copyOf(response.getPropertiesMap());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String reserveJobId(
        final AgentJobRequest agentJobRequest,
        final AgentClientMetadata agentClientMetadata
    ) throws JobReservationException, JobIdUnavailableException {
        final ReserveJobIdRequest request;
        try {
            request = jobServiceProtoConverter.toReserveJobIdRequestProto(agentJobRequest, agentClientMetadata);
        } catch (final GenieConversionException e) {
            throw new JobReservationException("Failed to construct request from parameters", e);
        }

        final ReserveJobIdResponse response = handleResponseFuture(this.client.reserveJobId(request));

        switch (response.getResponseCase()) {

            case ID:
                log.info("Successfully reserved job id: " + response.getId());
                break;

            case ERROR:
                throwForReservationError(response.getError());
                break;

            case RESPONSE_NOT_SET:
            default:
                throw new GenieRuntimeException("Unexpected server response " + response.toString());
        }

        return response.getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSpecification resolveJobSpecification(
        @NotBlank final String id
    ) throws JobSpecificationResolutionException {

        final JobSpecificationRequest request = jobServiceProtoConverter.toJobSpecificationRequestProto(id);

        final JobSpecificationResponse response = handleResponseFuture(client.resolveJobSpecification(request));

        return handleSpecificationResponse(response);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSpecification getJobSpecification(@NotBlank final String id) throws JobSpecificationResolutionException {

        final JobSpecificationRequest request = jobServiceProtoConverter.toJobSpecificationRequestProto(id);

        final JobSpecificationResponse response = handleResponseFuture(this.client.getJobSpecification(request));

        return handleSpecificationResponse(response);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSpecification resolveJobSpecificationDryRun(
        @Valid final AgentJobRequest jobRequest
    ) throws JobSpecificationResolutionException {

        final DryRunJobSpecificationRequest request;
        try {
            request = jobServiceProtoConverter.toDryRunJobSpecificationRequestProto(jobRequest);
        } catch (final GenieConversionException e) {
            throw new JobSpecificationResolutionException("Failed to construct request from parameters", e);
        }

        final JobSpecificationResponse response =
            handleResponseFuture(this.client.resolveJobSpecificationDryRun(request));

        return handleSpecificationResponse(response);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void claimJob(
        @NotBlank final String jobId,
        @Valid final AgentClientMetadata agentClientMetadata
    ) throws JobReservationException {

        final ClaimJobRequest request = jobServiceProtoConverter.toClaimJobRequestProto(jobId, agentClientMetadata);

        final ClaimJobResponse response = handleResponseFuture(this.client.claimJob(request));

        if (!response.getSuccessful()) {
            throwForClaimJobError(response.getError());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void changeJobStatus(
        final @NotBlank String jobId,
        final JobStatus currentJobStatus,
        final JobStatus newJobStatus,
        @Nullable final String message
    ) throws ChangeJobStatusException {

        final ChangeJobStatusRequest request = this.jobServiceProtoConverter.toChangeJobStatusRequestProto(
            jobId,
            currentJobStatus,
            newJobStatus,
            message == null ? NO_MESSAGE : message
        );

        final ChangeJobStatusResponse response = handleResponseFuture(this.client.changeJobStatus(request));

        if (!response.getSuccessful()) {
            throwForChangeJobStatusError(response.getError());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobStatus getJobStatus(@NotBlank final String jobId) throws GetJobStatusException {
        final GetJobStatusRequest request = this.jobServiceProtoConverter.toGetJobStatusRequestProto(jobId);

        final GetJobStatusResponse response = handleResponseFuture((this.client.getJobStatus(request)));

        final String statusString = response.getStatus();

        try {
            return JobStatus.valueOf(statusString);
        } catch (IllegalArgumentException e) {
            throw new GetJobStatusException("Invalid job status in response: " + statusString);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void changeJobArchiveStatus(
        @NotBlank final String jobId,
        final ArchiveStatus archiveStatus
    ) throws ChangeJobArchiveStatusException {

        final ChangeJobArchiveStatusRequest request =
            this.jobServiceProtoConverter.toChangeJobStatusArchiveRequestProto(
                jobId,
                archiveStatus
            );

        try {
            handleResponseFuture(this.client.changeJobArchiveStatus(request));
        } catch (GenieRuntimeException e) {
            throw new ChangeJobArchiveStatusException("Failed to update job archive status", e);
        }
    }

    private JobSpecification handleSpecificationResponse(
        final JobSpecificationResponse response
    ) throws JobSpecificationResolutionException {

        switch (response.getResponseCase()) {

            case SPECIFICATION:
                log.info("Successfully obtained job specification");
                break;

            case ERROR:
                return throwForJobSpecificationError(response.getError());

            case RESPONSE_NOT_SET:
            default:
                throw new GenieRuntimeException("Unexpected server response " + response.toString());
        }

        return jobServiceProtoConverter.toJobSpecificationDto(response.getSpecification());
    }

    private void throwForClaimJobError(final ClaimJobError error) throws JobReservationException {
        switch (error.getType()) {
            case NO_SUCH_JOB:
            case INVALID_STATUS:
            case INVALID_REQUEST:
            case ALREADY_CLAIMED:
                throw new JobReservationException(
                    "Failed to claim job: "
                        + error.getType().name()
                        + ": "
                        + error.getMessage()
                );
            default:
                throw new GenieRuntimeException("Unhandled error: " + error.getType() + ": " + error.getMessage());
        }
    }

    private void throwForReservationError(
        final ReserveJobIdError error
    ) throws JobIdUnavailableException, JobReservationException {
        switch (error.getType()) {
            case ID_NOT_AVAILABLE:
                throw new JobIdUnavailableException("The requested job id is already been used");
            case SERVER_ERROR:
                throw new JobReservationException("Server error: " + error.getMessage());
            case INVALID_REQUEST:
                throw new JobReservationException("Invalid request: " + error.getMessage());
            case UNKNOWN:
            default:
                throw new GenieRuntimeException("Unhandled error: " + error.getType() + ": " + error.getMessage());
        }
    }

    private JobSpecification throwForJobSpecificationError(
        final JobSpecificationError error
    ) throws JobSpecificationResolutionException {
        switch (error.getType()) {
            case NO_APPLICATION_FOUND:
            case NO_CLUSTER_FOUND:
            case NO_JOB_FOUND:
            case NO_COMMAND_FOUND:
            case RESOLUTION_FAILED:
                throw new JobSpecificationResolutionException(
                    "Failed to obtain specification: "
                        + error.getType().name()
                        + ": "
                        + error.getMessage()
                );
            case RUNTIME_ERROR:
                throw new GenieRuntimeException(
                    "Transient error resolving job specification: " + error.getMessage());
            case UNKNOWN:
            default:
                throw new GenieRuntimeException(
                    "Unhandled error: "
                        + error.getType()
                        + ": "
                        + error.getMessage()
                );
        }
    }

    private void throwForChangeJobStatusError(
        final ChangeJobStatusError error
    ) throws ChangeJobStatusException {
        switch (error.getType()) {
            case INVALID_REQUEST:
            case NO_SUCH_JOB:
            case INCORRECT_CURRENT_STATUS:
                throw new ChangeJobStatusException(
                    "Failed to claim job: "
                        + error.getType().name()
                        + ": "
                        + error.getMessage()
                );
            case UNKNOWN:
            default:
                throw new GenieRuntimeException(
                    "Unhandled error: "
                        + error.getType()
                        + ": "
                        + error.getMessage()
                );
        }
    }

    private <ResponseType> ResponseType handleResponseFuture(
        final ListenableFuture<ResponseType> responseListenableFuture
    ) {
        final ResponseType response;
        try {
            response = responseListenableFuture.get();
        } catch (final ExecutionException | InterruptedException e) {
            throw new GenieRuntimeException("Failed to perform request", e);
        }
        return response;
    }
}
