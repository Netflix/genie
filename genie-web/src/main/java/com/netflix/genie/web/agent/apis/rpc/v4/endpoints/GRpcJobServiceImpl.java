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
package com.netflix.genie.web.agent.apis.rpc.v4.endpoints;

import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.dtos.v4.converters.JobServiceProtoConverter;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.proto.ChangeJobArchiveStatusRequest;
import com.netflix.genie.proto.ChangeJobArchiveStatusResponse;
import com.netflix.genie.proto.ChangeJobStatusRequest;
import com.netflix.genie.proto.ChangeJobStatusResponse;
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
import com.netflix.genie.proto.JobSpecificationRequest;
import com.netflix.genie.proto.JobSpecificationResponse;
import com.netflix.genie.proto.ReserveJobIdRequest;
import com.netflix.genie.proto.ReserveJobIdResponse;
import com.netflix.genie.web.agent.services.AgentJobService;
import com.netflix.genie.web.util.MetricsUtils;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Extension of {@link JobServiceGrpc.JobServiceImplBase} to provide
 * functionality for resolving and fetching specifications for jobs to be run by the Genie Agent.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class GRpcJobServiceImpl extends JobServiceGrpc.JobServiceImplBase {
    private static final String AGENT_VERSION_TAG = "agentVersion";
    private static final String STATUS_FROM_TAG = "statusFrom";
    private static final String STATUS_TO_TAG = "statusTo";
    private static final String UNKNOWN_VERSION = "unknown";
    private static final String TIMERS_PREFIX = "genie.rpc.job";
    private static final String HANDSHAKE_TIMER = TIMERS_PREFIX + ".handshake.timer";
    private static final String CONFIGURE_TIMER = TIMERS_PREFIX + ".configure.timer";
    private static final String RESERVE_TIMER = TIMERS_PREFIX + ".reserve.timer";
    private static final String RESOLVE_TIMER = TIMERS_PREFIX + ".resolve.timer";
    private static final String GET_SPECIFICATION_TIMER = TIMERS_PREFIX + ".getSpecification.timer";
    private static final String DRY_RUN_RESOLVE_TIMER = TIMERS_PREFIX + ".dryRunResolve.timer";
    private static final String CLAIM_TIMER = TIMERS_PREFIX + ".claim.timer";
    private static final String CHANGE_STATUS_TIMER = TIMERS_PREFIX + ".changeStatus.timer";
    private static final String GET_STATUS_TIMER = TIMERS_PREFIX + ".getStatus.timer";
    private static final String CHANGE_ARCHIVE_STATUS_TIMER = TIMERS_PREFIX + ".changeArchiveStatus.timer";
    private final AgentJobService agentJobService;
    private final JobServiceProtoConverter jobServiceProtoConverter;
    private final JobServiceProtoErrorComposer protoErrorComposer;
    private final MeterRegistry meterRegistry;

    /**
     * Constructor.
     *
     * @param agentJobService          The implementation of the {@link AgentJobService} to use
     * @param jobServiceProtoConverter DTO/Proto converter
     * @param protoErrorComposer       proto error message composer
     * @param meterRegistry            meter registry
     */
    public GRpcJobServiceImpl(
        final AgentJobService agentJobService,
        final JobServiceProtoConverter jobServiceProtoConverter,
        final JobServiceProtoErrorComposer protoErrorComposer,
        final MeterRegistry meterRegistry
    ) {
        this.agentJobService = agentJobService;
        this.jobServiceProtoConverter = jobServiceProtoConverter;
        this.protoErrorComposer = protoErrorComposer;
        this.meterRegistry = meterRegistry;
    }

    /**
     * This API gives the server a chance to reject a client/agent based on its metadata (version, location, ...).
     *
     * @param request          The request containing client metadata
     * @param responseObserver To send the response
     */
    @Override
    public void handshake(
        final HandshakeRequest request,
        final StreamObserver<HandshakeResponse> responseObserver
    ) {
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        try {
            final AgentClientMetadata agentMetadata =
                jobServiceProtoConverter.toAgentClientMetadataDto(request.getAgentMetadata());

            tags.add(Tag.of(AGENT_VERSION_TAG, agentMetadata.getVersion().orElse(UNKNOWN_VERSION)));

            agentJobService.handshake(agentMetadata);

            responseObserver.onNext(
                HandshakeResponse.newBuilder()
                    .setMessage("Agent is allowed to proceed")
                    .setType(HandshakeResponse.Type.ALLOWED)
                    .build()
            );

            MetricsUtils.addSuccessTags(tags);
        } catch (final Exception e) {
            log.error(e.getMessage(), e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            responseObserver.onNext(protoErrorComposer.toProtoHandshakeResponse(e));
        } finally {
            meterRegistry
                .timer(HANDSHAKE_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }

        responseObserver.onCompleted();
    }

    /**
     * This API provides runtime configuration to the agent (example: timeouts, parallelism, etc.).
     *
     * @param request          The agent request
     * @param responseObserver To send the response
     */
    @Override
    public void configure(
        final ConfigureRequest request,
        final StreamObserver<ConfigureResponse> responseObserver
    ) {
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        try {
            final AgentClientMetadata agentMetadata =
                jobServiceProtoConverter.toAgentClientMetadataDto(request.getAgentMetadata());

            tags.add(Tag.of(AGENT_VERSION_TAG, agentMetadata.getVersion().orElse(UNKNOWN_VERSION)));

            final Map<String, String> agentProperties = agentJobService.getAgentProperties(agentMetadata);

            responseObserver.onNext(
                ConfigureResponse.newBuilder()
                    .putAllProperties(agentProperties)
                    .build()
            );

            MetricsUtils.addSuccessTags(tags);
        } catch (final Exception e) {
            log.error(e.getMessage(), e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            responseObserver.onNext(protoErrorComposer.toProtoConfigureResponse(e));
        } finally {
            meterRegistry
                .timer(CONFIGURE_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }

        responseObserver.onCompleted();
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
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        try {
            final JobRequest jobRequest = jobServiceProtoConverter.toJobRequestDto(request);
            final AgentClientMetadata agentClientMetadata
                = jobServiceProtoConverter.toAgentClientMetadataDto(request.getAgentMetadata());

            tags.add(Tag.of(AGENT_VERSION_TAG, agentClientMetadata.getVersion().orElse(UNKNOWN_VERSION)));

            final String jobId = this.agentJobService.reserveJobId(jobRequest, agentClientMetadata);
            responseObserver.onNext(
                ReserveJobIdResponse.newBuilder()
                    .setId(jobId)
                    .build()
            );
            MetricsUtils.addSuccessTags(tags);
        } catch (final Exception e) {
            log.error(e.getMessage(), e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            responseObserver.onNext(protoErrorComposer.toProtoReserveJobIdResponse(e));
        } finally {
            meterRegistry
                .timer(RESERVE_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
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
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        try {
            final String id = request.getId();
            final JobSpecification jobSpec = this.agentJobService.resolveJobSpecification(id);
            responseObserver.onNext(jobServiceProtoConverter.toJobSpecificationResponseProto(jobSpec));
            MetricsUtils.addSuccessTags(tags);
        } catch (final Exception e) {
            log.error(e.getMessage(), e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            responseObserver.onNext(protoErrorComposer.toProtoJobSpecificationResponse(e));
        } finally {
            meterRegistry
                .timer(RESOLVE_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
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
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        try {
            final String id = request.getId();
            final JobSpecification jobSpecification = this.agentJobService.getJobSpecification(id);
            responseObserver.onNext(jobServiceProtoConverter.toJobSpecificationResponseProto(jobSpecification));
            MetricsUtils.addSuccessTags(tags);
        } catch (final Exception e) {
            log.error(e.getMessage(), e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            responseObserver.onNext(protoErrorComposer.toProtoJobSpecificationResponse(e));
        } finally {
            meterRegistry
                .timer(GET_SPECIFICATION_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
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
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        try {
            final JobRequest jobRequest = jobServiceProtoConverter.toJobRequestDto(request);
            final JobSpecification jobSpecification = this.agentJobService.dryRunJobSpecificationResolution(jobRequest);
            responseObserver.onNext(jobServiceProtoConverter.toJobSpecificationResponseProto(jobSpecification));
            MetricsUtils.addSuccessTags(tags);
        } catch (final Exception e) {
            log.error("Error resolving job specification for request " + request, e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            responseObserver.onNext(protoErrorComposer.toProtoJobSpecificationResponse(e));
        } finally {
            meterRegistry
                .timer(DRY_RUN_RESOLVE_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
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
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        try {
            final String id = request.getId();
            final AgentClientMetadata clientMetadata
                = jobServiceProtoConverter.toAgentClientMetadataDto(request.getAgentMetadata());

            tags.add(Tag.of(AGENT_VERSION_TAG, clientMetadata.getVersion().orElse(UNKNOWN_VERSION)));

            this.agentJobService.claimJob(id, clientMetadata);
            responseObserver.onNext(ClaimJobResponse.newBuilder().setSuccessful(true).build());
            MetricsUtils.addSuccessTags(tags);
        } catch (final Exception e) {
            log.error("Error claiming job for request " + request, e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            responseObserver.onNext(this.protoErrorComposer.toProtoClaimJobResponse(e));
        } finally {
            meterRegistry
                .timer(CLAIM_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
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
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        try {
            final String id = request.getId();
            final JobStatus currentStatus = JobStatus.valueOf(request.getCurrentStatus().toUpperCase());
            final JobStatus newStatus = JobStatus.valueOf(request.getNewStatus().toUpperCase());
            final String newStatusMessage = request.getNewStatusMessage();

            tags.add(Tag.of(STATUS_FROM_TAG, currentStatus.name()));
            tags.add(Tag.of(STATUS_TO_TAG, newStatus.name()));

            this.agentJobService.updateJobStatus(id, currentStatus, newStatus, newStatusMessage);
            responseObserver.onNext(ChangeJobStatusResponse.newBuilder().setSuccessful(true).build());
            MetricsUtils.addSuccessTags(tags);
        } catch (Exception e) {
            log.error("Error changing job status for request " + request, e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            responseObserver.onNext(protoErrorComposer.toProtoChangeJobStatusResponse(e));
        } finally {
            meterRegistry
                .timer(CHANGE_STATUS_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
        responseObserver.onCompleted();
    }

    /**
     * When the agent wants to confirm it's status is still the expected one (i.e. that the leader didn't mark the
     * job failed).
     *
     * @param request          The request containing the job id to look up
     * @param responseObserver The observer to send a response with
     */
    @Override
    public void getJobStatus(
        final GetJobStatusRequest request,
        final StreamObserver<GetJobStatusResponse> responseObserver
    ) {
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        final String id = request.getId();
        try {
            final JobStatus status = this.agentJobService.getJobStatus(id);
            responseObserver.onNext(GetJobStatusResponse.newBuilder().setStatus(status.name()).build());
            responseObserver.onCompleted();
            MetricsUtils.addSuccessTags(tags);
        } catch (Exception e) {
            log.error("Error retrieving job {} status: {}", id, e.getMessage());
            MetricsUtils.addFailureTagsWithException(tags, e);
            responseObserver.onError(e);
        } finally {
            meterRegistry
                .timer(GET_STATUS_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * When the agent wants to set the archival status of a job post-execution (i.e. archived successfully, or failed
     * to archive).
     *
     * @param request          The request containing the job id to look up
     * @param responseObserver The observer to send a response with
     */
    @Override
    public void changeJobArchiveStatus(
        final ChangeJobArchiveStatusRequest request,
        final StreamObserver<ChangeJobArchiveStatusResponse> responseObserver
    ) {
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();

        final String id = request.getId();
        final ArchiveStatus newArchiveStatus = ArchiveStatus.valueOf(request.getNewStatus());
        tags.add(Tag.of(STATUS_TO_TAG, newArchiveStatus.name()));

        try {
            this.agentJobService.updateJobArchiveStatus(id, newArchiveStatus);
            responseObserver.onNext(ChangeJobArchiveStatusResponse.newBuilder().build());
            responseObserver.onCompleted();
            MetricsUtils.addSuccessTags(tags);
        } catch (GenieJobNotFoundException e) {
            log.error("Cannot update archive status of job {}, job not found", id);
            MetricsUtils.addFailureTagsWithException(tags, e);
            responseObserver.onError(Status.NOT_FOUND.withCause(e).asException());
        } catch (Exception e) {
            log.error("Error retrieving job {} status: {}", id, e.getMessage());
            MetricsUtils.addFailureTagsWithException(tags, e);
            responseObserver.onError(e);
        } finally {
            meterRegistry
                .timer(CHANGE_ARCHIVE_STATUS_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }
}
