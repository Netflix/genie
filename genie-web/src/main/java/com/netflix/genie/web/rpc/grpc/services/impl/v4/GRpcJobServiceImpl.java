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

import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.dto.v4.JobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.dto.v4.converters.JobServiceProtoConverter;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.JobSpecificationRequest;
import com.netflix.genie.proto.JobSpecificationResponse;
import com.netflix.genie.proto.ReserveJobIdError;
import com.netflix.genie.proto.ReserveJobIdRequest;
import com.netflix.genie.proto.ReserveJobIdResponse;
import com.netflix.genie.web.rpc.grpc.interceptors.SimpleLoggingInterceptor;
import com.netflix.genie.web.services.AgentJobService;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;
import org.apache.commons.lang3.StringUtils;

/**
 * Extension of {@link JobServiceGrpc.JobServiceImplBase} to provide
 * functionality for resolving and fetching specifications for jobs to be run by the Genie Agent.
 *
 * @author tgianos
 * @since 4.0.0
 */
@GrpcService(
    value = JobServiceGrpc.class,
    interceptors = {
        SimpleLoggingInterceptor.class,
    }
)
@Slf4j
public class GRpcJobServiceImpl extends JobServiceGrpc.JobServiceImplBase {
    private final AgentJobService agentJobService;
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param agentJobService The implementation of the {@link AgentJobService} to use
     * @param registry        The metrics repository to use
     */
    public GRpcJobServiceImpl(final AgentJobService agentJobService, final MeterRegistry registry) {
        this.agentJobService = agentJobService;
        this.registry = registry;
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
        // TODO: Metrics
        try {
            final JobRequest jobRequest = JobServiceProtoConverter.toJobRequestDTO(request);
            final AgentClientMetadata agentClientMetadata
                = JobServiceProtoConverter.toAgentClientMetadataDTO(request.getAgentMetadata());
            responseObserver.onNext(
                ReserveJobIdResponse
                    .newBuilder()
                    .setId(this.agentJobService.reserveJobId(jobRequest, agentClientMetadata))
                    .build()
            );
        } catch (final Exception e) {
            log.error("Error reserving job id for request " + request, e);
            responseObserver.onNext(
                ReserveJobIdResponse
                    .newBuilder()
                    .setError(
                        ReserveJobIdError
                            .newBuilder()
                            .setException(e.getClass().getCanonicalName())
                            .setMessage(e.getMessage())
                    )
                    .build()
            );
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
        // TODO: Metrics?
        final String id = request.getId();
        if (StringUtils.isBlank(id)) {
            responseObserver.onNext(
                JobServiceProtoConverter.toProtoJobSpecificationResponse(
                    new GeniePreconditionException("No job id entered")
                )
            );
        } else {
            try {
                final JobSpecification jobSpec = this.agentJobService.resolveJobSpecification(id);
                responseObserver.onNext(JobServiceProtoConverter.toProtoJobSpecificationResponse(jobSpec));
            } catch (final Throwable t) {
                log.error(t.getMessage(), t);
                responseObserver.onNext(JobServiceProtoConverter.toProtoJobSpecificationResponse(t));
            }
        }
        responseObserver.onCompleted();
    }

    /**
     * Assuming a specification has already been resolved the agent will call this API with a job id to fetch the
     * specification. The server will mark the specification as owned.
     *
     * @param request          The request containing the job id to return the specification for
     * @param responseObserver How to send a response
     */
    @Override
    public void getJobSpecification(
        final JobSpecificationRequest request,
        final StreamObserver<JobSpecificationResponse> responseObserver
    ) {
        super.getJobSpecification(request, responseObserver);
    }
}
