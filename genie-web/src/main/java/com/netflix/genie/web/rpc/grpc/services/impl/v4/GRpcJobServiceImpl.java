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

import com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.dto.v4.converters.JobSpecificationServiceConverter;
import com.netflix.genie.proto.GetJobSpecificationRequest;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.JobSpecificationResponse;
import com.netflix.genie.proto.ResolveJobSpecificationRequest;
import com.netflix.genie.web.rpc.interceptors.SimpleLoggingInterceptor;
import com.netflix.genie.web.services.JobSpecificationService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

/**
 * Extension of {@link com.netflix.genie.proto.JobServiceGrpc.JobServiceImplBase} to provide
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

    private final JobSpecificationService jobSpecificationService;

    /**
     * Constructor.
     *
     * @param jobSpecificationService The implementation of the job specification service to use.
     */
    public GRpcJobServiceImpl(final JobSpecificationService jobSpecificationService) {
        this.jobSpecificationService = jobSpecificationService;
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
        final ResolveJobSpecificationRequest request,
        final StreamObserver<JobSpecificationResponse> responseObserver
    ) {
        // TODO: Make this better after the database interaction is done. Particularly the ID resolution.
        // TODO: Metrics?
        final String id = StringUtils.isEmpty(request.getId())
            ? UUID.randomUUID().toString()
            : request.getId();
        try {
            final ExecutionResourceCriteria criteria
                = JobSpecificationServiceConverter.toExecutionResourceCriteriaDTO(request);
            final JobSpecification jobSpec = this.jobSpecificationService.resolveJobSpecification(id, criteria);
            responseObserver.onNext(JobSpecificationServiceConverter.toProtoJobSpecificationResponse(jobSpec));
        } catch (final Throwable t) {
            log.error(t.getMessage(), t);
            responseObserver.onNext(JobSpecificationServiceConverter.toProtoJobSpecificationResponse(t));
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
        final GetJobSpecificationRequest request,
        final StreamObserver<JobSpecificationResponse> responseObserver
    ) {
        super.getJobSpecification(request, responseObserver);
    }
}
