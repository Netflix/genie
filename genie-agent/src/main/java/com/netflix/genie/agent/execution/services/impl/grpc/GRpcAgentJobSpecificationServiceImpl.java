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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException;
import com.netflix.genie.agent.execution.services.AgentJobSpecificationService;
import com.netflix.genie.common.dto.v4.AgentJobRequest;
import com.netflix.genie.common.dto.v4.JobSpecification;
import com.netflix.genie.proto.GetJobSpecificationRequest;
import com.netflix.genie.proto.JobSpecificationResponse;
import com.netflix.genie.proto.JobSpecificationServiceGrpc;
import com.netflix.genie.proto.ResolveJobSpecificationRequest;
import com.netflix.genie.proto.v4.adapters.JobSpecificationServiceAdapter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.util.concurrent.ExecutionException;

/**
 * Implementation of the job specification service.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Lazy
@Service
@Slf4j
public class GRpcAgentJobSpecificationServiceImpl implements AgentJobSpecificationService {

    private final JobSpecificationServiceGrpc.JobSpecificationServiceFutureStub client;

    /**
     * Constructor.
     *
     * @param client The gRPC client to use to call the server. Asynchronous version to allow timeouts.
     */
    public GRpcAgentJobSpecificationServiceImpl(
        final JobSpecificationServiceGrpc.JobSpecificationServiceFutureStub client
    ) {
        this.client = client;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSpecification resolveJobSpecification(
        @Valid final AgentJobRequest agentJobRequest
    ) throws JobSpecificationResolutionException {

        final ResolveJobSpecificationRequest resolveRequest;
        try {
            resolveRequest = JobSpecificationServiceAdapter.toProtoResolveJobSpecificationRequest(agentJobRequest);
        } catch (final JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid job request for job specification", e);
        }

        final ListenableFuture<JobSpecificationResponse> requestFuture = client.resolveJobSpecification(resolveRequest);

        return getAndHandleResponse(requestFuture);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSpecification getJobSpecification(@NotEmpty final String id) throws JobSpecificationResolutionException {

        final GetJobSpecificationRequest getRequest = GetJobSpecificationRequest.newBuilder()
            .setId(id)
            .build();

        final ListenableFuture<JobSpecificationResponse> requestFuture = client.getJobSpecification(getRequest);

        return getAndHandleResponse(requestFuture);
    }

    private JobSpecification getAndHandleResponse(
        final ListenableFuture<JobSpecificationResponse> requestFuture
    ) throws JobSpecificationResolutionException {
        final JobSpecificationResponse specResponse;
        try {
            specResponse = requestFuture.get();
        } catch (final ExecutionException | InterruptedException e) {
            throw new JobSpecificationResolutionException("Failed to resolve job specification", e);
        }

        if (specResponse.getResponseCase() == JobSpecificationResponse.ResponseCase.ERROR) {
            throw new JobSpecificationResolutionException(
                "Failed to resolve specification, server responded with error:"
                    + specResponse.getResponseCase().toString()
                    + " - "
                    + specResponse.getError().getMessage()
            );
        } else if (specResponse.getResponseCase() == JobSpecificationResponse.ResponseCase.RESPONSE_NOT_SET) {
            throw new JobSpecificationResolutionException(
                "Failed to resolve specification, server sent an empty response"
                    + specResponse.getResponseCase().toString()
            );
        }

        return JobSpecificationServiceAdapter.toJobSpecificationDTO(specResponse);

    }
}
