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

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.common.internal.dto.v4.AgentJobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.dto.v4.converters.JobSpecificationServiceConverter;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.JobSpecificationResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
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
public class GRpcAgentJobServiceImpl implements AgentJobService {

    private final JobServiceGrpc.JobServiceFutureStub client;

    /**
     * Constructor.
     *
     * @param client The gRPC client to use to call the server. Asynchronous version to allow timeouts.
     */
    public GRpcAgentJobServiceImpl(
        final JobServiceGrpc.JobServiceFutureStub client
    ) {
        this.client = client;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSpecification resolveJobSpecification(
        @NotBlank final String id
    ) throws JobSpecificationResolutionException {
        final ListenableFuture<JobSpecificationResponse> requestFuture = this.client
            .resolveJobSpecification(JobSpecificationServiceConverter.toProtoJobSpecificationRequest(id));

        return this.getAndHandleResponse(requestFuture);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSpecification getJobSpecification(@NotBlank final String id) throws JobSpecificationResolutionException {
        final ListenableFuture<JobSpecificationResponse> requestFuture = this.client
            .getJobSpecification(JobSpecificationServiceConverter.toProtoJobSpecificationRequest(id));

        return this.getAndHandleResponse(requestFuture);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSpecification resolveJobSpecificationDryRun(
        @Valid final AgentJobRequest jobRequest
    ) throws JobSpecificationResolutionException {
        throw new UnsupportedOperationException("Not yet implemented");
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

        return JobSpecificationServiceConverter.toJobSpecificationDTO(specResponse);

    }
}
