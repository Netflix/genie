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

import com.netflix.genie.agent.execution.services.AgentJobSpecificationService;
import com.netflix.genie.common.dto.v4.JobRequest;
import com.netflix.genie.common.dto.v4.JobSpecification;
import com.netflix.genie.proto.JobSpecificationServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.validation.Valid;

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
    public JobSpecification resolveJobSpecification(@Valid final JobRequest jobRequest) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSpecification getJobSpecification(final String id) {
        return null;
    }
}
