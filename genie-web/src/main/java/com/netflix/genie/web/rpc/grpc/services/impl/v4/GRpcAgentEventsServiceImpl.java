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

import com.netflix.genie.common.dto.v4.AgentEvent;
import com.netflix.genie.proto.AgentEventRequest;
import com.netflix.genie.proto.AgentEventResponse;
import com.netflix.genie.proto.AgentEventsServiceGrpc;
import com.netflix.genie.proto.JobSpecificationServiceGrpc;
import com.netflix.genie.proto.v4.converters.AgentEventConverter;
import com.netflix.genie.web.rpc.interceptors.SimpleLoggingInterceptor;
import com.netflix.genie.web.services.AgentEventsService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;

/**
 * Implementation of gRPC JobExecutionEventService that handles events sent by agents running jobs.
 *
 * @author mprimi
 * @since 4.0.0
 */
@GrpcService(
    value = JobSpecificationServiceGrpc.class,
    interceptors = {
        SimpleLoggingInterceptor.class,
    }
)
@Slf4j
class GRpcAgentEventsServiceImpl extends AgentEventsServiceGrpc.AgentEventsServiceImplBase {

    private final AgentEventConverter agententEventConverter;
    private final AgentEventsService agentEventsService;

    GRpcAgentEventsServiceImpl(
        final AgentEventConverter agententEventConverter,
        final AgentEventsService agentEventsService
    ) {
        this.agententEventConverter = agententEventConverter;
        this.agentEventsService = agentEventsService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publishEvent(
        final AgentEventRequest request,
        final StreamObserver<AgentEventResponse> responseObserver
    ) {
        final AgentEvent agentEvent = agententEventConverter.toDTO(request);

        switch (request.getEventTypeCase()) {
            case JOB_STATUS_UPDATE_EVENT:
                agentEventsService.handleAgentEvent(((AgentEvent.JobStatusUpdate) agentEvent));
                break;
            case STATE_CHANGE_EVENT:
                agentEventsService.handleAgentEvent(((AgentEvent.StateChange) agentEvent));
                break;
            case STATE_ACTION_EXECUTION_EVENT:
                agentEventsService.handleAgentEvent(((AgentEvent.StateActionExecution) agentEvent));
                break;

            default:
                //TODO better strategy for handling invalid messages, for the moment acknowledge.
                log.warn("Unhandled event message class: {}", agentEvent.getClass().getCanonicalName());
        }

        responseObserver.onNext(AgentEventResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
