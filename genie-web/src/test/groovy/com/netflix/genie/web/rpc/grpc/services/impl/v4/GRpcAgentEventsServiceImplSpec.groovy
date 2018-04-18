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

package com.netflix.genie.web.rpc.grpc.services.impl.v4

import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.dto.v4.AgentEvent
import com.netflix.genie.proto.AgentEventRequest
import com.netflix.genie.proto.AgentEventResponse
import com.netflix.genie.proto.v4.converters.AgentEventConverter
import com.netflix.genie.web.services.AgentEventsService
import io.grpc.stub.StreamObserver
import spock.lang.Specification
import spock.lang.Unroll

class GRpcAgentEventsServiceImplSpec extends Specification {

    static AgentEventConverter converter = new AgentEventConverter()
    static String agentId = UUID.randomUUID().toString()
    static JobStatus jobStatus = JobStatus.SUCCEEDED
    static String currentState = "MONITOR_JOB"
    static String actionName = "com.netflix.genie.agent.StateAction.CleanUp"
    static String jobId = UUID.randomUUID().toString()
    static Throwable exception = new RuntimeException(new IOException("fail"))
    GRpcAgentEventsServiceImpl service
    StreamObserver responseObserver
    AgentEventConverter agententEventConverter
    AgentEventsService agentEventsService

    void setup() {
        this.agententEventConverter = Mock(AgentEventConverter)
        this.agentEventsService = Mock(AgentEventsService)
        this.service = new GRpcAgentEventsServiceImpl(agententEventConverter, agentEventsService)
        this.responseObserver = Mock(StreamObserver)
    }

    void cleanup() {
    }

    @Unroll
    def "PublishEvent"(AgentEventRequest agentEventRequest) {
        setup:
        def agentEventDTO

        switch (agentEventRequest.eventTypeCase) {
            case AgentEventRequest.EventTypeCase.STATE_CHANGE_EVENT:
                agentEventDTO = Mock(AgentEvent.StateChange)
                break
            case AgentEventRequest.EventTypeCase.JOB_STATUS_UPDATE_EVENT:
                agentEventDTO = Mock(AgentEvent.JobStatusUpdate)
                break
            case AgentEventRequest.EventTypeCase.STATE_ACTION_EXECUTION_EVENT:
                agentEventDTO = Mock(AgentEvent.StateActionExecution)
        }

        when:
        service.publishEvent(agentEventRequest, responseObserver)

        then:
        1 * agententEventConverter.toDTO(agentEventRequest) >> agentEventDTO
        1 * agentEventsService.handleAgentEvent(agentEventDTO)
        1 * responseObserver.onNext(AgentEventResponse.getDefaultInstance())
        1 * responseObserver.onCompleted()

        where:
        agentEventRequest                                                                                    | _
        converter.toProto(new AgentEvent.JobStatusUpdate(agentId, jobId, jobStatus))                         | _
        converter.toProto(new AgentEvent.StateChange(agentId, currentState, currentState))                   | _
        converter.toProto(new AgentEvent.StateActionExecution(agentId, currentState, actionName))            | _
        converter.toProto(new AgentEvent.StateActionExecution(agentId, currentState, actionName, exception)) | _
    }
}
