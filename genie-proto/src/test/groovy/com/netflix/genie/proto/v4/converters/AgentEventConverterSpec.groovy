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

package com.netflix.genie.proto.v4.converters

import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.dto.v4.AgentEvent

import com.netflix.genie.proto.AgentEventRequest
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant

class AgentEventConverterSpec extends Specification {

    static final String AGENT_ID = "agent-1234"
    static final String JOB_ID = "job-1234"
    static final String CURRENT_STATE = "StateFoo"
    static final String FROM_STATE = "SomeState"
    static final String TO_STATE = "SomeOtherState"
    static final String ACTION_NAME = "com.netflix.genie.SomeStateAction"
    static final Throwable EXCEPTION = new RuntimeException("error", new IOException())
    static final JobStatus JOB_STATUS = JobStatus.RUNNING
    static final Instant TIMESTAMP = Instant.now()

    AgentEventConverter converter

    def setup() {
        converter = new AgentEventConverter()
    }

    @Unroll
    def "Convert to proto and back"(AgentEvent agentEvent) {
        when:
        def agentEventProto = converter.toProto(agentEvent)
        def agentEventDTO = converter.toDTO(agentEventProto)

        then:
        agentEvent == agentEventDTO

        where:
        agentEvent                                                                                      | _
        new AgentEvent.JobStatusUpdate(AGENT_ID, JOB_ID, JOB_STATUS, TIMESTAMP)                         | _
        new AgentEvent.StateChange(AGENT_ID, null, TO_STATE, TIMESTAMP)                       | _
        new AgentEvent.StateChange(AGENT_ID, FROM_STATE, TO_STATE, TIMESTAMP)                           | _
        new AgentEvent.StateActionExecution(AGENT_ID, CURRENT_STATE, ACTION_NAME, TIMESTAMP)            | _
        new AgentEvent.StateActionExecution(AGENT_ID, CURRENT_STATE, ACTION_NAME, EXCEPTION, TIMESTAMP) | _

    }

    @Unroll
    def "Convert invalid messages"(AgentEventRequest agentEventRequest) {
        when:
        converter.toDTO(agentEventRequest)

        then:
        thrown(IllegalArgumentException)

        where:
        agentEventRequest                      | _
        AgentEventRequest.newBuilder().build() | _
    }
}
