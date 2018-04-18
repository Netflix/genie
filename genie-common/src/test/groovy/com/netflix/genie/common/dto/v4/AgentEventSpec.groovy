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

package com.netflix.genie.common.dto.v4

import com.netflix.genie.common.dto.JobStatus
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant

class AgentEventSpec extends Specification {

    static final String AGENT_ID = "agent-1234"
    static final String JOB_ID = "job-1234"
    static final JobStatus JOB_STATUS = JobStatus.RUNNING
    static final String CURRENT_STATE = "StateFoo"
    static final String FROM_STATE = "SomeState"
    static final String TO_STATE = "SomeOtherState"
    static final String ACTION_NAME = "com.netflix.genie.SomeStateAction"
    static final Exception EXCEPTION = new RuntimeException("error", new IOException())
    static final Instant TIMESTAMP = Instant.now()

    @Unroll
    def "Check event fields"(AgentEvent agentEvent) {
        when:
        agentEvent.toString()

        then:
        noExceptionThrown()
        validateEventFields(agentEvent)
        agentEvent == agentEvent

        where:
        agentEvent                                                                                      | _
        new AgentEvent.JobStatusUpdate(AGENT_ID, JOB_ID, JOB_STATUS, TIMESTAMP)                         | _
        new AgentEvent.StateChange(AGENT_ID, FROM_STATE, TO_STATE, TIMESTAMP)                           | _
        new AgentEvent.StateActionExecution(AGENT_ID, CURRENT_STATE, ACTION_NAME, TIMESTAMP)            | _
        new AgentEvent.StateActionExecution(AGENT_ID, CURRENT_STATE, ACTION_NAME, EXCEPTION, TIMESTAMP) | _
    }

    @Unroll
    def "Validate auto-generated timestamp of event"(AgentEvent agentEvent) {

        setup:
        final long MAX_DELTA = 2 //Max delta in seconds from creation to now()

        expect:
        Instant.now().getEpochSecond() - agentEvent.getTimestamp().getEpochSecond() <= MAX_DELTA

        where:
        agentEvent                                                                           | _
        new AgentEvent.JobStatusUpdate(AGENT_ID, JOB_ID, JOB_STATUS)                         | _
        new AgentEvent.StateChange(AGENT_ID, FROM_STATE, TO_STATE)                           | _
        new AgentEvent.StateActionExecution(AGENT_ID, CURRENT_STATE, ACTION_NAME)            | _
        new AgentEvent.StateActionExecution(AGENT_ID, CURRENT_STATE, ACTION_NAME, EXCEPTION) | _
    }

    def "Validate too many optional arguments"() {

        when:
        new AgentEvent.JobStatusUpdate(AGENT_ID, JOB_ID, JOB_STATUS, TIMESTAMP, TIMESTAMP)

        then:
        thrown(IllegalArgumentException)

        when:
        new AgentEvent.StateChange(AGENT_ID, FROM_STATE, TO_STATE, TIMESTAMP, TIMESTAMP)

        then:
        thrown(IllegalArgumentException)

        when:
        new AgentEvent.StateActionExecution(AGENT_ID, CURRENT_STATE, ACTION_NAME, TIMESTAMP, TIMESTAMP)

        then:
        thrown(IllegalArgumentException)

        when:
        new AgentEvent.StateActionExecution(AGENT_ID, CURRENT_STATE, ACTION_NAME, EXCEPTION, TIMESTAMP, TIMESTAMP)

        then:
        thrown(IllegalArgumentException)
    }

    def validateEventFields(final AgentEvent agentEvent) {
        assert agentEvent.getAgentId() == AGENT_ID
        assert agentEvent.getTimestamp() == TIMESTAMP

        if (agentEvent instanceof AgentEvent.JobStatusUpdate) {
            assert agentEvent.getJobId() == JOB_ID
            assert agentEvent.getJobStatus() == JOB_STATUS

        } else if (agentEvent instanceof AgentEvent.StateChange) {
            assert agentEvent.getFromState() == FROM_STATE
            assert agentEvent.getToState() == TO_STATE

        } else if (agentEvent instanceof AgentEvent.StateActionExecution) {
            assert agentEvent.getState() == CURRENT_STATE
            assert agentEvent.getAction() == ACTION_NAME
            if (agentEvent.isActionException()) {
                assert agentEvent.getExceptionClass() == EXCEPTION.getClass().getCanonicalName()
                assert agentEvent.getExceptionMessage() == EXCEPTION.getMessage()
                assert agentEvent.getExceptionTrace().size() == 2
                assert agentEvent.getExceptionTrace().get(0).contains(EXCEPTION.getClass().getCanonicalName())
                assert agentEvent.getExceptionTrace().get(0).contains(EXCEPTION.getMessage())
                assert agentEvent.getExceptionTrace().get(1).contains(EXCEPTION.getCause().getClass().getCanonicalName())
                assert agentEvent.getExceptionTrace().get(1).contains("null")
            } else {
                assert agentEvent.getExceptionClass() == null
                assert agentEvent.getExceptionMessage() == null
                assert agentEvent.getExceptionTrace() == null
            }

        } else {
            assert false
        }
        return true
    }

}
