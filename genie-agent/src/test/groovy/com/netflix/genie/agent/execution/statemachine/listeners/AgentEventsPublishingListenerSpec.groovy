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

package com.netflix.genie.agent.execution.statemachine.listeners

import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.services.AgentEventsService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.agent.execution.statemachine.States
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import org.springframework.statemachine.StateMachine
import org.springframework.statemachine.action.Action
import org.springframework.statemachine.state.State
import spock.lang.Specification

@Category(UnitTest)
class AgentEventsPublishingListenerSpec extends Specification {
    ExecutionContext executionContext
    AgentEventsService agentEventService
    AgentEventsPublishingListener listener

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.agentEventService = Mock(AgentEventsService)
        this.listener = new AgentEventsPublishingListener(executionContext, agentEventService)
    }

    def "StateChanged"() {
        setup:
        def fromState = Mock(State)
        def toState = Mock(State)

        when:
        listener.stateChanged(fromState, toState)

        then:
        1 * fromState.getId() >> States.CONFIGURE_AGENT
        1 * toState.getId() >> States.SETUP_JOB
        1 * agentEventService.emitStateChange(States.CONFIGURE_AGENT, States.SETUP_JOB)
    }

    def "StateChanged from null"() {
        setup:
        def fromState = null
        def toState = Mock(State)

        when:
        listener.stateChanged(fromState, toState)

        then:
        1 * toState.getId() >> States.SETUP_JOB
        1 * agentEventService.emitStateChange(null, States.SETUP_JOB)
    }

    def "StateEntered -- successful execution"() {
        setup:
        State<States, Events> stateMock = Mock()

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.READY
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.INITIALIZE
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.CONFIGURE_AGENT
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.RESOLVE_JOB_SPECIFICATION
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.SETUP_JOB
        1 * agentEventService.emitJobStatusUpdate(JobStatus.INIT)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.LAUNCH_JOB
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.MONITOR_JOB
        1 * agentEventService.emitJobStatusUpdate(JobStatus.RUNNING)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.CLEANUP_JOB
        1 * executionContext.getFinalJobStatus() >> JobStatus.SUCCEEDED
        1 * agentEventService.emitJobStatusUpdate(JobStatus.SUCCEEDED)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.SHUTDOWN
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.END
        0 * agentEventService.emitJobStatusUpdate(_)

    }

    def "StateEntered -- failed execution"() {
        setup:
        State<States, Events> stateMock = Mock()

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.READY
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.INITIALIZE
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.CONFIGURE_AGENT
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.RESOLVE_JOB_SPECIFICATION
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.SETUP_JOB
        1 * agentEventService.emitJobStatusUpdate(JobStatus.INIT)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.LAUNCH_JOB
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.MONITOR_JOB
        1 * agentEventService.emitJobStatusUpdate(JobStatus.RUNNING)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.CLEANUP_JOB
        1 * executionContext.getFinalJobStatus() >> JobStatus.FAILED
        1 * agentEventService.emitJobStatusUpdate(JobStatus.FAILED)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.SHUTDOWN
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.END
        0 * agentEventService.emitJobStatusUpdate(_)

    }

    def "StateEntered -- error before init"() {
        setup:
        State<States, Events> stateMock = Mock()

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.READY
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.INITIALIZE
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.CONFIGURE_AGENT
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.HANDLE_ERROR
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.END
        0 * agentEventService.emitJobStatusUpdate(_)

    }

    def "StateEntered -- error after init"() {
        setup:
        State<States, Events> stateMock = Mock()

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.READY
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.INITIALIZE
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.CONFIGURE_AGENT
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.RESOLVE_JOB_SPECIFICATION
        0 * agentEventService.emitJobStatusUpdate(_)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.SETUP_JOB
        1 * agentEventService.emitJobStatusUpdate(JobStatus.INIT)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.HANDLE_ERROR
        1 * agentEventService.emitJobStatusUpdate(JobStatus.FAILED)

        when:
        listener.stateEntered(stateMock)
        then:
        1 * stateMock.getId() >> States.END
        0 * agentEventService.emitJobStatusUpdate(_)
    }
}

