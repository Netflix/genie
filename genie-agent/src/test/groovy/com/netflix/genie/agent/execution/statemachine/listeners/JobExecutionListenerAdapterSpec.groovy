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

import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.agent.execution.statemachine.States
import org.springframework.messaging.Message
import org.springframework.statemachine.StateContext
import org.springframework.statemachine.StateMachine
import org.springframework.statemachine.action.Action
import org.springframework.statemachine.state.State
import org.springframework.statemachine.transition.Transition
import spock.lang.Specification

class JobExecutionListenerAdapterSpec extends Specification {
    def "Verify noop"() {
        setup:
        def adapter = new JobExecutionListenerAdapter()

        StateMachine<States, Events> stateMachine = Mock()
        Action<States, Events> action = Mock()
        long duration = 1234
        State<States, Events> stateFrom = Mock()
        State<States, Events> stateTo = Mock()
        State<States, Events> state = Mock()
        Message<Events> event = Mock()
        Transition<States, Events> transition = Mock()
        Exception exception = Mock()
        Object key = Mock()
        Object value = Mock()
        StateContext<States, Events> stateContext = Mock()

        when:
        adapter.onExecute(stateMachine, action, duration)
        adapter.stateChanged(stateFrom, stateTo)
        adapter.stateEntered(state)
        adapter.stateExited(state)
        adapter.eventNotAccepted(event)
        adapter.transition(transition)
        adapter.transitionStarted(transition)
        adapter.transitionEnded(transition)
        adapter.stateMachineStarted(stateMachine)
        adapter.stateMachineStopped(stateMachine)
        adapter.stateMachineError(stateMachine, exception)
        adapter.extendedStateChanged(key, value)
        adapter.stateContext(stateContext)

        then:
        0 * stateMachine._
        0 * action._
        0 * stateFrom._
        0 * stateTo._
        0 * state._
        0 * event._
        0 * transition._
        0 * exception._
        0 * key._
        0 * value._
        0 * stateContext._
    }
}
