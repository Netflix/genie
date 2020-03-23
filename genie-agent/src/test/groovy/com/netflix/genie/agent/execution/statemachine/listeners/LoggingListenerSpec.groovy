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
import org.assertj.core.util.Lists
import org.springframework.messaging.Message
import org.springframework.statemachine.StateContext
import org.springframework.statemachine.StateMachine
import org.springframework.statemachine.action.Action
import org.springframework.statemachine.state.State
import org.springframework.statemachine.transition.Transition
import org.springframework.statemachine.transition.TransitionKind
import org.springframework.statemachine.trigger.Trigger
import spock.lang.Specification

class LoggingListenerSpec extends Specification {
    LoggingListener listener
    StateMachine<States, Events> stateMachine
    State<States, Events> state
    State<States, Events> nullState
    Events event
    Transition<States, Events> transition
    Trigger<States, Events> trigger
    Exception exception
    StateContext<States, Events> stateContext
    Action<States, Events> action

    void setup() {
        this.listener = new LoggingListener()
        this.state = Mock(State)
        this.nullState = null
        this.event = Events.PROCEED
        this.transition = Mock(Transition)
        this.trigger = Mock(Trigger)
        this.stateMachine = Mock(StateMachine)
        this.exception = Mock(Exception)
        this.stateContext = Mock(StateContext)
        this.action = Mock(Action)
    }

    void cleanup() {
    }

    def "StateChanged"() {
        when:
        listener.stateChanged(state, state)

        then:
        2 * state.getId() >> States.HANDSHAKE
    }

    def "StateChangedFromNull"() {
        when:
        listener.stateChanged(nullState, state)

        then:
        1 * state.getId() >> States.HANDSHAKE
    }

    def "StateEntered"() {
        when:
        listener.stateEntered(state)

        then:
        1 * state.getId() >> States.HANDSHAKE
        2 * state.getEntryActions() >> Lists.newArrayList()
        2 * state.getExitActions() >> Lists.newArrayList()
    }

    def "StateEnteredWithNullActions"() {
        when:
        listener.stateEntered(state)

        then:
        1 * state.getId() >> States.HANDSHAKE
        1 * state.getEntryActions() >> null
        1 * state.getExitActions() >> null
    }

    def "StateExited"() {
        when:
        listener.stateExited(state)

        then:
        1 * state.getId() >> States.HANDSHAKE
    }

    def "EventNotAccepted"() {
        Message<Events> message = Mock(Message)

        when:
        listener.eventNotAccepted(message)

        then:
        1 * message.getPayload() >> event
    }

    def "Transition"() {
        when:
        listener.transition(transition)

        then:
        1 * transition.getSource() >> state
        1 * transition.getTarget() >> state
        2 * state.getId() >> States.HANDSHAKE
        2 * transition.getActions() >> Lists.newArrayList()
        1 * transition.getTrigger() >> trigger
        1 * trigger.getEvent() >> event
    }

    def "TransitionWithActionsFromNullState"() {
        when:
        listener.transition(transition)

        then:
        1 * transition.getSource() >> nullState
        1 * transition.getTarget() >> state
        1 * state.getId() >> States.HANDSHAKE
        1 * transition.getActions() >> null
        1 * transition.getTrigger() >> null
    }

    def "TransitionStarted"() {
        when:
        listener.transitionStarted(transition)

        then:
        1 * transition.getKind() >> TransitionKind.EXTERNAL
        1 * transition.getSource() >> state
        1 * transition.getTarget() >> state
        2 * state.getId() >> States.HANDSHAKE
        2 * transition.getActions() >> Lists.newArrayList()
        1 * transition.getTrigger() >> trigger
        1 * trigger.getEvent() >> Events.START

    }

    def "TransitionStartedWithoutEvent"() {
        when:
        listener.transitionStarted(transition)

        then:
        1 * transition.getKind() >> TransitionKind.EXTERNAL
        1 * transition.getSource() >> state
        1 * transition.getTarget() >> state
        2 * state.getId() >> States.HANDSHAKE
        2 * transition.getActions() >> Lists.newArrayList()
        1 * transition.getTrigger() >> trigger
        1 * trigger.getEvent() >> null

    }

    def "TransitionEnded"() {
        when:
        listener.transitionEnded(transition)

        then:
        1 * transition.getKind() >> TransitionKind.EXTERNAL
        1 * transition.getSource() >> state
        1 * transition.getTarget() >> state
        2 * state.getId() >> States.HANDSHAKE
    }

    def "StateMachineStarted"() {
        when:
        listener.stateMachineStarted(stateMachine)

        then:
        true
    }

    def "StateMachineStopped"() {
        when:
        listener.stateMachineStopped(stateMachine)

        then:
        1 * stateMachine.getState() >> state
        1 * state.getId() >> States.DONE
    }

    def "StateMachineError"() {
        when:
        listener.stateMachineError(stateMachine, exception)

        then:
        1 * exception.getMessage() >> "Error"
    }

    def "ExtendedStateChanged"() {
        when:
        listener.extendedStateChanged("Foo", "bar")

        then:
        true
    }

    def "StateContext"() {
        when:
        listener.stateContext(stateContext)

        then:
        true
    }

    def "OnExecute"() {
        when:
        listener.onExecute(stateMachine, action, 100)

        then:
        1 * stateMachine.getState() >> state
        1 * state.getId() >> States.HANDSHAKE
    }
}
