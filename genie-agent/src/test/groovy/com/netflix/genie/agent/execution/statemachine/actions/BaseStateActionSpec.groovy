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

package com.netflix.genie.agent.execution.statemachine.actions

import com.google.common.collect.Lists
import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.agent.execution.statemachine.States
import org.springframework.statemachine.StateContext
import org.springframework.statemachine.StateMachine
import org.springframework.statemachine.action.Action
import org.springframework.statemachine.state.State
import spock.lang.Specification

class BaseStateActionSpec extends Specification {
    StateContext<States, Events> stateContext
    StateMachine<States, Events> stateMachine
    State<States, Events> state
    ExecutionContext executionContext
    List<StateAction> cleanupList = Lists.newArrayList()

    void setup() {
        this.stateContext = Mock(StateContext)
        this.stateMachine = Mock(StateMachine)
        this.state = Mock(State)
        this.executionContext = Mock(ExecutionContext)

        executionContext.getCleanupActions() >> cleanupList
    }

    void cleanup() {
    }

    def "Execute"() {
        setup:
        def stateAction = new BaseStateAction(executionContext) {
            @Override
            protected Events executeStateAction(ExecutionContext executionContext) {
                return Events.INITIALIZE_COMPLETE
            }
        }

        when:
        stateAction.execute(stateContext)

        then:
        2 * stateContext.getStateMachine() >> stateMachine
        1 * stateMachine.getState() >> state
        1 * state.getId() >> States.READY
        1 * executionContext.addCleanupActions(stateAction)
        1 * stateMachine.sendEvent(Events.INITIALIZE_COMPLETE) >> true
    }

    def "ExecuteThrows"() {
        setup:
        def exception = new RuntimeException()
        def stateAction = new BaseStateAction(executionContext) {
            @Override
            protected Events executeStateAction(ExecutionContext executionContext) {
                throw exception
            }
        }

        when:
        stateAction.execute(stateContext)

        then:
        2 * stateContext.getStateMachine() >> stateMachine
        1 * stateMachine.getState() >> state
        1 * state.getId() >> States.READY
        1 * executionContext.addCleanupActions(stateAction)
        1 * stateMachine.sendEvent(Events.ERROR) >> true
        1 * executionContext.addStateActionError(States.READY, stateAction.getClass() as Class<? extends Action>, exception)
    }
}
