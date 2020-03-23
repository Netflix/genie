/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.agent.execution.statemachine


import org.springframework.statemachine.ExtendedState
import org.springframework.statemachine.StateContext
import org.springframework.statemachine.StateMachine
import spock.lang.Specification


class ExecutionStageSpec extends Specification {
    StateContext<States, Events> stateContext
    ExtendedState extendedState
    ExecutionContext executionContext
    StateMachine stateMachine
    int transitionExecutionCount
    Throwable transitionException

    void setup() {
        this.stateMachine = Mock(StateMachine)
        this.executionContext = Mock(ExecutionContext)
        this.extendedState = Mock(ExtendedState) {
            get(ExecutionStage.EXECUTION_CONTEXT_CONTEXT_KEY, ExecutionContext.class) >> executionContext
        }
        this.stateContext = Mock(StateContext) {
            getExtendedState() >> extendedState
            getStateMachine() >> stateMachine
        }
        this.transitionExecutionCount = 0;
        this.transitionException = null
    }

    def "Execution without errors"() {
        setup:
        int executionCount = 0
        ExecutionStage stage = new ExecutionStage(States.RELOCATE_LOG) {
            @Override
            protected void attemptTransition(final ExecutionContext executionContext) throws RetryableTransitionException, FatalTransitionException {
                executionCount += 1
            }
        }

        when: "State action runs"
        stage.getStateAction().execute(stateContext)

        then:
        1 * executionContext.setAttemptsLeft(1)
        1 * stateMachine.sendEvent(Events.PROCEED)

        when: "Transition action runs"
        stage.getTransitionAction().execute(stateContext)

        then:
        1 * executionContext.isExecutionAborted() >> false
        1 * executionContext.getAttemptsLeft() >> 2
        1 * executionContext.setAttemptsLeft(1)
        executionCount == 1
    }

    def "Reach a skip state after execution has been aborted"() {
        setup:
        ExecutionStage stage = new ExecutionStage(States.CONFIGURE_EXECUTION) {
            @Override
            protected void attemptTransition(final ExecutionContext executionContext) throws RetryableTransitionException, FatalTransitionException {
                throw new IllegalStateException("...")
            }
        }

        when: "Transition action should not execute"
        stage.getTransitionAction().execute(stateContext)

        then:
        1 * executionContext.isExecutionAborted() >> true
        1 * executionContext.getAttemptsLeft() >> 2
    }

    def "Reach a non-skip state after execution has been aborted, exhaust retries"() {
        setup:
        Throwable throwable = new RetryableTransitionException("...", null)
        ExecutionStage stage = new ExecutionStage(States.SET_STATUS_FINAL) {
            @Override
            protected void attemptTransition(final ExecutionContext executionContext) throws RetryableTransitionException, FatalTransitionException {
                throw throwable
            }
        }

        when: "Transition action attempt 1"
        stage.getTransitionAction().execute(stateContext)

        then:
        1 * executionContext.isExecutionAborted() >> true
        1 * executionContext.getAttemptsLeft() >> 2
        1 * executionContext.setAttemptsLeft(1)
        thrown(RetryableTransitionException)

        when: "Transition action error action for attempt 1"
        stage.getTransitionErrorAction().execute(stateContext)

        then:
        1 * stateContext.getException() >> throwable
        1 * executionContext.getAttemptsLeft() >> 1
        1 * executionContext.recordTransitionException(stage.getState(), throwable)
        1 * stateMachine.sendEvent(Events.PROCEED)

        when: "Transition action attempt 2"
        stage.getTransitionAction().execute(stateContext)

        then:
        1 * executionContext.isExecutionAborted() >> true
        1 * executionContext.getAttemptsLeft() >> 1
        1 * executionContext.setAttemptsLeft(0)
        thrown(RetryableTransitionException)

        when: "Transition action error action for attempt 2"
        stage.getTransitionErrorAction().execute(stateContext)

        then:
        1 * stateContext.getException() >> throwable
        1 * executionContext.getAttemptsLeft() >> 0
        1 * executionContext.recordTransitionException(stage.getState(), _ as FatalTransitionException) >> {
            args ->
                assert (args[1] as FatalTransitionException).getCause() == throwable
        }
        1 * executionContext.setExecutionAbortedFatalException(_ as FatalTransitionException)
        1 * stateMachine.sendEvent(Events.PROCEED)

        when: "Transition action skip"
        stage.getTransitionAction().execute(stateContext)

        then:
        1 * executionContext.isExecutionAborted() >> true
        1 * executionContext.getAttemptsLeft() >> 0
    }

    def "Retryable state stops transition attempts after fatal error"() {
        setup:
        Throwable cause = new IOException("...")
        Throwable throwable = new FatalTransitionException(States.RELOCATE_LOG, "...", cause)
        ExecutionStage stage = new ExecutionStage(States.RELOCATE_LOG) {
            @Override
            protected void attemptTransition(final ExecutionContext executionContext) throws RetryableTransitionException, FatalTransitionException {
                throw throwable
            }
        }

        when: "Transition action -- fatal error"
        stage.getTransitionAction().execute(stateContext)

        then:
        1 * executionContext.isExecutionAborted() >> false
        1 * executionContext.getAttemptsLeft() >> 4
        1 * executionContext.setAttemptsLeft(3)
        thrown(FatalTransitionException)

        when: "Transition action error action"
        stage.getTransitionErrorAction().execute(stateContext)

        then:
        1 * stateContext.getException() >> throwable
        1 * executionContext.getAttemptsLeft() >> 3
        1 * executionContext.recordTransitionException(stage.getState(), throwable)
        1 * executionContext.setAttemptsLeft(0)
        0 * executionContext.setExecutionAbortedFatalException(_)
        1 * stateMachine.sendEvent(Events.PROCEED)

        when: "Transition action skip"
        stage.getTransitionAction().execute(stateContext)

        then:
        1 * executionContext.isExecutionAborted() >> true
        1 * executionContext.getAttemptsLeft() >> 0
    }

    def "State stops transition attempts after unhandled error"() {
        setup:
        Throwable throwable = new IOException("...")
        ExecutionStage stage = new ExecutionStage(States.CREATE_JOB_DIRECTORY) {
            @Override
            protected void attemptTransition(final ExecutionContext executionContext) throws RetryableTransitionException, FatalTransitionException {
                throw throwable
            }
        }

        when: "Transition action -- unhandled error"
        stage.getTransitionAction().execute(stateContext)

        then:
        1 * executionContext.isExecutionAborted() >> false
        1 * executionContext.getAttemptsLeft() >> 4
        1 * executionContext.setAttemptsLeft(3)
        thrown(IOException)

        when: "Transition action error action"
        stage.getTransitionErrorAction().execute(stateContext)

        then:
        1 * stateContext.getException() >> throwable
        1 * executionContext.getAttemptsLeft() >> 3
        1 * executionContext.recordTransitionException(stage.getState(), _ as Throwable) >> {
            args ->
                assert (args[1] as FatalTransitionException).getCause() == throwable
        }
        1 * executionContext.setAttemptsLeft(0)
        1 * executionContext.setExecutionAbortedFatalException(_ as FatalTransitionException)
        1 * stateMachine.sendEvent(Events.PROCEED)

        when: "Transition action skip"
        stage.getTransitionAction().execute(stateContext)

        then:
        1 * executionContext.isExecutionAborted() >> true
        1 * executionContext.getAttemptsLeft() >> 0
    }
}
