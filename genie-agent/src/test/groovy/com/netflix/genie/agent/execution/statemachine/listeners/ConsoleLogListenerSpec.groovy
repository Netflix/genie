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
package com.netflix.genie.agent.execution.statemachine.listeners

import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException
import com.netflix.genie.agent.execution.statemachine.States
import spock.lang.Specification
import spock.lang.Unroll

class ConsoleLogListenerSpec extends Specification {

    def "Test all methods"() {
        setup:
        ConsoleLogListener listener = new ConsoleLogListener()

        when:
        listener.stateEntered(States.SET_STATUS_FINAL)
        listener.stateExited(States.SET_STATUS_FINAL)
        listener.beforeStateActionAttempt(States.SET_STATUS_FINAL)
        listener.afterStateActionAttempt(States.SET_STATUS_FINAL, null)
        listener.afterStateActionAttempt(States.SET_STATUS_FINAL, new RuntimeException())
        listener.stateMachineStarted()
        listener.stateMachineStopped()
        listener.stateSkipped()
        listener.fatalException(States.SET_STATUS_FINAL, new FatalJobExecutionException(States.SET_STATUS_FINAL, "..."))
        listener.executionAborted(States.SET_STATUS_FINAL, new FatalJobExecutionException(States.SET_STATUS_FINAL, "..."))
        listener.delayedStateActionRetry(States.SET_STATUS_FINAL, 200L)

        then:
        noExceptionThrown()
    }

    @Unroll
    def "Test message for action attempt: #state"() {
        setup:
        ConsoleLogListener listener = new ConsoleLogListener()

        when:
        listener.beforeStateActionAttempt(state)

        then:
        noExceptionThrown()

        where:
        state << States.values()
    }
}
