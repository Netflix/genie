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
package com.netflix.genie.agent.execution.statemachine

import com.netflix.genie.agent.execution.services.KillService
import org.springframework.statemachine.ExtendedState
import org.springframework.statemachine.StateMachine
import org.springframework.statemachine.listener.StateMachineListener
import org.springframework.statemachine.state.State
import spock.lang.Specification

class JobExecutionStateMachineImplSpec extends Specification {

    def "RunAndWaitForStop"() {
        StateMachine<States, Events> stateMachine = Mock(StateMachine)
        ExecutionContext executionContext = Mock(ExecutionContext)
        ExtendedState extendedState = Mock(ExtendedState)
        Map<Object, Object> extendedStateVariables = Mock(Map)
        KillService.KillEvent killEvent = new KillService.KillEvent(KillService.KillSource.API_KILL_REQUEST)
        State<States, Events> state = Mock()
        StateMachineListener listener
        List<ExecutionStage> executionStages = Mock()

        when:
        JobExecutionStateMachineImpl jobstateMachine = new JobExecutionStateMachineImpl(stateMachine, executionContext, executionStages)

        then:
        1 * stateMachine.getExtendedState() >> extendedState
        1 * extendedState.getVariables() >> extendedStateVariables
        1 * extendedStateVariables.put(ExecutionStage.EXECUTION_CONTEXT_CONTEXT_KEY, executionContext)

        when:
        jobstateMachine.start()

        then:
        1 * stateMachine.addStateListener(_ as StateMachineListener) >> {
            args ->
                listener = args[0] as StateMachineListener

        }
        listener != null
        1 * stateMachine.start()
        1 * stateMachine.sendEvent(Events.START)

        when:
        jobstateMachine.onApplicationEvent(killEvent)

        then:
        1 * executionContext.setJobKilled(true)
        1 * stateMachine.getState() >> state
        1 * state.getId() >> States.CREATE_JOB_DIRECTORY

        when:
        listener.stateMachineStopped(stateMachine)
        jobstateMachine.waitForStop()

        then:
        1 * stateMachine.getState() >> state
        1 * state.getId() >> States.DONE
    }
}
