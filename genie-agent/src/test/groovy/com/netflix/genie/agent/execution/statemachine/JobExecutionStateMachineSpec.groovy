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

import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import org.springframework.statemachine.StateMachine
import org.springframework.statemachine.listener.StateMachineListener
import org.springframework.statemachine.state.State
import spock.lang.Specification

@Category(UnitTest.class)
class JobExecutionStateMachineSpec extends Specification {
    StateMachine<States, Events> sm
    void setup() {
         sm = Mock()
    }

    void cleanup() {
    }

    def "RunToCompletion"() {
        setup:
        def smImpl = new JobExecutionStateMachineImpl(sm)
        StateMachineListener<States, Events> capturedListener
        State<States,Events> endState = Mock()

        when:
        smImpl.start()

        then:
        1 * sm.addStateListener(_) >> {
            args -> capturedListener = args[0]
        }
        1 * sm.start()

        when:
        capturedListener.stateMachineStopped(sm)
        smImpl.waitForStop()

        then:
        1 * sm.getState() >> endState
        1 * endState.getId() >> States.END
    }
}
