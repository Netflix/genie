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

import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.agent.execution.statemachine.States
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import org.springframework.statemachine.StateContext
import spock.lang.Specification

@Category(UnitTest.class)
class SetUpJobActionSpec extends Specification {
    StateContext<States, Events> stateContext
    SetUpJobAction action

    void setup() {
        this.stateContext = Mock(StateContext)
        this.action = new SetUpJobAction()
    }

    void cleanup() {
    }

    def "ExecuteStateAction"() {
        when:
        def event = action.executeStateAction(stateContext)
        then:
        event == Events.SETUP_JOB_COMPLETE
    }
}
