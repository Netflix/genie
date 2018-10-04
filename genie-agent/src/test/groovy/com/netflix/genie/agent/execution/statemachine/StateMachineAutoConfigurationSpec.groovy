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

import com.netflix.genie.agent.execution.statemachine.listeners.JobExecutionListener
import com.netflix.genie.agent.execution.statemachine.actions.StateAction
import com.netflix.genie.test.categories.UnitTest
import org.apache.commons.lang3.tuple.Triple
import org.junit.experimental.categories.Category
import org.springframework.statemachine.StateMachine
import spock.lang.Specification

@Category(UnitTest.class)
class StateMachineAutoConfigurationSpec extends Specification {
    void setup() {
    }

    void cleanup() {
    }

    def "StateMachine"() {
        setup:
        def config = new StateMachineAutoConfiguration()

        when:
        StateMachine<States, Events> sm = config.stateMachine(
                config.statesWithActions(
                    Mock(StateAction.Initialize),
                    Mock(StateAction.ConfigureAgent),
                    Mock(StateAction.ResolveJobSpecification),
                    Mock(StateAction.SetUpJob),
                    Mock(StateAction.LaunchJob),
                    Mock(StateAction.MonitorJob),
                    Mock(StateAction.CleanupJob),
                    Mock(StateAction.Shutdown),
                    Mock(StateAction.HandleError)
                ),
                new LinkedList<Triple<States,Events,States>>(),
                new LinkedList<States>(),
                new LinkedList<JobExecutionListener>()
        )

        then:
        sm != null
    }
}
