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
import org.springframework.statemachine.config.StateMachineBuilder
import spock.lang.Specification

@Category(UnitTest.class)
class JobExecutionStateMachineImplSpec extends Specification {
    def "RunAndWaitForStop"() {
        setup:
        def builder = new StateMachineBuilder.Builder<States, Events>()

        builder.configureStates()
                .withStates()
                .initial(States.READY)
                .end(States.END)
                .states(EnumSet.allOf(States.class))

        builder.configureTransitions()
            .withExternal()
            .source(States.READY)
            .target(States.INITIALIZE)
            .event(Events.START)
            .and()
            .withExternal()
            .source(States.INITIALIZE)
            .target(States.END)

        def stateMachine = new JobExecutionStateMachineImpl(builder.build())

        when:
        stateMachine.start()
        def finalState = stateMachine.waitForStop()

        then:
        finalState == States.END

    }
}
