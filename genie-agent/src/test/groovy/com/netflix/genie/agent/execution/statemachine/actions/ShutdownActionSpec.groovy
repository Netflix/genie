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

import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.Events
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

class ShutdownActionSpec extends Specification {
    @Rule
    TemporaryFolder temporaryFolder
    ExecutionContext executionContext
    ShutdownAction action

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.action = new ShutdownAction(executionContext)
    }

    void cleanup() {
    }

    def "Execute action successfully"() {
        def event

        when:
        event = action.executeStateAction(executionContext)

        then:
        event == Events.SHUTDOWN_COMPLETE
    }

    def "Pre and post action validation"() {
        when:
        action.executePreActionValidation()

        then:
        noExceptionThrown()

        when:
        action.executePostActionValidation()

        then:
        noExceptionThrown()
    }
}
