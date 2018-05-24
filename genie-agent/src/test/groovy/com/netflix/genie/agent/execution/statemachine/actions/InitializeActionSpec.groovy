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
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class InitializeActionSpec extends Specification {
    ExecutionContext executionContext
    String agentId

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.agentId = UUID.randomUUID().toString()
    }

    void cleanup() {
    }

    def "Initialize"() {
        setup:
        InitializeAction action = new InitializeAction(executionContext)
        when:
        def event = action.executeStateAction(executionContext)
        then:
        event == Events.INITIALIZE_COMPLETE
    }
}
