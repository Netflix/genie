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
import com.netflix.genie.agent.execution.exceptions.AgentRegistrationException
import com.netflix.genie.agent.execution.services.AgentEventsService
import com.netflix.genie.agent.execution.services.AgentRegistrationService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class InitializeActionSpec extends Specification {
    AgentRegistrationService agentRegistrationService
    ExecutionContext executionContext
    String agentId
    AgentEventsService agentEventsService = null
    InitializeAction action

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.agentRegistrationService = Mock(AgentRegistrationService)
        this.executionContext = Mock(ExecutionContext)
        this.agentId = UUID.randomUUID().toString()
        this.action = new InitializeAction(agentRegistrationService, executionContext, agentEventsService)
    }

    void cleanup() {
    }

    def "Register"() {
        when:
        def event = action.executeStateAction(executionContext)
        then:
        1 * agentRegistrationService.registerAgent() >> agentId
        1 * executionContext.setAgentId(agentId)
        event == Events.INITIALIZE_COMPLETE
    }

    def "RegistrationException"() {
        setup:
        def e = new AgentRegistrationException("test")
        when:
        action.executeStateAction(executionContext)
        then:
        1 * agentRegistrationService.registerAgent() >> { throw e }
        0 * executionContext.setAgentId(agentId)
        thrown(RuntimeException.class)
    }

    def "RegistrationRuntimeException"() {
        setup:
        def e = new RuntimeException("test")
        when:
        action.executeStateAction(executionContext)
        then:
        1 * agentRegistrationService.registerAgent() >> { throw e }
        0 * executionContext.setAgentId(agentId)
        thrown(RuntimeException.class)
    }

}
