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
package com.netflix.genie.agent.execution.statemachine.stages

import com.netflix.genie.agent.execution.services.AgentFileStreamService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.States
import spock.lang.Specification
import spock.lang.Unroll

class RefreshManifestStageSpec extends Specification {
    AgentFileStreamService agentFileService

    ExecutionContext executionContext

    void setup() {
        this.agentFileService = Mock(AgentFileStreamService)
        this.executionContext = Mock(ExecutionContext)
    }

    @Unroll
    def "AttemptTransition for state #state"() {
        ExecutionStage stage = new RefreshManifestStage(agentFileService, state)

        when:
        stage.attemptTransition(executionContext)

        then:
        1 * executionContext.getJobDirectory() >> Mock(File)
        1 * agentFileService.forceServerSync()

        when:
        stage.attemptTransition(executionContext)

        then:
        1 * executionContext.getJobDirectory() >> null
        0 * agentFileService.forceServerSync()

        where:
        state | _
        States.POST_SETUP_MANIFEST_REFRESH | _
        States.POST_LAUNCH_MANIFEST_REFRESH | _
        States.POST_EXECUTION_MANIFEST_REFRESH | _
    }
}
