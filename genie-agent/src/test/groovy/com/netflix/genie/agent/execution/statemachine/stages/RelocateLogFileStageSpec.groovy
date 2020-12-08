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

import com.netflix.genie.agent.cli.logging.AgentLogManager
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class RelocateLogFileStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    @TempDir
    Path jobDir

    AgentLogManager agentLogManager

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.agentLogManager = Mock(AgentLogManager)
        this.stage = new RelocateLogFileStage(this.agentLogManager)
    }

    def "AttemptTransition"() {
        Path expectedDestination = this.jobDir.resolve("genie").resolve("logs").resolve("agent.log")

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobDirectory() >> this.jobDir.toFile()
        1 * agentLogManager.relocateLogFile(expectedDestination)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobDirectory() >> this.jobDir.toFile()
        1 * agentLogManager.relocateLogFile(expectedDestination) >> { throw new IOException("...") }
        noExceptionThrown()
    }
}
