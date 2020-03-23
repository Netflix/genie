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
import spock.lang.Specification

import java.nio.file.Path

class StartFileServiceStageSpec extends Specification {
    StartServiceStage stage
    ExecutionContext executionContext
    AgentFileStreamService service
    String jobId

    void setup() {
        this.jobId = UUID.randomUUID().toString()
        this.executionContext = Mock(ExecutionContext)
        this.service = Mock(AgentFileStreamService)
        this.stage = new StartFileServiceStage(service)
    }

    def "AttemptTransition"() {
        setup:
        File jobDir = Mock(File)
        Path jobDirPath = Mock(Path)

        when:
        stage.startService(jobId, executionContext)

        then:
        1 * executionContext.getJobDirectory() >> jobDir
        1 * jobDir.toPath() >> jobDirPath
        1 * service.start(jobId, jobDir)
    }
}
