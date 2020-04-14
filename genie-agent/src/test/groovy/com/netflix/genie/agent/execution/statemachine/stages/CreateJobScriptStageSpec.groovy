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

import com.netflix.genie.agent.execution.exceptions.SetUpJobException
import com.netflix.genie.agent.execution.services.JobSetupService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import spock.lang.Specification

class CreateJobScriptStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    JobSetupService jobSetupService
    JobSpecification jobSpec
    File jobScript
    File jobDir

    void setup() {
        this.jobSetupService = Mock(JobSetupService)
        this.jobSpec = Mock(JobSpecification)
        this.jobScript = Mock(File)
        this.jobDir = Mock(File)
        this.executionContext = Mock(ExecutionContext)
        this.stage = new CreateJobScriptStage(jobSetupService)
    }

    def "AttemptTransition -- success"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobDir
        1 * jobSetupService.createJobScript(jobSpec, jobDir) >> jobScript
        1 * executionContext.setJobScript(jobScript)
    }

    def "AttemptTransition -- error"() {
        setup:
        SetUpJobException setupException = Mock(SetUpJobException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobDir
        1 * jobSetupService.createJobScript(jobSpec, jobDir) >> { throw setupException }
        0 * executionContext.setJobScript(jobScript)
        def e = thrown(FatalJobExecutionException)
        e.getCause() == setupException
    }
}
