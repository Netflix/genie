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
import com.netflix.genie.common.internal.dtos.JobSpecification
import org.assertj.core.util.Sets
import spock.lang.Specification

class DownloadDependenciesStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    JobSetupService jobSetupService
    JobSpecification jobSpec
    File jobDir


    void setup() {
        this.jobSetupService = Mock(JobSetupService)
        this.jobSpec = Mock(JobSpecification)
        this.jobDir = Mock(File)
        this.executionContext = Mock(ExecutionContext)
        this.stage = new DownloadDependenciesStage(jobSetupService)
    }

    def "AttemptTransition -- success"() {
        setup:
        Set<File> files = Sets.newHashSet()

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobDir
        1 * jobSetupService.downloadJobResources(jobSpec, jobDir) >> files
    }

    def "AttemptTransition -- error"() {
        setup:

        SetUpJobException setupException = Mock(SetUpJobException)
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobDir
        1 * jobSetupService.downloadJobResources(jobSpec, jobDir) >> { throw setupException }
        def e = thrown(FatalJobExecutionException)
        e.getCause() == setupException
    }

}
