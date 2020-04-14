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

import com.netflix.genie.agent.execution.CleanupStrategy
import com.netflix.genie.agent.execution.services.JobSetupService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException
import spock.lang.Specification

import java.nio.file.Path

class CleanupJobDirectoryStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    JobSetupService jobSetupService
    File jobDirectory
    CleanupStrategy cleanupStrategy
    Path jobDirectoryPath

    void setup() {
        this.jobSetupService = Mock(JobSetupService)
        this.jobDirectory = Mock(File)
        this.cleanupStrategy = CleanupStrategy.FULL_CLEANUP
        this.executionContext = Mock(ExecutionContext)
        this.stage = new CleanupJobDirectoryStage(jobSetupService)
    }

    def "AttemptTransition -- success"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobDirectory() >> jobDirectory
        1 * executionContext.getCleanupStrategy() >> cleanupStrategy
        1 * jobDirectory.toPath() >> jobDirectoryPath
        1 * jobSetupService.cleanupJobDirectory(jobDirectoryPath, cleanupStrategy)
    }

    def "AttemptTransition -- no job directory"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobDirectory() >> null
        1 * executionContext.getCleanupStrategy() >> cleanupStrategy
        0 * jobDirectory.toPath() >> jobDirectoryPath
        0 * jobSetupService.cleanupJobDirectory(_, _)
    }

    def "AttemptTransition -- error"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobDirectory() >> jobDirectory
        1 * executionContext.getCleanupStrategy() >> cleanupStrategy
        1 * jobDirectory.toPath() >> jobDirectoryPath
        1 * jobSetupService.cleanupJobDirectory(jobDirectoryPath, cleanupStrategy) >> { throw new IOException() }
        def e = thrown(RetryableJobExecutionException)
        e.getCause().getClass() == IOException
    }
}
