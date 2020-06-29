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

import com.netflix.genie.agent.execution.process.JobProcessManager
import com.netflix.genie.agent.execution.process.JobProcessResult
import com.netflix.genie.agent.execution.services.JobMonitorService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException
import com.netflix.genie.common.external.dtos.v4.JobStatus
import spock.lang.Specification

import java.nio.file.Path

class WaitJobCompletionStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    JobProcessManager jobProcessManager
    JobProcessResult jobProcessResult
    JobMonitorService jobMonitorService
    File jobDirectory
    Path jobDirectoryPath
    String jobId

    void setup() {
        this.jobProcessResult = Mock(JobProcessResult)
        this.jobProcessManager = Mock(JobProcessManager)
        this.executionContext = Mock(ExecutionContext)
        this.jobMonitorService = Mock(JobMonitorService)
        this.jobDirectory = Mock(File)
        this.jobDirectoryPath = Mock(Path)
        this.jobId = UUID.randomUUID().toString()
        this.stage = new WaitJobCompletionStage(jobProcessManager, jobMonitorService)
    }

    def "AttemptTransition -- not launched"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.isJobLaunched() >> false
        0 * executionContext.getClaimedJobId() >> jobId
        0 * jobProcessManager.waitFor()
        0 * executionContext.getJobDirectory()
        0 * jobMonitorService.start(_, _)
        0 * jobMonitorService.stop()
    }

    def "AttemptTransition -- success"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.isJobLaunched() >> true
        1 * executionContext.getJobDirectory() >> jobDirectory
        1 * executionContext.getClaimedJobId() >> jobId
        1 * jobDirectory.toPath() >> jobDirectoryPath
        1 * jobMonitorService.start(jobId, jobDirectory)
        1 * jobProcessManager.waitFor() >> jobProcessResult
        1 * jobMonitorService.stop()
        1 * executionContext.setJobProcessResult(jobProcessResult)
        1 * jobProcessResult.getFinalStatus() >> JobStatus.KILLED
    }

    def "AttemptTransition -- error"() {
        setup:
        InterruptedException interruptedException = Mock(InterruptedException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.isJobLaunched() >> true
        1 * executionContext.getJobDirectory() >> jobDirectory
        1 * executionContext.getClaimedJobId() >> jobId
        1 * jobDirectory.toPath() >> jobDirectoryPath
        1 * jobMonitorService.start(jobId, jobDirectory)
        1 * jobProcessManager.waitFor() >> { throw interruptedException }
        1 * jobMonitorService.stop()
        def e = thrown(FatalJobExecutionException)
        e.getCause() == interruptedException
        0 * executionContext.setJobProcessResult(jobProcessResult)
        0 * jobProcessResult.getFinalStatus() >> JobStatus.KILLED
    }
}
