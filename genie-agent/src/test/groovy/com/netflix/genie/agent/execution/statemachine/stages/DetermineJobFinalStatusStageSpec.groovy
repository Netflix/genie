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


import com.netflix.genie.agent.execution.process.JobProcessResult
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException
import com.netflix.genie.agent.execution.statemachine.States
import com.netflix.genie.common.dto.JobStatusMessages
import com.netflix.genie.common.external.dtos.v4.JobStatus
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

@Unroll
class DetermineJobFinalStatusStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext

    @Shared
    FatalJobExecutionException claimException = new FatalJobExecutionException(States.CLAIM_JOB, "...", new IOException())
    @Shared
    String killedResultMessage = "Killed"
    @Shared
    JobProcessResult killedResult = Mock(JobProcessResult) {
        getFinalStatus() >> JobStatus.KILLED
        getFinalStatusMessage() >> killedResultMessage
    }

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.stage = new DetermineJobFinalStatusStage()
    }

    def "AttemptTransition"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getCurrentJobStatus() >> currentJobStatus
        1 * executionContext.isJobLaunched() >> launched
        1 * executionContext.isJobKilled() >> killed
        1 * executionContext.getExecutionAbortedFatalException() >> fatalException
        1 * executionContext.getJobProcessResult() >> jobProcessResult
        1 * executionContext.setNextJobStatus(nextJobStatus)
        1 * executionContext.setNextJobStatusMessage(nextJobStatusMessage)

        where:
        currentJobStatus  | launched | killed | fatalException | jobProcessResult | nextJobStatus     | nextJobStatusMessage
        JobStatus.RUNNING | true     | true   | null           | killedResult     | JobStatus.KILLED  | killedResultMessage
        JobStatus.CLAIMED | false    | false  | claimException | null             | JobStatus.FAILED  | JobStatusMessages.FAILED_TO_CLAIM_JOB
        JobStatus.CLAIMED | false    | true   | null           | null             | JobStatus.KILLED  | JobStatusMessages.JOB_KILLED_BY_USER
        JobStatus.CLAIMED | true     | false  | null           | null             | JobStatus.INVALID | JobStatusMessages.UNKNOWN_JOB_STATE

    }
}
