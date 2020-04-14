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

import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import spock.lang.Specification

abstract class UpdateJobStatusStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    String jobId
    JobStatus currentStatus
    JobStatus nextStatus
    String nextStatusMessage
    AgentJobService agentJobService

    protected void _setup(
        Class<ExecutionStage> executionStageClass,
        AgentJobService agentJobService,
        JobStatus currentStatus,
        JobStatus nextStatus
    ) {
        this.jobId = UUID.randomUUID().toString()
        this.currentStatus = currentStatus
        this.nextStatus = nextStatus
        this.nextStatusMessage = "..."
        this.executionContext = Mock(ExecutionContext)
        this.stage = executionStageClass.getConstructor(AgentJobService).newInstance(agentJobService)
        this.agentJobService = agentJobService
    }

    def "AttemptTransition -- success"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getReservedJobId() >> jobId
        1 * executionContext.getCurrentJobStatus() >> currentStatus
        1 * executionContext.getNextJobStatus() >> nextStatus
        1 * executionContext.getNextJobStatusMessage() >> nextStatusMessage
        1 * agentJobService.changeJobStatus(jobId, currentStatus, nextStatus, nextStatusMessage)
        1 * executionContext.setCurrentJobStatus(nextStatus)
    }

    def "AttemptTransition -- skip due to invalid status"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getReservedJobId() >> jobId
        1 * executionContext.getCurrentJobStatus() >> currentStatus
        1 * executionContext.getNextJobStatus() >> JobStatus.INVALID
        1 * executionContext.getNextJobStatusMessage() >> null
        0 * agentJobService.changeJobStatus(jobId, currentStatus, nextStatus, nextStatusMessage)
        0 * executionContext.setCurrentJobStatus(nextStatus)
    }

    def "AttemptTransition -- fatal error"() {
        setup:
        Throwable changeStatusException = Mock(ChangeJobStatusException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getReservedJobId() >> jobId
        1 * executionContext.getCurrentJobStatus() >> currentStatus
        1 * executionContext.getNextJobStatus() >> nextStatus
        1 * executionContext.getNextJobStatusMessage() >> nextStatusMessage
        1 * agentJobService.changeJobStatus(jobId, currentStatus, nextStatus, nextStatusMessage) >> { throw changeStatusException }
        0 * executionContext.setCurrentJobStatus(nextStatus)
        def e = thrown(FatalJobExecutionException)
        e.getCause() == changeStatusException
    }

    def "AttemptTransition -- retryable error"() {
        setup:
        Throwable changeStatusException = Mock(GenieRuntimeException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getReservedJobId() >> jobId
        1 * executionContext.getCurrentJobStatus() >> currentStatus
        1 * executionContext.getNextJobStatus() >> nextStatus
        1 * executionContext.getNextJobStatusMessage() >> nextStatusMessage
        1 * agentJobService.changeJobStatus(jobId, currentStatus, nextStatus, nextStatusMessage) >> { throw changeStatusException }
        0 * executionContext.setCurrentJobStatus(nextStatus)
        def e = thrown(RetryableJobExecutionException)
        e.getCause() == changeStatusException
    }
}
