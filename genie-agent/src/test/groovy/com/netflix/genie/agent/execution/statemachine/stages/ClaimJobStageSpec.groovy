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

import com.netflix.genie.agent.execution.exceptions.JobReservationException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException
import com.netflix.genie.common.dto.JobStatusMessages
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import spock.lang.Specification

class ClaimJobStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    AgentJobService agentJobService
    String jobId
    AgentClientMetadata agentClientMetadata

    void setup() {
        this.agentJobService = Mock(AgentJobService)
        this.executionContext = Mock(ExecutionContext)
        this.jobId = UUID.randomUUID().toString()
        this.agentClientMetadata = Mock(AgentClientMetadata)
        this.stage = new ClaimJobStage(agentJobService)
    }

    def "AttemptTransition - success"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getReservedJobId() >> jobId
        1 * executionContext.getAgentClientMetadata() >> agentClientMetadata
        1 * agentJobService.claimJob(jobId, agentClientMetadata)
        1 * executionContext.setCurrentJobStatus(JobStatus.CLAIMED)
        1 * executionContext.setClaimedJobId(jobId)
        1 * executionContext.setNextJobStatus(JobStatus.INIT)
        1 * executionContext.setNextJobStatusMessage(JobStatusMessages.JOB_INITIALIZING)
    }

    def "AttemptTransition - fatal error"() {
        Exception reservationException = new JobReservationException("...")

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getReservedJobId() >> jobId
        1 * executionContext.getAgentClientMetadata() >> agentClientMetadata
        1 * agentJobService.claimJob(jobId, agentClientMetadata) >> { throw reservationException }
        0 * executionContext.setCurrentJobStatus(JobStatus.CLAIMED)
        0 * executionContext.setClaimedJobId(jobId)
        def e = thrown(FatalJobExecutionException)
        e.getCause() == reservationException
    }

    def "AttemptTransition - retryable error"() {
        Exception genieRuntimeException = new GenieRuntimeException("...")

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getReservedJobId() >> jobId
        1 * executionContext.getAgentClientMetadata() >> agentClientMetadata
        1 * agentJobService.claimJob(jobId, agentClientMetadata) >> { throw genieRuntimeException }
        0 * executionContext.setCurrentJobStatus(JobStatus.CLAIMED)
        0 * executionContext.setClaimedJobId(jobId)
        def e = thrown(RetryableJobExecutionException)
        e.getCause() == genieRuntimeException
    }
}
