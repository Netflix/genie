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
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import spock.lang.Specification

class ReserveJobIdStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    String jobId
    AgentJobRequest jobRequest
    AgentClientMetadata agentClientMetadata
    AgentJobService agentJobService

    void setup() {
        this.agentJobService = Mock(AgentJobService)
        this.jobRequest = Mock(AgentJobRequest)
        this.agentClientMetadata = Mock(AgentClientMetadata)
        this.jobId = UUID.randomUUID().toString()
        this.executionContext = Mock(ExecutionContext)
        this.stage = new ReserveJobIdStage(agentJobService)
    }

    def "AttemptTransition -- pre-reserved job"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.isPreResolved() >> true
        1 * executionContext.getRequestedJobId() >> jobId
        1 * executionContext.setCurrentJobStatus(JobStatus.ACCEPTED)
        1 * executionContext.setReservedJobId(jobId)
    }

    def "AttemptTransition -- new job"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.isPreResolved() >> false
        1 * executionContext.getRequestedJobId() >> jobId
        1 * executionContext.getAgentJobRequest() >> jobRequest
        1 * executionContext.getAgentClientMetadata() >> agentClientMetadata
        1 * agentJobService.reserveJobId(jobRequest, agentClientMetadata) >> jobId
        1 * executionContext.setCurrentJobStatus(JobStatus.RESERVED)
        1 * executionContext.setReservedJobId(jobId)
    }

    def "AttemptTransition -- new job, fatal error"() {
        setup:
        Throwable reservationException = Mock(JobReservationException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.isPreResolved() >> false
        1 * executionContext.getRequestedJobId() >> jobId
        1 * executionContext.getAgentJobRequest() >> jobRequest
        1 * executionContext.getAgentClientMetadata() >> agentClientMetadata
        1 * agentJobService.reserveJobId(jobRequest, agentClientMetadata) >> { throw reservationException }
        0 * executionContext.setCurrentJobStatus(JobStatus.RESERVED)
        0 * executionContext.setReservedJobId(jobId)
        def e = thrown(FatalJobExecutionException)
        e.getCause() == reservationException
    }

    def "AttemptTransition -- new job, retryable error"() {
        setup:
        Throwable reservationException = Mock(GenieRuntimeException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.isPreResolved() >> false
        1 * executionContext.getRequestedJobId() >> jobId
        1 * executionContext.getAgentJobRequest() >> jobRequest
        1 * executionContext.getAgentClientMetadata() >> agentClientMetadata
        1 * agentJobService.reserveJobId(jobRequest, agentClientMetadata) >> { throw reservationException }
        0 * executionContext.setCurrentJobStatus(JobStatus.RESERVED)
        0 * executionContext.setReservedJobId(jobId)
        def e = thrown(RetryableJobExecutionException)
        e.getCause() == reservationException
    }
}
