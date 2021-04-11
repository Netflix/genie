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

import brave.SpanCustomizer
import brave.Tracer
import com.netflix.genie.agent.execution.exceptions.GetJobStatusException
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
import com.netflix.genie.common.internal.tracing.TracingConstants
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator
import com.netflix.genie.common.internal.tracing.brave.BraveTracingCleanup
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents
import spock.lang.Specification

@SuppressWarnings("GroovyAccessibility")
class ReserveJobIdStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    String jobId
    AgentJobRequest jobRequest
    AgentClientMetadata agentClientMetadata
    AgentJobService agentJobService
    Tracer tracer
    BraveTagAdapter tagAdapter
    SpanCustomizer spanCustomizer

    void setup() {
        this.agentJobService = Mock(AgentJobService)
        this.jobRequest = Mock(AgentJobRequest)
        this.agentClientMetadata = Mock(AgentClientMetadata)
        this.jobId = UUID.randomUUID().toString()
        this.executionContext = Mock(ExecutionContext)
        this.tracer = Mock(Tracer)
        this.tagAdapter = Mock(BraveTagAdapter)
        this.spanCustomizer = Mock(SpanCustomizer)
        this.stage = new ReserveJobIdStage(
            this.agentJobService,
            new BraveTracingComponents(
                this.tracer,
                Mock(BraveTracePropagator),
                Mock(BraveTracingCleanup),
                this.tagAdapter
            )
        )
    }

    def "AttemptTransition -- pre-reserved job"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.isPreResolved() >> true
        1 * executionContext.getRequestedJobId() >> jobId
        1 * agentJobService.getJobStatus(jobId) >> JobStatus.ACCEPTED
        1 * executionContext.setCurrentJobStatus(JobStatus.ACCEPTED)
        1 * executionContext.setReservedJobId(jobId)
        1 * this.tracer.currentSpanCustomizer() >> this.spanCustomizer
        1 * this.tagAdapter.tag(this.spanCustomizer, TracingConstants.JOB_ID_TAG, this.jobId)
    }

    def "AttemptTransition -- pre-reserved job, retry-able error"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.isPreResolved() >> true
        1 * executionContext.getRequestedJobId() >> jobId
        1 * agentJobService.getJobStatus(jobId) >> { throw new GetJobStatusException("...") }
        0 * this.tracer.currentSpanCustomizer()
        0 * this.tagAdapter.tag(this.spanCustomizer, _ as String, _ as String)
        thrown(RetryableJobExecutionException)
    }

    def "AttemptTransition -- pre-reserved job, fatal error"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.isPreResolved() >> true
        1 * executionContext.getRequestedJobId() >> jobId
        1 * agentJobService.getJobStatus(jobId) >> JobStatus.RUNNING
        0 * this.tracer.currentSpanCustomizer()
        0 * this.tagAdapter.tag(_ as SpanCustomizer, _ as String, _ as String)
        thrown(FatalJobExecutionException)
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
        1 * this.tracer.currentSpanCustomizer() >> this.spanCustomizer
        1 * this.tagAdapter.tag(this.spanCustomizer, TracingConstants.JOB_ID_TAG, this.jobId)
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
        0 * this.tracer.currentSpanCustomizer()
        0 * this.tagAdapter.tag(this.spanCustomizer, _ as String, _ as String)
        def e = thrown(FatalJobExecutionException)
        e.getCause() == reservationException
    }

    def "AttemptTransition -- new job, retry-able error"() {
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
        0 * this.tracer.currentSpanCustomizer()
        0 * this.tagAdapter.tag(this.spanCustomizer, _ as String, _ as String)
        def e = thrown(RetryableJobExecutionException)
        e.getCause() == reservationException
    }
}
