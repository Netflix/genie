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

import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import spock.lang.Specification

class ObtainJobSpecificationStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    AgentJobService agentJobService
    String jobId
    JobSpecification jobSpec

    void setup() {
        this.jobId = UUID.randomUUID().toString()
        this.jobSpec = Mock(JobSpecification)
        this.agentJobService = Mock(AgentJobService)
        this.executionContext = Mock(ExecutionContext)
        this.stage = new ObtainJobSpecificationStage(agentJobService)
    }

    def "AttemptTransition -- api job"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getReservedJobId() >> jobId
        1 * executionContext.isPreResolved() >> true
        1 * agentJobService.getJobSpecification(jobId) >> jobSpec
        1 * executionContext.setJobSpecification(jobSpec)
    }

    def "AttemptTransition -- api job, error"() {
        setup:
        JobSpecificationResolutionException resolutionException = Mock(JobSpecificationResolutionException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getReservedJobId() >> jobId
        1 * executionContext.isPreResolved() >> true
        1 * agentJobService.getJobSpecification(jobId) >> { throw resolutionException }
        0 * executionContext.setJobSpecification(jobSpec)
        def e = thrown(FatalJobExecutionException)
        e.getCause() == resolutionException
    }

    def "AttemptTransition -- CLI job"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getReservedJobId() >> jobId
        1 * executionContext.isPreResolved() >> false
        1 * agentJobService.resolveJobSpecification(jobId) >> jobSpec
        1 * executionContext.setCurrentJobStatus(JobStatus.RESOLVED)
        1 * executionContext.setJobSpecification(jobSpec)
    }

    def "AttemptTransition -- CLI job, fatal error"() {
        setup:
        Throwable resolutionException = Mock(JobSpecificationResolutionException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getReservedJobId() >> jobId
        1 * executionContext.isPreResolved() >> false
        1 * agentJobService.resolveJobSpecification(jobId) >> { throw resolutionException }
        0 * executionContext.setCurrentJobStatus(JobStatus.RESOLVED)
        0 * executionContext.setJobSpecification(jobSpec)
        def e = thrown(FatalJobExecutionException)
        e.getCause() == resolutionException
    }

    def "AttemptTransition -- CLI job, retryable error"() {
        setup:
        Throwable resolutionException = Mock(GenieRuntimeException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getReservedJobId() >> jobId
        1 * executionContext.isPreResolved() >> false
        1 * agentJobService.resolveJobSpecification(jobId) >> { throw resolutionException }
        0 * executionContext.setCurrentJobStatus(JobStatus.RESOLVED)
        0 * executionContext.setJobSpecification(jobSpec)
        def e = thrown(RetryableJobExecutionException)
        e.getCause() == resolutionException
    }
}
