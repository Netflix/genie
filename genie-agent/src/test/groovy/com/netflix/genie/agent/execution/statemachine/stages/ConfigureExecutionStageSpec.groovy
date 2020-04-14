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

import com.netflix.genie.agent.cli.ArgumentDelegates
import com.netflix.genie.agent.cli.JobRequestConverter
import com.netflix.genie.agent.execution.CleanupStrategy
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest
import spock.lang.Specification

class ConfigureExecutionStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    JobRequestConverter jobRequestConverter
    ArgumentDelegates.JobRequestArguments jobRequestArgs
    ArgumentDelegates.RuntimeConfigurationArguments runtimeConfigArgs
    ArgumentDelegates.CleanupArguments cleanupArgs
    boolean apiJob
    boolean cdToJobDir
    CleanupStrategy cleanupStrategy
    String jobId
    AgentJobRequest agentJobRequest

    void setup() {
        this.jobRequestConverter = Mock(JobRequestConverter)
        this.jobRequestArgs = Mock(ArgumentDelegates.JobRequestArguments)
        this.runtimeConfigArgs = Mock(ArgumentDelegates.RuntimeConfigurationArguments)
        this.cleanupArgs = Mock(ArgumentDelegates.CleanupArguments)
        this.executionContext = Mock(ExecutionContext)
        this.apiJob = false
        this.cdToJobDir = false
        this.cleanupStrategy = CleanupStrategy.FULL_CLEANUP
        this.jobId = UUID.randomUUID().toString()
        this.agentJobRequest = Mock(AgentJobRequest)
        this.stage = new ConfigureExecutionStage(
            jobRequestConverter,
            jobRequestArgs,
            runtimeConfigArgs,
            cleanupArgs
        )
    }

    def "AttemptTransition -- api job, success"() {
        setup:
        this.apiJob = false
        this.cdToJobDir = false
        this.cleanupStrategy = CleanupStrategy.FULL_CLEANUP

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * jobRequestArgs.isJobRequestedViaAPI() >> apiJob
        1 * executionContext.setPreResolved(apiJob)
        1 * runtimeConfigArgs.isLaunchInJobDirectory() >> cdToJobDir
        1 * executionContext.setRunFromJobDirectory(cdToJobDir)
        1 * cleanupArgs.getCleanupStrategy() >> cleanupStrategy
        1 * executionContext.setCleanupStrategy(cleanupStrategy)
        1 * jobRequestArgs.getJobId() >> jobId
        1 * executionContext.setRequestedJobId(jobId)
    }

    def "AttemptTransition -- api job, missing argument"() {

        setup:
        apiJob = true

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * jobRequestArgs.isJobRequestedViaAPI() >> apiJob
        1 * executionContext.setPreResolved(apiJob)
        1 * runtimeConfigArgs.isLaunchInJobDirectory() >> cdToJobDir
        1 * executionContext.setRunFromJobDirectory(cdToJobDir)
        1 * cleanupArgs.getCleanupStrategy() >> cleanupStrategy
        1 * executionContext.setCleanupStrategy(cleanupStrategy)
        1 * jobRequestArgs.getJobId() >> null
        def e = thrown(FatalJobExecutionException)
        e.getCause().getClass() == IllegalArgumentException
    }

    def "AttemptTransition -- cli job, success"() {

        setup:
        apiJob = false
        jobId = null

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * jobRequestArgs.isJobRequestedViaAPI() >> apiJob
        1 * executionContext.setPreResolved(apiJob)
        1 * runtimeConfigArgs.isLaunchInJobDirectory() >> cdToJobDir
        1 * executionContext.setRunFromJobDirectory(cdToJobDir)
        1 * cleanupArgs.getCleanupStrategy() >> cleanupStrategy
        1 * executionContext.setCleanupStrategy(cleanupStrategy)
        1 * jobRequestArgs.getJobId() >> jobId
        1 * jobRequestConverter.agentJobRequestArgsToDTO(jobRequestArgs) >> agentJobRequest
        1 * executionContext.setAgentJobRequest(agentJobRequest)
        1 * executionContext.setRequestedJobId(jobId)
    }

    def "AttemptTransition -- cli job, conversion error"() {

        setup:
        apiJob = false
        jobId = null
        def conversionException = Mock(JobRequestConverter.ConversionException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * jobRequestArgs.isJobRequestedViaAPI() >> apiJob
        1 * executionContext.setPreResolved(apiJob)
        1 * runtimeConfigArgs.isLaunchInJobDirectory() >> cdToJobDir
        1 * executionContext.setRunFromJobDirectory(cdToJobDir)
        1 * cleanupArgs.getCleanupStrategy() >> cleanupStrategy
        1 * executionContext.setCleanupStrategy(cleanupStrategy)
        1 * jobRequestArgs.getJobId() >> jobId
        1 * jobRequestConverter.agentJobRequestArgsToDTO(jobRequestArgs) >> { throw conversionException }
        0 * executionContext.setAgentJobRequest(agentJobRequest)
        0 * executionContext.setRequestedJobId(jobId)
        def e = thrown(FatalJobExecutionException)
        e.getCause() == conversionException
    }
}
