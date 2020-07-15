/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.agent.execution.statemachine


import com.netflix.genie.agent.execution.CleanupStrategy
import com.netflix.genie.agent.execution.process.JobProcessResult
import com.netflix.genie.agent.properties.AgentProperties
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.external.dtos.v4.JobStatus
import spock.lang.Specification

class ExecutionContextSpec extends Specification {

    def "Get and set all"() {
        setup:
        AgentClientMetadata agentClientMetadata = Mock(AgentClientMetadata)
        String jobId = UUID.randomUUID().toString()
        JobStatus currentJobStatus = JobStatus.ACCEPTED
        AgentJobRequest agentJobRequest = Mock(AgentJobRequest)
        JobSpecification jobSpecification = Mock(JobSpecification)
        File jobDirectory = Mock(File)
        File jobScript = Mock(File)
        JobProcessResult jobProcessResult = Mock(JobProcessResult)
        Exception retryableException = new RetryableJobExecutionException("...", null)
        Exception fatalException = new FatalJobExecutionException(States.CREATE_JOB_DIRECTORY, "...", new IOException())
        AgentProperties agentProperties = Mock(AgentProperties)
        JobExecutionStateMachine stateMachine = Mock(JobExecutionStateMachine)
        ExecutionContext executionContext = new ExecutionContext(agentProperties)

        expect:
        executionContext.getAgentClientMetadata() == null
        executionContext.getRequestedJobId() == null
        executionContext.getReservedJobId() == null
        executionContext.getClaimedJobId() == null
        executionContext.getCurrentJobStatus() == JobStatus.INVALID
        executionContext.getAgentJobRequest() == null
        executionContext.getJobSpecification() == null
        executionContext.getJobDirectory() == null
        executionContext.getJobScript() == null
        executionContext.getJobProcessResult() == null
        executionContext.getTransitionExceptionRecords() != null
        executionContext.getTransitionExceptionRecords().isEmpty()
        !executionContext.isPreResolved()
        !executionContext.isRunFromJobDirectory()
        !executionContext.isJobLaunched()
        !executionContext.isJobKilled()
        executionContext.getAttemptsLeft() == 0
        !executionContext.isExecutionAborted()
        executionContext.getExecutionAbortedFatalException() == null
        executionContext.getCleanupStrategy() == CleanupStrategy.DEFAULT_STRATEGY
        executionContext.getNextJobStatus() == JobStatus.INVALID
        executionContext.getNextJobStatusMessage() == null
        executionContext.getAgentProperties() != null
        executionContext.getStateMachine() == null
        !executionContext.isSkipFinalStatusUpdate()

        when:
        executionContext.setAgentClientMetadata(agentClientMetadata)
        executionContext.setRequestedJobId(jobId)
        executionContext.setReservedJobId(jobId)
        executionContext.setClaimedJobId(jobId)
        executionContext.setCurrentJobStatus(currentJobStatus)
        executionContext.setAgentJobRequest(agentJobRequest)
        executionContext.setJobSpecification(jobSpecification)
        executionContext.setJobDirectory(jobDirectory)
        executionContext.setJobScript(jobScript)
        executionContext.setJobProcessResult(jobProcessResult)
        executionContext.recordTransitionException(States.HANDSHAKE, retryableException)
        executionContext.recordTransitionException(States.CREATE_JOB_DIRECTORY, fatalException)
        !executionContext.setPreResolved(true)
        !executionContext.setRunFromJobDirectory(true)
        !executionContext.setJobLaunched(true)
        !executionContext.setJobKilled(true)
        executionContext.setAttemptsLeft(3)
        executionContext.setExecutionAbortedFatalException(fatalException)
        executionContext.setCleanupStrategy(CleanupStrategy.FULL_CLEANUP)
        executionContext.setNextJobStatus(JobStatus.RUNNING)
        executionContext.setNextJobStatusMessage("Job running")
        executionContext.setStateMachine(stateMachine)
        executionContext.setSkipFinalStatusUpdate(true)

        then:
        executionContext.getAgentClientMetadata() == agentClientMetadata
        executionContext.getRequestedJobId() == jobId
        executionContext.getReservedJobId() == jobId
        executionContext.getClaimedJobId() == jobId
        executionContext.getCurrentJobStatus() == currentJobStatus
        executionContext.getAgentJobRequest() == agentJobRequest
        executionContext.getJobSpecification() == jobSpecification
        executionContext.getJobDirectory() == jobDirectory
        executionContext.getJobScript() == jobScript
        executionContext.getJobProcessResult() == jobProcessResult
        executionContext.getTransitionExceptionRecords().size() == 2
        executionContext.getTransitionExceptionRecords().get(0).getRecordedException() == retryableException
        executionContext.getTransitionExceptionRecords().get(0).getState() == States.HANDSHAKE
        executionContext.getTransitionExceptionRecords().get(1).getRecordedException() == fatalException
        executionContext.getTransitionExceptionRecords().get(1).getState() == States.CREATE_JOB_DIRECTORY
        executionContext.isPreResolved()
        executionContext.isRunFromJobDirectory()
        executionContext.isJobLaunched()
        executionContext.isJobKilled()
        executionContext.getAttemptsLeft() == 3
        executionContext.isExecutionAborted()
        executionContext.getExecutionAbortedFatalException() == fatalException
        executionContext.getCleanupStrategy() == CleanupStrategy.FULL_CLEANUP
        executionContext.getNextJobStatus() == JobStatus.RUNNING
        executionContext.getNextJobStatusMessage() == "Job running"
        executionContext.getStateMachine() == stateMachine
        executionContext.isSkipFinalStatusUpdate()
    }
}
