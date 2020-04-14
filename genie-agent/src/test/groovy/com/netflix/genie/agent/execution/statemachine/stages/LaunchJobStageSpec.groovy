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

import com.netflix.genie.agent.execution.exceptions.JobLaunchException
import com.netflix.genie.agent.execution.process.JobProcessManager
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException
import com.netflix.genie.common.dto.JobStatusMessages
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.external.dtos.v4.JobStatus
import spock.lang.Specification
import spock.lang.Unroll

class LaunchJobStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    JobProcessManager jobProcessManager
    File jobDir
    File jobScript
    JobSpecification jobSpec

    void setup() {
        this.jobDir = Mock(File)
        this.jobScript = Mock(File)
        this.jobSpec = Mock(JobSpecification)
        this.jobProcessManager = Mock(JobProcessManager)
        this.executionContext = Mock(ExecutionContext)
        this.stage = new LaunchJobStage(jobProcessManager)
    }

    @Unroll
    def "AttemptTransition -- success"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobDirectory() >> jobDir
        1 * executionContext.getJobScript() >> jobScript
        1 * executionContext.isRunFromJobDirectory() >> doCd
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * jobSpec.isInteractive() >> interactive
        1 * jobSpec.getTimeout() >> Optional.ofNullable(timeout)
        1 * jobProcessManager.launchProcess(jobDir, jobScript, interactive, timeout, doCd)
        1 * executionContext.setNextJobStatus(JobStatus.RUNNING)
        1 * executionContext.setNextJobStatusMessage(JobStatusMessages.JOB_RUNNING)
        1 * executionContext.setJobLaunched(true)

        where:
        interactive | doCd  | timeout
        false       | true  | 3
        true        | false | null
    }

    def "AttemptTransition -- error"() {
        setup:
        JobLaunchException launchException = Mock(JobLaunchException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobDirectory() >> jobDir
        1 * executionContext.getJobScript() >> jobScript
        1 * executionContext.isRunFromJobDirectory() >> true
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * jobSpec.isInteractive() >> false
        1 * jobSpec.getTimeout() >> Optional.empty()
        1 * jobProcessManager.launchProcess(jobDir, jobScript, false, null, true) >> { throw launchException }
        0 * executionContext.setNextJobStatus(JobStatus.RUNNING)
        0 * executionContext.setNextJobStatusMessage(JobStatusMessages.JOB_RUNNING)
        0 * executionContext.setJobLaunched(true)
        def e = thrown(FatalJobExecutionException)
        e.getCause() == launchException

    }
}
