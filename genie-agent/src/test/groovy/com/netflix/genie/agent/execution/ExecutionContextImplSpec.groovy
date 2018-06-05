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

package com.netflix.genie.agent.execution

import com.netflix.genie.agent.execution.statemachine.States
import com.netflix.genie.agent.execution.statemachine.actions.StateAction
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.test.categories.UnitTest
import org.apache.commons.lang3.tuple.Triple
import org.junit.experimental.categories.Category
import spock.lang.Specification
import spock.lang.Unroll

@Category(UnitTest.class)
class ExecutionContextImplSpec extends Specification {

    def "Get and set all"() {
        setup:
        ExecutionContext executionContext = new ExecutionContextImpl()
        String agentId = "foo"
        Process process = Mock()
        File directory = Mock()
        JobSpecification spec = Mock()
        Map<String, String> env = [ "foo": "bar" ]
        JobStatus finalJobStatus = JobStatus.SUCCEEDED
        Exception exception = new RuntimeException()
        StateAction action1 = Mock(StateAction.SetUpJob)
        StateAction action2 = Mock(StateAction.LaunchJob)
        JobStatus jobStatus = JobStatus.INIT
        String jobId = UUID.randomUUID().toString()

        expect:
        null == executionContext.getJobProcess()
        null == executionContext.getJobDirectory()
        null == executionContext.getJobSpecification()
        null == executionContext.getJobEnvironment()
        null == executionContext.getFinalJobStatus()
        null == executionContext.getCurrentJobStatus()
        null == executionContext.getClaimedJobId()
        !executionContext.hasStateActionError()
        executionContext.getStateActionErrors().isEmpty()
        executionContext.getCleanupActions().isEmpty()

        when:
        executionContext.setJobProcess(process)
        executionContext.setJobDirectory(directory)
        executionContext.setJobSpecification(spec)
        executionContext.setJobEnvironment(env)
        executionContext.setFinalJobStatus(finalJobStatus)
        executionContext.addStateActionError(States.RESOLVE_JOB_SPECIFICATION, StateAction.ResolveJobSpecification, exception)
        executionContext.addCleanupActions(action1)
        executionContext.setCurrentJobStatus(jobStatus)
        executionContext.setClaimedJobId(jobId)

        then:
        process == executionContext.getJobProcess()
        directory == executionContext.getJobDirectory()
        spec == executionContext.getJobSpecification()
        env == executionContext.getJobEnvironment()
        finalJobStatus == executionContext.getFinalJobStatus()
        executionContext.hasStateActionError()
        1 == executionContext.getStateActionErrors().size()
        Triple.of(States.RESOLVE_JOB_SPECIFICATION, StateAction.ResolveJobSpecification, exception) == executionContext.getStateActionErrors().get(0)
        1 == executionContext.getCleanupActions().size()
        action1 == executionContext.getCleanupActions().get(0)
        jobStatus == executionContext.getCurrentJobStatus()
        jobId == executionContext.getClaimedJobId()

        when:
        executionContext.setJobProcess(Mock(Process))

        then:
        thrown(RuntimeException)

        when:
        executionContext.setJobDirectory(Mock(File))

        then:
        thrown(RuntimeException)

        when:
        executionContext.setJobSpecification(Mock(JobSpecification))

        then:
        thrown(RuntimeException)

        when:
        executionContext.setJobEnvironment(new HashMap<String, String>())

        then:
        thrown(RuntimeException)

        when:
        executionContext.setFinalJobStatus(JobStatus.FAILED)

        then:
        thrown(RuntimeException)

        when:
        executionContext.addStateActionError(States.SETUP_JOB, StateAction.SetUpJob, exception)

        then:
        executionContext.hasStateActionError()
        2 == executionContext.getStateActionErrors().size()
        Triple.of(States.RESOLVE_JOB_SPECIFICATION, StateAction.ResolveJobSpecification, exception) == executionContext.getStateActionErrors().get(0)
        Triple.of(States.SETUP_JOB, StateAction.SetUpJob, exception) == executionContext.getStateActionErrors().get(1)

        when:
        executionContext.addCleanupActions(action2)

        then:
        2 == executionContext.getCleanupActions().size()
        action1 == executionContext.getCleanupActions().get(0)
        action2 == executionContext.getCleanupActions().get(1)

        when:
        executionContext.setClaimedJobId("xxx")

        then:
        thrown(RuntimeException)
    }

    @Unroll
    def "Set final job status #jobStatus"(JobStatus jobStatus) {
        setup:
        ExecutionContext executionContext = new ExecutionContextImpl()

        when:
        executionContext.setFinalJobStatus(jobStatus)

        then:
        jobStatus == executionContext.getFinalJobStatus()

        where:
        jobStatus           | _
        JobStatus.FAILED    | _
        JobStatus.SUCCEEDED | _
        JobStatus.KILLED    | _
    }

    @Unroll
    def "Set invalid final job status #jobStatus"(JobStatus jobStatus) {
        setup:
        ExecutionContext executionContext = new ExecutionContextImpl()

        when:
        executionContext.setFinalJobStatus(jobStatus)

        then:
        thrown(IllegalArgumentException)

        where:
        jobStatus         | _
        JobStatus.INIT    | _
        JobStatus.RUNNING | _
    }
}
