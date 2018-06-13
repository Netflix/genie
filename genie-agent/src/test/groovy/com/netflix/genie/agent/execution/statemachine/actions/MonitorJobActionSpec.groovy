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

package com.netflix.genie.agent.execution.statemachine.actions

import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification
import spock.lang.Unroll

@Category(UnitTest.class)
class MonitorJobActionSpec extends Specification {
    String id
    ExecutionContext executionContext
    MonitorJobAction action
    Process process
    AgentJobService agentJobService

    void setup() {
        this.id = UUID.randomUUID().toString()
        this.executionContext = Mock(ExecutionContext)
        this.agentJobService = Mock(AgentJobService)
        this.process = Mock(Process)
        this.action = new MonitorJobAction(executionContext, agentJobService)
    }

    @Unroll
    def "Successful, expecting job status #expectedJobStatus"() {
        JobStatus currentJobStatus = JobStatus.RUNNING

        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobProcess() >> process
        1 * process.waitFor() >> exitCode
        1 * executionContext.setFinalJobStatus(expectedJobStatus)
        1 * executionContext.getClaimedJobId() >> id
        1 * executionContext.getCurrentJobStatus() >> currentJobStatus
        1 * agentJobService.changeJobStatus(id, currentJobStatus, expectedJobStatus, _ as String)
        1 * executionContext.setCurrentJobStatus(expectedJobStatus)

        expect:
        event == Events.MONITOR_JOB_COMPLETE

        where:
        exitCode | expectedJobStatus
        0        | JobStatus.SUCCEEDED
        123L     | JobStatus.FAILED
    }

    def "Interrupt while monitoring"() {
        setup:
        def exception = new InterruptedException("...")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobProcess() >> process
        1 * process.waitFor() >> { throw exception }
        Exception e = thrown(RuntimeException)
        e.getCause() == exception
    }

    def "Change job status exception"() {
        JobStatus currentJobStatus = JobStatus.RUNNING
        JobStatus expectedJobStatus = JobStatus.SUCCEEDED
        Exception exception = new ChangeJobStatusException("...")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobProcess() >> process
        1 * process.waitFor() >> 0
        1 * executionContext.setFinalJobStatus(expectedJobStatus)
        1 * executionContext.getClaimedJobId() >> id
        1 * executionContext.getCurrentJobStatus() >> currentJobStatus
        1 * agentJobService.changeJobStatus(id, currentJobStatus, expectedJobStatus, _ as String) >> { throw exception }
        0 * executionContext.setCurrentJobStatus(_)
        Exception e = thrown(RuntimeException)
        e.getCause() == exception
    }
}
