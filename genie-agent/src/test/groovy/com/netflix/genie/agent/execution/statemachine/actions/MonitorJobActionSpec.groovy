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
import com.netflix.genie.agent.execution.services.KillService
import com.netflix.genie.agent.execution.services.LaunchJobService
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
    LaunchJobService launchJobService

    void setup() {
        this.id = UUID.randomUUID().toString()
        this.executionContext = Mock(ExecutionContext)
        this.agentJobService = Mock(AgentJobService)
        this.launchJobService = Mock(LaunchJobService)
        this.process = Mock(Process)
        this.action = new MonitorJobAction(executionContext, agentJobService, launchJobService)
    }

    @Unroll
    def "Successful #expectedJobStatus"() {
        JobStatus currentJobStatus = JobStatus.RUNNING

        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * launchJobService.waitFor() >> expectedJobStatus
        1 * executionContext.getClaimedJobId() >> id
        1 * executionContext.getCurrentJobStatus() >> currentJobStatus
        1 * agentJobService.changeJobStatus(id, currentJobStatus, expectedJobStatus, _ as String)
        1 * executionContext.setCurrentJobStatus(expectedJobStatus)
        1 * executionContext.setFinalJobStatus(expectedJobStatus)

        expect:
        event == Events.MONITOR_JOB_COMPLETE

        where:
        _ | expectedJobStatus
        _ | JobStatus.SUCCEEDED
        _ | JobStatus.FAILED
        _ | JobStatus.KILLED
    }

    def "Interrupt while monitoring"() {
        setup:
        def exception = new InterruptedException("...")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * launchJobService.waitFor() >> { throw exception }
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
        1 * launchJobService.waitFor() >> expectedJobStatus
        1 * executionContext.getClaimedJobId() >> id
        1 * executionContext.getCurrentJobStatus() >> currentJobStatus
        1 * agentJobService.changeJobStatus(id, currentJobStatus, expectedJobStatus, _ as String) >> { throw exception }
        Exception e = thrown(RuntimeException)
        e.getCause() == exception
    }
}
