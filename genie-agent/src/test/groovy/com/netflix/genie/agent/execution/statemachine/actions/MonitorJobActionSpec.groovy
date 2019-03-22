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
import com.netflix.genie.agent.execution.services.LaunchJobService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.dto.JobStatus
import spock.lang.Specification
import spock.lang.Unroll

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
        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * launchJobService.waitFor() >> expectedJobStatus
        1 * executionContext.getClaimedJobId() >> Optional.of(id)
        1 * agentJobService.changeJobStatus(id, JobStatus.RUNNING, expectedJobStatus, _ as String)
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
        JobStatus expectedJobStatus = JobStatus.SUCCEEDED
        Exception exception = new ChangeJobStatusException("...")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * launchJobService.waitFor() >> expectedJobStatus
        1 * executionContext.getClaimedJobId() >> Optional.of(id)
        1 * agentJobService.changeJobStatus(id, JobStatus.RUNNING, expectedJobStatus, _ as String) >> {
            throw exception
        }
        Exception e = thrown(RuntimeException)
        e.getCause() == exception
    }

    def "Pre and post action validation"() {
        when:
        action.executePreActionValidation()

        then:
        1 * executionContext.getClaimedJobId() >> Optional.of(id)
        1 * executionContext.getCurrentJobStatus() >> Optional.of(JobStatus.RUNNING)
        1 * executionContext.getFinalJobStatus() >> Optional.empty()

        when:
        action.executePostActionValidation()

        then:
        1 * executionContext.getCurrentJobStatus() >> Optional.of(JobStatus.KILLED)
        2 * executionContext.getFinalJobStatus() >> Optional.of(JobStatus.KILLED)
    }
}
