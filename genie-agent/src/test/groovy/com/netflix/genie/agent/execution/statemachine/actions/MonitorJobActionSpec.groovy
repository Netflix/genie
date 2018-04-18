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
import com.netflix.genie.agent.execution.services.AgentEventsService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class MonitorJobActionSpec extends Specification {
    ExecutionContext executionContext
    MonitorJobAction action
    Process process
    AgentEventsService agentEventsService = null

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.action = new MonitorJobAction(executionContext, agentEventsService)
        this.process = Mock(Process)
    }

    void cleanup() {
    }

    def "Successful job execution"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobProcess() >> process
        1 * process.waitFor() >> 0
        1 * executionContext.setFinalJobStatus(JobStatus.SUCCEEDED)
        event == Events.MONITOR_JOB_COMPLETE
    }

    def "Unsuccessful job execution"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobProcess() >> process
        1 * process.waitFor() >> 123
        1 * executionContext.setFinalJobStatus(JobStatus.FAILED)
        event == Events.MONITOR_JOB_COMPLETE
    }

    def "Interrupt while monitoring"() {
        setup:
        def interruptException = new InterruptedException("test")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobProcess() >> process
        1 * process.waitFor() >> { throw interruptException }
        Exception e = thrown(RuntimeException)
        e.getCause() == interruptException
    }
}
