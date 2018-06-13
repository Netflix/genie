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

@Category(UnitTest.class)
class HandleErrorActionSpec extends Specification {
    String id
    ExecutionContext executionContext
    HandleErrorAction action
    AgentJobService agentJobService

    void setup() {
        this.id = UUID.randomUUID().toString()
        this.executionContext = Mock(ExecutionContext)
        this.agentJobService = Mock(AgentJobService)
        this.action = new HandleErrorAction(executionContext, agentJobService)
    }

    def "Successful"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getCurrentJobStatus() >> JobStatus.RUNNING
        1 * executionContext.getClaimedJobId() >> id
        1 * agentJobService.changeJobStatus(id, JobStatus.RUNNING, JobStatus.FAILED, _ as String)

        event == Events.HANDLE_ERROR_COMPLETE
    }

    def "Skip job status update"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getCurrentJobStatus() >> null
        1 * executionContext.getClaimedJobId() >> null
        0 * agentJobService.changeJobStatus(_, _, _, _)

        event == Events.HANDLE_ERROR_COMPLETE
    }

    def "Change job status exception"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getCurrentJobStatus() >> JobStatus.RUNNING
        1 * executionContext.getClaimedJobId() >> id
        1 * agentJobService.changeJobStatus(id, JobStatus.RUNNING, JobStatus.FAILED, _ as String) >> {throw new ChangeJobStatusException("...")}

        event == Events.HANDLE_ERROR_COMPLETE
    }

    def "Change job status runtime exception"() {
        Exception exception = new RuntimeException("...")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getCurrentJobStatus() >> JobStatus.RUNNING
        1 * executionContext.getClaimedJobId() >> id
        1 * agentJobService.changeJobStatus(id, JobStatus.RUNNING, JobStatus.FAILED, _ as String) >> {throw exception}

        thrown(exception.class)
    }

}
