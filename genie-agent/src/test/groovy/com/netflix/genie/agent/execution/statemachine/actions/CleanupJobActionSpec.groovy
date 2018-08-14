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
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.dto.JobStatus
import org.assertj.core.util.Lists
import spock.lang.Specification

class CleanupJobActionSpec extends Specification {
    ExecutionContext executionContext
    CleanupJobAction action
    List<StateAction> cleanupQueue
    AgentJobService agentJobService
    String jobId

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.agentJobService = Mock(AgentJobService)
        this.action = new CleanupJobAction(executionContext, agentJobService)
        this.cleanupQueue = Lists.newArrayList()
        this.jobId = UUID.randomUUID().toString()
    }

    void cleanup() {
    }

    def "Execute with unclaimed job and no cleanup actions"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        event == Events.CLEANUP_JOB_COMPLETE
        1 * executionContext.getClaimedJobId() >> null
        1 * executionContext.getFinalJobStatus() >> JobStatus.INIT
        0 * executionContext.getCurrentJobStatus()
        0 * agentJobService.changeJobStatus(_ as String, _ as JobStatus, _ as JobStatus, _ as String)
        0 * executionContext.setFinalJobStatus(_ as JobStatus)
        0 * executionContext.setCurrentJobStatus(_ as JobStatus)
        1 * executionContext.getCleanupActions() >> cleanupQueue
    }

    def "Execute with unclaimed job and some cleanup actions"() {
        setup:
        def action1 = Mock(StateAction)
        def action2 = Mock(StateAction)
        cleanupQueue.add(action1)
        cleanupQueue.add(action2)
        cleanupQueue.add(action)

        when:
        def event = action.executeStateAction(executionContext)

        then:
        event == Events.CLEANUP_JOB_COMPLETE
        1 * executionContext.getClaimedJobId() >> null
        1 * executionContext.getFinalJobStatus() >> JobStatus.INIT
        0 * executionContext.getCurrentJobStatus()
        0 * agentJobService.changeJobStatus(_ as String, _ as JobStatus, _ as JobStatus, _ as String)
        0 * executionContext.setFinalJobStatus(_ as JobStatus)
        0 * executionContext.setCurrentJobStatus(_ as JobStatus)
        1 * executionContext.getCleanupActions() >> cleanupQueue
        1 * action1.cleanup()
        1 * action2.cleanup()
        0 * action.cleanup()
    }

    def "Execute with unclaimed job and throwing cleanup action"() {
        setup:
        def action1 = Mock(StateAction)
        def action2 = Mock(StateAction)
        cleanupQueue.add(action1)
        cleanupQueue.add(action2)
        cleanupQueue.add(action)

        when:
        def event = action.executeStateAction(executionContext)

        then:
        event == Events.CLEANUP_JOB_COMPLETE
        1 * executionContext.getClaimedJobId() >> null
        1 * executionContext.getFinalJobStatus() >> JobStatus.INIT
        0 * executionContext.getCurrentJobStatus()
        0 * agentJobService.changeJobStatus(_ as String, _ as JobStatus, _ as JobStatus, _ as String)
        0 * executionContext.setFinalJobStatus(_ as JobStatus)
        0 * executionContext.setCurrentJobStatus(_ as JobStatus)
        1 * executionContext.getCleanupActions() >> cleanupQueue
        1 * action1.cleanup() >> {throw new RuntimeException()}
        1 * action2.cleanup()
        0 * action.cleanup()
    }

    def "Execute with claimed job and aborted job launch"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        event == Events.CLEANUP_JOB_COMPLETE
        1 * executionContext.getClaimedJobId() >> jobId
        1 * executionContext.getFinalJobStatus() >> null
        1 * executionContext.getCurrentJobStatus() >> JobStatus.CLAIMED
        1 * agentJobService.changeJobStatus(
            jobId,
            JobStatus.CLAIMED,
            JobStatus.KILLED,
            _ as String
        )
        1 * executionContext.setFinalJobStatus(JobStatus.KILLED)
        1 * executionContext.setCurrentJobStatus(JobStatus.KILLED)
        1 * executionContext.getCleanupActions() >> cleanupQueue
    }

    def "Execute with claimed job and exception updating status"() {
        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getClaimedJobId() >> jobId
        1 * executionContext.getFinalJobStatus() >> null
        1 * executionContext.getCurrentJobStatus() >> JobStatus.CLAIMED
        1 * agentJobService.changeJobStatus(
            jobId,
            JobStatus.CLAIMED,
            JobStatus.KILLED,
            _ as String
        ) >> { throw new ChangeJobStatusException("test")}
        thrown(RuntimeException)
    }
}
