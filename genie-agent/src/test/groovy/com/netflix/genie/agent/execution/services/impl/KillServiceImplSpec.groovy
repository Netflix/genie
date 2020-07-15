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
package com.netflix.genie.agent.execution.services.impl

import com.netflix.genie.agent.execution.services.KillService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.JobExecutionStateMachine
import com.netflix.genie.agent.properties.AgentProperties
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.ThreadFactory

class KillServiceImplSpec extends Specification {
    ExecutionContext executionContext
    JobExecutionStateMachine stateMachine
    KillService service
    AgentProperties agentProperties
    ThreadFactory threadFactory

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.stateMachine = Mock(JobExecutionStateMachine)
        this.agentProperties = new AgentProperties()
        this.threadFactory = Mock(ThreadFactory)
        this.service = new KillServiceImpl(executionContext, agentProperties, threadFactory)
    }

    @Unroll
    def "Kill via #source"() {
        when:
        service.kill(source)

        then:
        1 * executionContext.getStateMachine() >> stateMachine
        1 * stateMachine.kill(source)
        1 * threadFactory.newThread(_ as Runnable) >> Mock(Thread)

        where:
        source                                       | _
        KillService.KillSource.SYSTEM_SIGNAL         | _
        KillService.KillSource.API_KILL_REQUEST      | _
        KillService.KillSource.TIMEOUT               | _
        KillService.KillSource.FILES_LIMIT           | _
        KillService.KillSource.REMOTE_STATUS_MONITOR | _
    }
}
