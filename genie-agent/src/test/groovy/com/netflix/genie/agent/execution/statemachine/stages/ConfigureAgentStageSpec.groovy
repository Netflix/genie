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


import com.netflix.genie.agent.execution.exceptions.ConfigureException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException
import com.netflix.genie.agent.properties.AgentProperties
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata
import spock.lang.Specification

import java.time.Duration

class ConfigureAgentStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    AgentJobService agentJobService
    AgentClientMetadata agentClientMetadata
    AgentProperties agentProperties

    void setup() {
        this.agentClientMetadata = Mock(AgentClientMetadata)
        this.executionContext = Mock(ExecutionContext)
        this.agentJobService = Mock(AgentJobService)
        this.agentProperties = new AgentProperties()
        this.stage = new ConfigureAgentStage(
            agentJobService
        )
    }

    def "Attempt transition -- no server values does not override/wipe defaults"() {
        def killMaxDelayValue = agentProperties.getJobKillService().getResponseCheckBackOff().getMaxDelay()
        def emergencyShutdownDelayValue = agentProperties.getEmergencyShutdownDelay()

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getAgentClientMetadata() >> agentClientMetadata
        1 * executionContext.getAgentProperties() >> agentProperties
        1 * agentJobService.configure(agentClientMetadata) >> [:]

        expect:
        agentProperties.getJobKillService().getResponseCheckBackOff().getMaxDelay() == killMaxDelayValue
        agentProperties.getEmergencyShutdownDelay() == emergencyShutdownDelayValue
    }

    def "Attempt transition -- server values does override defaults, unknown properties are ignored"() {
        def killMaxDelayValue = agentProperties.getJobKillService().getResponseCheckBackOff().getMaxDelay()
        def emergencyShutdownDelayValue = agentProperties.getEmergencyShutdownDelay()
        def heartbeatInterval = agentProperties.getHeartBeatService().getInterval()

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getAgentClientMetadata() >> agentClientMetadata
        1 * executionContext.getAgentProperties() >> agentProperties
        1 * agentJobService.configure(agentClientMetadata) >> [
            'genie.agent.runtime.heart-beat-service.interval': '10s',
            'foo.bar'                                        : 'blah',
        ]

        expect:
        agentProperties.getJobKillService().getResponseCheckBackOff().getMaxDelay() == killMaxDelayValue
        agentProperties.getEmergencyShutdownDelay() == emergencyShutdownDelayValue
        agentProperties.getHeartBeatService().getInterval() != heartbeatInterval
        agentProperties.getHeartBeatService().getInterval() == Duration.ofSeconds(10)
    }


    def "Attempt transition -- handle service exception"() {
        Exception configureException = new ConfigureException("...")

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getAgentClientMetadata() >> agentClientMetadata
        1 * executionContext.getAgentProperties() >> agentProperties
        1 * agentJobService.configure(agentClientMetadata) >> { throw configureException }
        def e = thrown(RetryableJobExecutionException)
        e.getCause() == configureException
    }
}
