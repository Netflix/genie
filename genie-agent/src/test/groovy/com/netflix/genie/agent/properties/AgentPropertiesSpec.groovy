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
package com.netflix.genie.agent.properties

import spock.lang.Specification

import java.time.Duration

class AgentPropertiesSpec extends Specification {

    AgentProperties agentProperties

    void setup() {
        agentProperties = new AgentProperties()
    }

    def "defaults, getters setters"() {
        expect:
        agentProperties.getEmergencyShutdownDelay() == Duration.ofMinutes(5)
        agentProperties.getForceManifestRefreshTimeout() == Duration.ofSeconds(5)
        agentProperties.getFileStreamService() != null
        agentProperties.getHeartBeatService() != null
        agentProperties.getJobKillService() != null
        agentProperties.getJobMonitorService() != null
        agentProperties.getShutdown() != null

        when:
        def fileStreamServiceProps = Mock(FileStreamServiceProperties)
        def heartBeatServiceProps = Mock(HeartBeatServiceProperties)
        def jobKillServiceProps = Mock(JobKillServiceProperties)
        def jobLimitsProps = Mock(JobMonitorServiceProperties)
        def shutdownProps = Mock(ShutdownProperties)

        agentProperties.setEmergencyShutdownDelay(Duration.ofMinutes(10))
        agentProperties.setForceManifestRefreshTimeout(Duration.ofSeconds(10))
        agentProperties.setFileStreamService(fileStreamServiceProps)
        agentProperties.setHeartBeatService(heartBeatServiceProps)
        agentProperties.setJobKillService(jobKillServiceProps)
        agentProperties.setJobMonitorService(jobLimitsProps)
        agentProperties.setShutdown(shutdownProps)

        then:
        agentProperties.getEmergencyShutdownDelay() == Duration.ofMinutes(10)
        agentProperties.getForceManifestRefreshTimeout() == Duration.ofSeconds(10)
        agentProperties.getFileStreamService() == fileStreamServiceProps
        agentProperties.getHeartBeatService() == heartBeatServiceProps
        agentProperties.getJobKillService() == jobKillServiceProps
        agentProperties.getJobMonitorService() == jobLimitsProps
        agentProperties.getShutdown() == shutdownProps
    }
}
