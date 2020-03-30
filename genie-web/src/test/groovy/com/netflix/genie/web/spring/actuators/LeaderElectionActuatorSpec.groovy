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
package com.netflix.genie.web.spring.actuators

import com.netflix.genie.web.services.ClusterLeaderService
import spock.lang.Specification

class LeaderElectionActuatorSpec extends Specification {
    ClusterLeaderService clusterLeaderService
    LeaderElectionActuator actuator

    void setup() {
        this.clusterLeaderService = Mock(ClusterLeaderService)
        this.actuator = new LeaderElectionActuator(clusterLeaderService)
    }

    def "Status"() {
        when:
        Map<String, Object> status = this.actuator.getStatus()

        then:
        1 * clusterLeaderService.isRunning() >> running
        1 * clusterLeaderService.isLeader() >> leader
        status.get(LeaderElectionActuator.RUNNING) == running
        status.get(LeaderElectionActuator.LEADER) == leader

        where:
        running | leader
        true    | false
        false   | true

    }

    def "Actions"() {
        when:
        this.actuator.doAction(LeaderElectionActuator.Action.START)

        then:
        1 * clusterLeaderService.start()

        when:
        this.actuator.doAction(LeaderElectionActuator.Action.STOP)

        then:
        1 * clusterLeaderService.stop()

        when:
        this.actuator.doAction(LeaderElectionActuator.Action.RESTART)

        then:
        1 * clusterLeaderService.stop()
        1 * clusterLeaderService.start()

        when:
        this.actuator.doAction(LeaderElectionActuator.Action.TEST)

        then:
        thrown(UnsupportedOperationException)
    }
}
