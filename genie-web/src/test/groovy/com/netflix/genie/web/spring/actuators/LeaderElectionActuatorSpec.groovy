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
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes
import spock.lang.Specification

class LeaderElectionActuatorSpec extends Specification {
    ClusterLeaderService clusterLeaderService
    LeaderElectionActuator actuator
    MockHttpServletRequest request

    void setup() {
        this.clusterLeaderService = Mock(ClusterLeaderService)
        this.actuator = new LeaderElectionActuator(clusterLeaderService)
        this.request = new MockHttpServletRequest()
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request))
    }

    void cleanup() {
        RequestContextHolder.resetRequestAttributes()
    }

    def "Status"() {
        when:
        Map<String, Object> status = this.actuator.getStatus()

        then:
        1 * clusterLeaderService.isRunning() >> running
        1 * clusterLeaderService.isLeader() >> leader
        status.get("running") == running
        status.get("leader") == leader

        where:
        running | leader
        true    | false
        false   | true
    }

    def "Actions"() {
        when:
        request.addParameter("action", "START")
        this.actuator.doAction()

        then:
        1 * clusterLeaderService.start()
        noExceptionThrown()

        when:
        request = new MockHttpServletRequest()
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request))
        request.addParameter("action", "STOP")
        this.actuator.doAction()

        then:
        1 * clusterLeaderService.stop()
        noExceptionThrown()

        when:
        request = new MockHttpServletRequest()
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request))
        request.addParameter("action", "RESTART")
        this.actuator.doAction()

        then:
        1 * clusterLeaderService.stop()
        1 * clusterLeaderService.start()
        noExceptionThrown()

        when:
        request = new MockHttpServletRequest()
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request))
        request.addParameter("action", "TEST")
        this.actuator.doAction()

        then:
        0 * clusterLeaderService._
        thrown(UnsupportedOperationException)
    }

    def "Action with invalid or missing parameters"() {
        when:
        RequestContextHolder.resetRequestAttributes()
        this.actuator.doAction()

        then:
        thrown(IllegalStateException)

        when:
        request = new MockHttpServletRequest()
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request))
        this.actuator.doAction()

        then:
        thrown(IllegalArgumentException)

        when:
        request.addParameter("action", "INVALID_ACTION")
        this.actuator.doAction()

        then:
        thrown(IllegalArgumentException)
    }
}
