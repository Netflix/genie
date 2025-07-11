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
package com.netflix.genie.web.services.impl

import com.netflix.genie.web.services.ClusterLeaderService
import spock.lang.Specification
import org.springframework.integration.zookeeper.leader.LeaderInitiator
import org.springframework.integration.zookeeper.leader.LeaderInitiator.CuratorContext

class ClusterLeaderServiceCuratorImplSpec extends Specification {
    LeaderInitiator leaderInitiator
    ClusterLeaderService service

    void setup() {
        this.leaderInitiator = Mock(LeaderInitiator)
        this.service = new ClusterLeaderServiceCuratorImpl(leaderInitiator)
    }

    def "Start"() {
        when:
        this.service.start()

        then:
        1 * leaderInitiator.start()
    }

    def "Stop"() {
        when:
        this.service.stop()

        then:
        1 * leaderInitiator.stop()
    }

    def "isLeader"() {
        def mockContext = Mock(CuratorContext)
        mockContext.isLeader() >> true

        def mockLeaderInitiator = Mock(LeaderInitiator)
        mockLeaderInitiator.getContext() >> mockContext

        def service = new ClusterLeaderServiceCuratorImpl(mockLeaderInitiator)

        expect:
        service.isLeader()
    }


    def "isRunning"() {
        when:
        boolean isRunning = this.service.isRunning()

        then:
        1 * leaderInitiator.isRunning() >> true
        isRunning
    }
}
