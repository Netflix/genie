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
import com.netflix.genie.web.tasks.leader.LocalLeader
import spock.lang.Specification

class ClusterLeaderServiceLocalLeaderImplSpec extends Specification {
    LocalLeader localLeader
    ClusterLeaderService service

    void setup() {
        this.localLeader = Mock(LocalLeader)
        this.service = new ClusterLeaderServiceLocalLeaderImpl(localLeader)
    }

    def "Start"() {
        when:
        this.service.start()

        then:
        1 * localLeader.start()
    }

    def "Stop"() {
        when:
        this.service.stop()

        then:
        1 * localLeader.stop()
    }

    def "isLeader"() {
        when:
        boolean isLeader = this.service.isLeader()

        then:
        1 * localLeader.isLeader() >> true
        isLeader
    }


    def "isRunning"() {
        when:
        boolean isRunning = this.service.isRunning()

        then:
        1 * localLeader.isRunning() >> true
        isRunning
    }
}
