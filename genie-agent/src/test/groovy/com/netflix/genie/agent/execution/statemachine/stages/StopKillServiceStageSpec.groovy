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

import com.netflix.genie.agent.execution.services.AgentJobKillService
import spock.lang.Specification

class StopKillServiceStageSpec extends Specification {
    StopServiceStage stage
    AgentJobKillService service

    void setup() {
        this.service = Mock(AgentJobKillService)
        this.stage = new StopKillServiceStage(service)
    }

    def "AttemptTransition"() {
        when:
        stage.stopService()

        then:
        1 * service.stop()
    }
}
