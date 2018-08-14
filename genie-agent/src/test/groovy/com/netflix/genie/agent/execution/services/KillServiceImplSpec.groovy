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

package com.netflix.genie.agent.execution.services

import com.netflix.genie.agent.execution.services.impl.KillServiceImpl
import com.netflix.genie.agent.execution.statemachine.JobExecutionStateMachine
import spock.lang.Specification
import spock.lang.Unroll

class KillServiceImplSpec extends Specification {
    KillService service
    JobExecutionStateMachine jobExecutionStateMachine
    LaunchJobService launchJobService

    void setup() {
        jobExecutionStateMachine = Mock()
        launchJobService = Mock()
    }

    @Unroll
    def "Kill via #source"() {
        setup:
        service = new KillServiceImpl(jobExecutionStateMachine, launchJobService)

        when:
        service.kill(source)

        then:
        1 * jobExecutionStateMachine.stop()
        1 * launchJobService.kill(sendSigInt)

        where:
        source                                  | sendSigInt
        KillService.KillSource.SYSTEM_SIGNAL    | false
        KillService.KillSource.API_KILL_REQUEST | true
    }
}
