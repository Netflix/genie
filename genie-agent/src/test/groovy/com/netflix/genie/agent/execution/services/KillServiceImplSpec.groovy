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
import org.springframework.context.ApplicationEventPublisher
import spock.lang.Specification
import spock.lang.Unroll

class KillServiceImplSpec extends Specification {
    ApplicationEventPublisher applicationEventPublisher
    KillService service

    void setup() {
        applicationEventPublisher = Mock(ApplicationEventPublisher)
        service = new KillServiceImpl(applicationEventPublisher)
    }

    @Unroll
    def "Kill via #source"() {
        setup:

        when:
        service.kill(source)

        then:
        1 * applicationEventPublisher.publishEvent(_ as KillService.KillEvent) >> {
            args ->
                assert args[0] != null
                assert (args[0] as KillService.KillEvent).getKillSource() == source
        }

        where:
        source                                  | _
        KillService.KillSource.SYSTEM_SIGNAL    | _
        KillService.KillSource.API_KILL_REQUEST | _
    }
}
