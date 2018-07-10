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

package com.netflix.genie.agent.cli

import com.netflix.genie.agent.execution.services.AgentHeartBeatService
import org.apache.commons.lang3.RandomUtils
import org.apache.commons.lang3.StringUtils
import spock.lang.Specification

class HeartBeatCommandSpec extends Specification {

    AgentHeartBeatService service
    HeartBeatCommand.HeartBeatCommandArguments commandArguments
    HeartBeatCommand command

    void setup() {
        this.service = Mock(AgentHeartBeatService)
        this.commandArguments = Mock(HeartBeatCommand.HeartBeatCommandArguments)
        this.command = new HeartBeatCommand(commandArguments, service)

    }

    void cleanup() {
    }

    def "Run"() {
        when:
        command.run()

        then:
        1 * service.start(_ as String) >> {
            args ->
                assert StringUtils.isNotBlank(args[0] as String)
        }
        _ * service.isConnected() >> {
            return RandomUtils.nextBoolean()
        }
        1 * commandArguments.getRunDuration() >> 2
        1 * service.stop()
    }
}
