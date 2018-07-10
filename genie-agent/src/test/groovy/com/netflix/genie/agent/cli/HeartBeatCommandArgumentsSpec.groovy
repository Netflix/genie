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

import com.beust.jcommander.JCommander
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class HeartBeatCommandArgumentsSpec extends Specification {

    HeartBeatCommand.HeartBeatCommandArguments options
    JCommander jCommander
    ArgumentDelegates.ServerArguments serverArguments

    void setup() {
        serverArguments = new ServerArgumentsImpl()
        options = new HeartBeatCommand.HeartBeatCommandArguments(serverArguments)
        jCommander = new JCommander(options)
    }

    void cleanup() {
    }

    def "Defaults"() {
        when:
        jCommander.parse()

        then:
        "genie.prod.netflix.net" == options.getServerArguments().getServerHost()
        7979 == options.getServerArguments().getServerPort()
        30L == options.getServerArguments().getRpcTimeout()
        0 == options.getRunDuration()
    }

    def "Parse"() {
        String requestId = UUID.randomUUID().toString()
        when:
        jCommander.parse(
            "--serverHost", "server.com",
            "--serverPort", "1234",
            "--rpcTimeout", "100",
            "--duration", "60"
        )

        then:
        "server.com" == options.getServerArguments().getServerHost()
        1234 == options.getServerArguments().getServerPort()
        100L == options.getServerArguments().getRpcTimeout()
        60 == options.getRunDuration()
    }
}
