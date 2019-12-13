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
import com.beust.jcommander.ParameterException
import com.beust.jcommander.ParametersDelegate
import spock.lang.Specification

class ServerArgumentsImplSpec extends Specification {

    TestOptions options
    JCommander jCommander

    void setup() {
        options = new TestOptions()
        jCommander = new JCommander(options)
    }

    void cleanup() {
    }

    def "Defaults"() {
        when:
        jCommander.parse()

        then:
        "localhost" == options.serverArguments.getServerHost()
        7979 == options.serverArguments.getServerPort()
        30L == options.serverArguments.getRpcTimeout()
    }

    def "Parse"() {
        when:
        jCommander.parse(
            "--server-host", "server.com",
            "--server-port", "1234",
            "--rpc-timeout", "100"
        )

        then:
        "server.com" == options.serverArguments.getServerHost()
        1234 == options.serverArguments.getServerPort()
        100L == options.serverArguments.getRpcTimeout()
    }

    def "InvalidServerHost"() {
        when:
        jCommander.parse(
            "--server-host", " ",
        )

        then:
        thrown(ParameterException)
    }

    def "InvalidServerPort"() {
        when:
        jCommander.parse(
            "--server-port", "-190",
        )

        then:
        thrown(ParameterException)
    }

    def "InvalidRpcTimeout"() {
        when:
        jCommander.parse(
            "--rpc-timeout", "-10",
        )

        then:
        thrown(ParameterException)
    }

    class TestOptions {
        @ParametersDelegate
        private ArgumentDelegates.ServerArguments serverArguments = new ServerArgumentsImpl()
    }
}
