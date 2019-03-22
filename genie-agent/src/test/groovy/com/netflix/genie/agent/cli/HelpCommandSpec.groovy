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

import spock.lang.Specification

class HelpCommandSpec extends Specification {
    ArgumentParser argsParser

    void setup() {
        argsParser = Mock()
    }

    void cleanup() {
    }

    def "Run"() {
        setup:
        def execCommand = new HelpCommand(argsParser)

        when:
        ExitCode exitCode = execCommand.run()

        then:
        1 * argsParser.getUsageMessage() >> "Usage: ..."
        exitCode == ExitCode.SUCCESS
    }
}
