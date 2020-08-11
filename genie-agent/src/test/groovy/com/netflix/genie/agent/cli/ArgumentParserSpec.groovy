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

import com.beust.jcommander.IUsageFormatter
import com.beust.jcommander.JCommander
import spock.lang.Specification

class ArgumentParserSpec extends Specification {

    JCommander mockJCommander
    CommandFactory mockCommandFactory
    MainCommandArguments mockMainCommandArguments

    void setup() {
        mockJCommander = Mock(JCommander.class)
        mockCommandFactory = Mock(CommandFactory.class)
        mockMainCommandArguments = Mock(MainCommandArguments.class)
    }

    def "Construct"() {
        setup:
        new ArgumentParser(mockJCommander, mockCommandFactory, mockMainCommandArguments)
    }

    def "Parse"() {
        setup:
        def parser = new ArgumentParser(mockJCommander, mockCommandFactory, mockMainCommandArguments)
        String[] args = ["foo", "bar", "--", "baz"]

        when:
        parser.parse(args)

        then:
        1 * mockJCommander.parse(["foo", "bar"] as String[])
        1 * mockMainCommandArguments.set(["baz"] as String[])
    }

    def "GetUsageMessage"() {
        setup:
        def parser = new ArgumentParser(mockJCommander, mockCommandFactory, mockMainCommandArguments)
        def usageFormatter = Mock(IUsageFormatter)

        when:
        parser.getUsageMessage()

        then:
        1 * mockJCommander.getUsageFormatter() >> usageFormatter
        1 * usageFormatter.usage(_ as StringBuilder)
    }

    def "GetSelectedCommand"() {
        setup:
        def parser = new ArgumentParser(mockJCommander, mockCommandFactory, mockMainCommandArguments)

        when:
        parser.getSelectedCommand()
        then:
        1 * mockJCommander.getParsedCommand()
    }

    def "GetCommandNames"() {
        setup:
        def parser = new ArgumentParser(mockJCommander, mockCommandFactory, mockMainCommandArguments)

        when:
        parser.getCommandNames()

        then:
        1 * mockCommandFactory.getCommandNames()
    }
}
