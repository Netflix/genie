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
class ArgumentParserSpec extends Specification {

    JCommander mockJCommander
    CommandFactory mockCommandFactory

    void setup() {
        mockJCommander = Mock(JCommander.class)
        mockCommandFactory = Mock(CommandFactory.class)
    }

    def "Construct"() {
        setup:
        new ArgumentParser(mockJCommander, mockCommandFactory)
    }

    def "Parse"() {
        setup:
        def parser = new ArgumentParser(mockJCommander, mockCommandFactory)
        String[] args = ["foo", "bar", "baz"]

        when:
        parser.parse(args)

        then:
        1 * mockJCommander.parse(args)
    }

    def "GetUsageMessage"() {
        setup:
        def parser = new ArgumentParser(mockJCommander, mockCommandFactory)

        when:
        parser.getUsageMessage()

        then:
        1 * mockJCommander.usage(_)
    }

    def "GetSelectedCommand"() {
        setup:
        def parser = new ArgumentParser(mockJCommander, mockCommandFactory)

        when:
        parser.getSelectedCommand()
        then:
        1 * mockJCommander.getParsedCommand()
    }

    def "GetCommandNames"() {
        setup:
        def parser = new ArgumentParser(mockJCommander, mockCommandFactory)

        when:
        parser.getCommandNames()

        then:
        1 * mockCommandFactory.getCommandNames()
    }
}
