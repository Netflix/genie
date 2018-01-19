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
import org.springframework.context.ApplicationContext
import spock.lang.Specification

@Category(UnitTest.class)
class CommandsConfigSpec extends Specification {
    CommandsConfig config

    void setup() {
        this.config = new CommandsConfig()
    }

    void cleanup() {
    }

    def "GlobalAgentArguments"() {
        when:
        def globalAgentArguments = config.globalAgentArguments()

        then:
        globalAgentArguments != null
        globalAgentArguments.class == GlobalAgentArguments.class
    }

    def "JCommander"() {
        when:
        def jCommander = config.jCommander(
            Mock(GlobalAgentArguments.class),
            TestCommands.allCommandArguments()
        )

        then:
        jCommander != null
        jCommander.class == JCommander.class
    }

    def "CommandFactory"() {
        when:
        def commandFactory = config.commandFactory(
            TestCommands.allCommandArguments(),
            Mock(ApplicationContext)
        )

        then:
        commandFactory != null
        commandFactory.class == CommandFactory.class
    }

    def "ArgumentParser"() {
        when:
        def argumentParser = config.argumentParser(
            Mock(JCommander.class),
            Mock(CommandFactory.class)
        )
        then:
        argumentParser != null
        argumentParser.class == ArgumentParser.class
    }
}
