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

import com.beust.jcommander.ParameterException
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.core.env.Environment
import spock.lang.Specification

@Category(UnitTest.class)
class GenieAgentRunnerSpec extends Specification {
    ArgumentParser argsParser
    AgentCommand agentCommand
    CommandFactory cmdFactory
    Environment environment
    String[] args
    GenieAgentRunner runner

    void setup() {
        this.argsParser = Mock(ArgumentParser.class)
        this.agentCommand = Mock(AgentCommand.class)
        this.cmdFactory = Mock(CommandFactory.class)
        this.environment = Mock(Environment)
        this.args = new String[0]
        this.runner = new GenieAgentRunner(argsParser, cmdFactory, environment)
    }

    def "Successful run"() {
        when:
        runner.run(args)

        then:
        1 * argsParser.parse(args)
        1 * argsParser.getSelectedCommand() >> TestCommands.ExampleCommand1.NAME
        1 * argsParser.getCommandNames() >> TestCommands.allCommandNames()
        1 * cmdFactory.get(TestCommands.ExampleCommand1.NAME) >> agentCommand
        1 * agentCommand.run() >> ExitCode.SUCCESS

        expect:
        ExitCode.SUCCESS.getCode() == runner.getExitCode()
    }

    def "Argument parse error"() {
        setup:
        def exception = new ParameterException("...")

        when:
        runner.run(args)

        then:
        1 * argsParser.parse(args) >> { throw exception }
        0 * argsParser.getSelectedCommand() >> TestCommands.ExampleCommand1.NAME
        0 * argsParser.getCommandNames() >> TestCommands.allCommandNames()
        0 * cmdFactory.get(TestCommands.ExampleCommand1.NAME) >> agentCommand
        0 * agentCommand.run()

        expect:
        ExitCode.INVALID_ARGS.getCode() == runner.getExitCode()
    }

    def "Select no command"() {
        when:
        runner.run(args)

        then:
        1 * argsParser.parse(args)
        1 * argsParser.getSelectedCommand() >> null
        1 * argsParser.getCommandNames() >> TestCommands.allCommandNames()
        0 * cmdFactory.get(_)
        0 * agentCommand.run()

        expect:
        ExitCode.INVALID_ARGS.getCode() == runner.getExitCode()
    }

    def "Select invalid command"() {
        when:
        runner.run(args)

        then:
        1 * argsParser.parse(args)
        1 * argsParser.getSelectedCommand() >> "foo"
        1 * argsParser.getCommandNames() >> TestCommands.allCommandNames()
        0 * cmdFactory.get(_)
        0 * agentCommand.run()

        expect:
        ExitCode.INVALID_ARGS.getCode() == runner.getExitCode()
    }

    def "Fail to create command bean"() {
        when:
        runner.run(args)

        then:
        1 * argsParser.parse(args)
        1 * argsParser.getSelectedCommand() >> TestCommands.ExampleCommand1.NAME
        1 * argsParser.getCommandNames() >> TestCommands.allCommandNames()
        1 * cmdFactory.get(TestCommands.ExampleCommand1.NAME) >> { throw new NoSuchBeanDefinitionException("...") }
        0 * agentCommand.run()

        expect:
        ExitCode.COMMAND_INIT_FAIL.getCode() == runner.getExitCode()
    }

    def "Execution exception"() {
        when:
        runner.run(args)

        then:
        1 * argsParser.parse(args)
        1 * argsParser.getSelectedCommand() >> TestCommands.ExampleCommand1.NAME
        1 * argsParser.getCommandNames() >> TestCommands.allCommandNames()
        1 * cmdFactory.get(TestCommands.ExampleCommand1.NAME) >> agentCommand
        1 * agentCommand.run() >> { throw new RuntimeException("...") }

        expect:
        ExitCode.EXEC_FAIL.getCode() == runner.getExitCode()
    }

}
