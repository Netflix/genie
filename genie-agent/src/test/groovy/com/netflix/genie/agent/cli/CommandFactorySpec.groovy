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

import com.google.common.collect.ImmutableList
import org.springframework.beans.factory.config.AutowireCapableBeanFactory
import org.springframework.context.ApplicationContext
import spock.lang.Specification

class CommandFactorySpec extends Specification {

    CommandFactory factory

    void setup() {
        List<AgentCommandArguments> commandArgs = ImmutableList.builder()
            .addAll(TestCommands.allCommandArguments())
            .addAll(TestCommands.allCommandArguments())
            .build()

        def mockApplicationContext = Mock(ApplicationContext)
        def mockBeanFactory = Mock(AutowireCapableBeanFactory)
        mockBeanFactory.getBean(TestCommands.ExampleCommand1.class) >> new TestCommands.ExampleCommand1()
        mockBeanFactory.getBean(TestCommands.ExampleCommand2.class) >> new TestCommands.ExampleCommand2()
        mockApplicationContext.getAutowireCapableBeanFactory() >> mockBeanFactory
        this.factory = new CommandFactory(
            commandArgs,
            mockApplicationContext
        )
    }

    def "GetCommandNames"() {
        setup:
        def expectedNames = new HashSet<String>(["example1", "ex1", "example2", "ex2"])

        when:
        def names = factory.getCommandNames()

        then:
        expectedNames == names
    }

    def "GetCommand"() {
        when:
        def command1 = factory.get("example1")
        def command2 = factory.get("ex2")
        def command3 = factory.get("none")

        then:
        TestCommands.ExampleCommand1.class == command1.class
        TestCommands.ExampleCommand2.class == command2.class
        null == command3
    }
}
