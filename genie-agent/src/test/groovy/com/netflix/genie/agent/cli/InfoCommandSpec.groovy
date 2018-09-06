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

import com.netflix.genie.agent.AgentMetadata
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.Environment
import org.springframework.core.env.MutablePropertySources
import spock.lang.Specification

@Category(UnitTest.class)
class InfoCommandSpec extends Specification {
    InfoCommand.InfoCommandArguments args
    ConfigurableApplicationContext ctx
    AgentMetadata agentMetadata
    ConfigurableEnvironment env
    Map<String, Object> map = ["Foo" : "foo", "Bar" : new Object(), "Baz" : null]


    void setup() {
        args = Mock()
        ctx = Mock()
        env = Mock()
        agentMetadata = Mock()
    }

    void cleanup() {
    }

    def "Run"() {
        setup:
        def cmd = new InfoCommand(args, ctx, agentMetadata)

        when:
        ExitCode exitCode = cmd.run()

        then:
        1 * agentMetadata.getAgentVersion() >> "1.0.0"
        1 * agentMetadata.getAgentHostName() >> "host-name"
        1 * agentMetadata.getAgentPid() >> "12345"
        1 * args.getIncludeBeans() >> true
        1 * ctx.getBeanDefinitionCount() >> 0
        1 * ctx.getBeanDefinitionNames() >> new String[0]
        1 * args.getIncludeEnvironment() >> true
        1 * args.isIncludeProperties() >> true
        5 * ctx.getEnvironment() >> env
        1 * env.getActiveProfiles() >> ["foo", "bar"]
        1 * env.getDefaultProfiles() >> ["default"]
        1 * env.getSystemEnvironment() >> map
        1 * env.getSystemProperties() >> map
        1 * env.getPropertySources() >> new MutablePropertySources()
        exitCode == ExitCode.SUCCESS
    }

    def "Run skip all"() {
        setup:
        def cmd = new InfoCommand(args, ctx, agentMetadata)

        when:
        ExitCode exitCode = cmd.run()

        then:
        1 * agentMetadata.getAgentVersion() >> "1.0.0"
        1 * agentMetadata.getAgentHostName() >> "host-name"
        1 * agentMetadata.getAgentPid() >> "12345"
        1 * args.getIncludeBeans() >> false
        0 * ctx.getBeanDefinitionCount()
        0 * ctx.getBeanDefinitionNames()
        1 * args.getIncludeEnvironment() >> false
        1 * args.isIncludeProperties() >> false
        2 * ctx.getEnvironment() >> env
        1 * env.getActiveProfiles() >> ["foo", "bar"]
        1 * env.getDefaultProfiles() >> ["default"]
        exitCode == ExitCode.SUCCESS
    }
}
