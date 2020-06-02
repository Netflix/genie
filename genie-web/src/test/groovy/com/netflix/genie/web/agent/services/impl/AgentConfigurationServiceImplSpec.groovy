/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.agent.services.impl

import com.netflix.genie.web.agent.services.AgentConfigurationService
import com.netflix.genie.web.properties.AgentConfigurationProperties
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.Environment
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.MutablePropertySources
import spock.lang.Specification

class AgentConfigurationServiceImplSpec extends Specification {
    AgentConfigurationProperties props
    ConfigurableEnvironment env
    MutablePropertySources propertySources

    void setup() {
        this.props = new AgentConfigurationProperties()
        this.env = Mock(ConfigurableEnvironment)
        this.propertySources = new MutablePropertySources()
    }

    def "Result is cached"() {
        AgentConfigurationService service = new AgentConfigurationServiceImpl(props, env)

        then:

        when:
        Map<String, String> agentProperties = service.getAgentProperties()

        then:
        1 * env.getPropertySources() >> propertySources
        agentProperties.isEmpty()

        when:
        service.getAgentProperties()

        then:
        0 * env.getPropertySources() >> propertySources
    }

    def "Fallback to standard environment during load"() {
        Environment env = Mock(Environment)

        when:
        AgentConfigurationService service = new AgentConfigurationServiceImpl(props, env)
        Map<String, String> agentProperties = service.getAgentProperties()

        then:
        _ * env.getProperty(_ as String) >> UUID.randomUUID().toString()
        agentProperties != null
    }

    def "Iterate through property sources with custom filter and some blank values"() {
        props.setAgentPropertiesFilterPattern("^agent\\..*")

        def map1 = [
            'agent.foo': "...",
            'agent.bar': "...",
            'server.foo': "...",
            'server.bar': "...",
            'agent.null': "...",
            'agent.empty': "...",
        ]
        def map2 = [
            'agent.foo': "...",
            'genie.agent.foo': "...",
            'genie.agent.bar': "...",
        ]

        def expectedProperties = [
            'agent.foo': "Foo",
            'agent.bar': "Bar",
        ]

        def propertySources = new MutablePropertySources()
        propertySources.addFirst(new MapPropertySource("map1", map1))
        propertySources.addFirst(new MapPropertySource("map2", map2))

        when:
        AgentConfigurationService service = new AgentConfigurationServiceImpl(props, env)
        Map<String, String> agentProperties = service.getAgentProperties()

        then:
        1 * env.getPropertySources() >> propertySources
        1 * env.getProperty("agent.foo") >> "Foo"
        1 * env.getProperty("agent.bar") >> "Bar"
        1 * env.getProperty("agent.null") >> null
        1 * env.getProperty("agent.empty") >> " "
        agentProperties == expectedProperties
    }
}
