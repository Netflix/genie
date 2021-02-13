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
package com.netflix.genie.common.internal.util

import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.MutablePropertySources
import spock.lang.Specification

import java.time.Duration

class PropertiesMapCacheSpec extends Specification {
    ConfigurableEnvironment env
    MutablePropertySources propertySources

    void setup() {
        this.env = Mock(ConfigurableEnvironment)
        this.propertySources = new MutablePropertySources()
    }

    def "Build map and cache it"() {

        def map1 = [
            'agent.foo'  : "...",
            'agent.bar'  : "...",
            'server.foo' : "...",
            'server.bar' : "...",
            'agent.null' : "...",
            'agent.empty': "...",
        ]
        def map2 = [
            'agent.foo'      : "...",
            'genie.agent.foo': "...",
            'genie.agent.bar': "...",
        ]

        def expectedProperties = [
            'foo': "Foo",
            'bar': "Bar",
        ]

        def propertySources = new MutablePropertySources()
        propertySources.addFirst(new MapPropertySource("map1", map1))
        propertySources.addFirst(new MapPropertySource("map2", map2))

        when:
        PropertiesMapCache cache = new PropertiesMapCache.Factory(env).get(Duration.ofMinutes(5), "agent.")
        Map<String, String> agentProperties

        then:
        0 * env.getPropertySources()

        when:
        agentProperties = cache.get()

        then:
        1 * env.getPropertySources() >> propertySources
        1 * env.getProperty("agent.foo") >> "Foo"
        1 * env.getProperty("agent.bar") >> "Bar"
        1 * env.getProperty("agent.null") >> null
        1 * env.getProperty("agent.empty") >> " "
        agentProperties == expectedProperties

        when:
        agentProperties = cache.get()

        then:
        0 * env.getPropertySources()
        agentProperties == expectedProperties
    }
}
