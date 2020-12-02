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

import com.google.common.collect.Maps
import com.netflix.genie.common.internal.util.PropertiesMapCache
import com.netflix.genie.web.agent.services.AgentConfigurationService
import com.netflix.genie.web.properties.AgentConfigurationProperties
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.MutablePropertySources
import spock.lang.Specification

class AgentConfigurationServiceImplSpec extends Specification {
    AgentConfigurationProperties props
    ConfigurableEnvironment env
    MutablePropertySources propertySources
    MeterRegistry registry
    Timer timer
    PropertiesMapCache cache

    void setup() {
        this.props = new AgentConfigurationProperties()
        this.env = Mock(ConfigurableEnvironment)
        this.registry = Mock(MeterRegistry)
        this.timer = Mock(Timer)
        this.propertySources = new MutablePropertySources()
        this.cache = Mock(PropertiesMapCache)
    }

    def "Produce properties"() {
        AgentConfigurationService service = new AgentConfigurationServiceImpl(props, cache, registry)
        Map<String, String> expectedProperties = Maps.newHashMap()
        expectedProperties.put("foo", "bar")

        when:
        Map<String, String> agentProperties = service.getAgentProperties()

        then:
        1 * cache.get() >> expectedProperties
        1 * registry.timer(AgentConfigurationServiceImpl.RELOAD_PROPERTIES_TIMER, _) >> timer
        1 * timer.record(_, _)
        agentProperties == expectedProperties
    }

    def "Handle runtime error"() {
        AgentConfigurationService service = new AgentConfigurationServiceImpl(props, cache, registry)

        when:
        service.getAgentProperties()

        then:
        1 * cache.get() >> { throw new RuntimeException("...") }
        1 * registry.timer(AgentConfigurationServiceImpl.RELOAD_PROPERTIES_TIMER, _) >> timer
        1 * timer.record(_, _)
        thrown(RuntimeException)
    }
}
