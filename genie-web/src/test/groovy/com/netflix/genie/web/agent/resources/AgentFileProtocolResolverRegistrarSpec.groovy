/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.agent.resources

import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext
import org.springframework.context.ConfigurableApplicationContext
import spock.lang.Specification

class AgentFileProtocolResolverRegistrarSpec extends Specification {
    AgentFileProtocolResolverRegistrar registrar
    AgentFileProtocolResolver resolver
    ConfigurableApplicationContext applicationContext

    void setup() {
        this.resolver = Mock(AgentFileProtocolResolver)
        this.applicationContext = Mock(ConfigurableApplicationContext)
        this.registrar = new AgentFileProtocolResolverRegistrar(resolver)
    }

    def "Register"() {
        when:
        this.registrar.setApplicationContext(applicationContext)

        then:
        1 * applicationContext.addProtocolResolver(resolver)
    }

    def "Register error"() {
        when:
        this.registrar.setApplicationContext(Mock(Object) as ApplicationContext)

        then:
        0 * applicationContext.addProtocolResolver(resolver)
        thrown(BeansException)
    }
}
