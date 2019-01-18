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

package com.netflix.genie.web.properties

import org.springframework.core.env.Environment
import spock.lang.Specification

class AgentFilterPropertiesSpec extends Specification {
    AgentFilterProperties agentFilterProperties

    void setup() {
        this.agentFilterProperties = new AgentFilterProperties()
    }

    def "Return null if environment is not set"() {
        String value

        when:
        value = agentFilterProperties.getMinimumVersion()

        then:
        value == null

        when:
        value = agentFilterProperties.getBlacklistedVersions()

        then:
        value == null

        when:
        value = agentFilterProperties.getWhitelistedVersions()

        then:
        value == null
    }

    def "Return environment value if environment is set"() {
        String environmentValue = "some-value"
        String value

        Environment environment = Mock(Environment)
        agentFilterProperties.setEnvironment(environment)

        when:
        value = agentFilterProperties.getMinimumVersion()

        then:
        1 * environment.getProperty(AgentFilterProperties.MINIMUM_VERSION_PROPERTY) >> environmentValue
        value == environmentValue

        when:
        value = agentFilterProperties.getBlacklistedVersions()

        then:
        1 * environment.getProperty(AgentFilterProperties.BLACKLISTED_VERSION_REGEX_PROPERTY) >> environmentValue
        value == environmentValue

        when:
        value = agentFilterProperties.getWhitelistedVersions()

        then:
        1 * environment.getProperty(AgentFilterProperties.WHITELISTED_VERSION_REGEX_PROPERTY) >> environmentValue
        value == environmentValue
    }
}
