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

import spock.lang.Specification

class AgentCleanupPropertiesSpec extends Specification {
    def "testDefaultsSettersAndGetters"() {
        setup:
        AgentCleanupProperties properties = new AgentCleanupProperties()

        expect:
        properties.getRefreshInterval() == 10000L
        properties.getTimeLimit() == 120000L
        properties.isEnabled()

        when:
        properties.setRefreshInterval(1000)
        properties.setTimeLimit(2000)
        properties.setEnabled(false)

        then:
        properties.getRefreshInterval() == 1000L
        properties.getTimeLimit() == 2000L
        !properties.isEnabled()
    }
}
