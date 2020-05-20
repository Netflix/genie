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

import java.time.Duration
import java.time.temporal.ChronoUnit

class AgentCleanupPropertiesSpec extends Specification {
    def "testDefaultsSettersAndGetters"() {
        setup:
        AgentCleanupProperties properties = new AgentCleanupProperties()

        expect:
        properties.getRefreshInterval() == Duration.of(10, ChronoUnit.SECONDS)
        properties.getReconnectTimeLimit() == Duration.of(2, ChronoUnit.MINUTES)
        properties.getLaunchTimeLimit() == Duration.of(4, ChronoUnit.MINUTES)
        properties.isEnabled()

        when:
        properties.setRefreshInterval(Duration.of(1, ChronoUnit.MINUTES))
        properties.setReconnectTimeLimit(Duration.of(2, ChronoUnit.MINUTES))
        properties.setLaunchTimeLimit(Duration.of(3, ChronoUnit.MINUTES))
        properties.setEnabled(false)

        then:
        properties.getRefreshInterval() == Duration.of(1, ChronoUnit.MINUTES)
        properties.getReconnectTimeLimit() == Duration.of(2, ChronoUnit.MINUTES)
        properties.getLaunchTimeLimit() == Duration.of(3, ChronoUnit.MINUTES)
        !properties.isEnabled()
    }
}
