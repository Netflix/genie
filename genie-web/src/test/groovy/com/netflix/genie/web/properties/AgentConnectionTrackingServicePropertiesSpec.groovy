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
package com.netflix.genie.web.properties

import spock.lang.Specification

import java.time.Duration

class AgentConnectionTrackingServicePropertiesSpec extends Specification {

    def "Defaults, getters, setters"() {
        when:
        AgentConnectionTrackingServiceProperties props = new AgentConnectionTrackingServiceProperties()

        then:
        props.getCleanupInterval() == Duration.ofSeconds(2)
        props.getConnectionExpirationPeriod() == Duration.ofSeconds(10)

        when:
        props.setCleanupInterval(Duration.ofSeconds(4))
        props.setConnectionExpirationPeriod(Duration.ofSeconds(20))

        then:
        props.getCleanupInterval() == Duration.ofSeconds(4)
        props.getConnectionExpirationPeriod() == Duration.ofSeconds(20)
    }
}
