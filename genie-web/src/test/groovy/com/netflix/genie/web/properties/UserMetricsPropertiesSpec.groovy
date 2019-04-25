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
package com.netflix.genie.web.properties

import spock.lang.Specification

/**
 * Specifications for the {@link UserMetricsProperties} class.
 * */
class UserMetricsPropertiesSpec extends Specification {

    def "Default parameters are as expected"() {
        when:
        def properties = new UserMetricsProperties()

        then:
        properties.isEnabled()
        properties.getRefreshInterval() == 30_000L
    }

    def "Can set new values"() {
        setup:
        def properties = new UserMetricsProperties()

        when:
        properties.setEnabled(false)
        properties.setRefreshInterval(5_000L)

        then:
        !properties.isEnabled()
        properties.getRefreshInterval() == 5_000L
    }
}
