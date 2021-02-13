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
import java.time.temporal.ChronoUnit

class ArchiveStatusCleanupPropertiesSpec extends Specification {
    def "testDefaultsSettersAndGetters"() {
        setup:
        ArchiveStatusCleanupProperties properties = new ArchiveStatusCleanupProperties()

        expect:
        properties.getCheckInterval() == Duration.of(10, ChronoUnit.SECONDS)
        properties.getGracePeriod() == Duration.of(2, ChronoUnit.MINUTES)
        properties.isEnabled()

        when:
        properties.setCheckInterval(Duration.of(1, ChronoUnit.MINUTES))
        properties.setGracePeriod(Duration.of(2, ChronoUnit.MINUTES))
        properties.setEnabled(false)

        then:
        properties.getCheckInterval() == Duration.of(1, ChronoUnit.MINUTES)
        properties.getGracePeriod() == Duration.of(2, ChronoUnit.MINUTES)
        !properties.isEnabled()
    }
}
