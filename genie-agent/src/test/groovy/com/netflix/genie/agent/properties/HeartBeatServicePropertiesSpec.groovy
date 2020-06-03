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
package com.netflix.genie.agent.properties

import spock.lang.Specification

import java.time.Duration

class HeartBeatServicePropertiesSpec extends Specification {

    def "Defaults, setters, getters"() {
        setup:
        HeartBeatServiceProperties props = new HeartBeatServiceProperties()

        expect:
        props.getInterval() == Duration.ofSeconds(2)
        props.getErrorRetryDelay() == Duration.ofSeconds(1)

        when:
        props.setInterval(Duration.ofSeconds(1))
        props.setErrorRetryDelay(Duration.ofSeconds(5))

        then:
        props.getInterval() == Duration.ofSeconds(1)
        props.getErrorRetryDelay() == Duration.ofSeconds(5)
    }
}
