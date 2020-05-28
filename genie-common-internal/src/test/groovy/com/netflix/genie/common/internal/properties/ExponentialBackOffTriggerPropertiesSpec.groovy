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
package com.netflix.genie.common.internal.properties

import com.netflix.genie.common.internal.util.ExponentialBackOffTrigger
import spock.lang.Specification

import java.time.Duration

class ExponentialBackOffTriggerPropertiesSpec extends Specification {
    ExponentialBackOffTriggerProperties props

    void setup() {
        this.props = new ExponentialBackOffTriggerProperties()
    }

    def "defaults, setters, getters"() {
        expect:
        props.getDelayType() == ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_COMPLETION
        props.getMinDelay() == Duration.ofMillis(100)
        props.getMaxDelay() == Duration.ofSeconds(3)
        props.getFactor() == 1.2f

        when:
        props.setDelayType(ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_SCHEDULING)
        props.setMinDelay(Duration.ofMinutes(1))
        props.setMaxDelay(Duration.ofMinutes(5))
        props.setFactor(1.5f)

        then:

        props.getDelayType() == ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_SCHEDULING
        props.getMinDelay() == Duration.ofMinutes(1)
        props.getMaxDelay() == Duration.ofMinutes(5)
        props.getFactor() == 1.5f
    }
}
