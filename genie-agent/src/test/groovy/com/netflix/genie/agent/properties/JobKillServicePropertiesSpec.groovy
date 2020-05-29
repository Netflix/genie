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

import com.netflix.genie.common.internal.properties.ExponentialBackOffTriggerProperties
import com.netflix.genie.common.internal.util.ExponentialBackOffTrigger
import spock.lang.Specification

import java.time.Duration

class JobKillServicePropertiesSpec extends Specification {

    def "Defaults, setters, getters"() {
        setup:
        JobKillServiceProperties props = new JobKillServiceProperties()

        expect:
        props.getResponseCheckBackOff().getDelayType() == ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_COMPLETION
        props.getResponseCheckBackOff().getMinDelay() == Duration.ofMillis(500)
        props.getResponseCheckBackOff().getMaxDelay() == Duration.ofSeconds(5)
        props.getResponseCheckBackOff().getFactor() == 1.2f

        when:
        def newResponseCheckBackOff = new ExponentialBackOffTriggerProperties(
            ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_BEGIN,
            Duration.ofSeconds(3),
            Duration.ofSeconds(5),
            1.5f
        )
        props.setResponseCheckBackOff(newResponseCheckBackOff)

        then:
        props.getResponseCheckBackOff() == newResponseCheckBackOff
    }
}
