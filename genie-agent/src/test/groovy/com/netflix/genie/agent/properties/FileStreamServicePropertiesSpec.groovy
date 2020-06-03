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
import org.springframework.util.unit.DataSize
import spock.lang.Specification

import java.time.Duration

class FileStreamServicePropertiesSpec extends Specification {

    def "Defaults, setters, getters"() {
        setup:
        FileStreamServiceProperties props = new FileStreamServiceProperties()

        expect:
        props.getErrorBackOff().getDelayType() == ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_BEGIN
        props.getErrorBackOff().getMinDelay() == Duration.ofSeconds(1)
        props.getErrorBackOff().getMaxDelay() == Duration.ofSeconds(10)
        props.getErrorBackOff().getFactor() == 1.1f
        props.isEnableCompression()
        props.getDataChunkMaxSize() == DataSize.ofMegabytes(1)
        props.getMaxConcurrentStreams() == 5
        props.getDrainTimeout() == Duration.ofSeconds(15)

        when:
        def newErrorBackOff = new ExponentialBackOffTriggerProperties(
            ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_COMPLETION,
            Duration.ofSeconds(3),
            Duration.ofSeconds(5),
            1.5f
        )
        props.setErrorBackOff(newErrorBackOff)
        props.setEnableCompression(false)
        props.setDataChunkMaxSize(DataSize.ofKilobytes(512))
        props.setMaxConcurrentStreams(10)
        props.setDrainTimeout(Duration.ofSeconds(20))

        then:
        props.getErrorBackOff() == newErrorBackOff
        !props.isEnableCompression()
        props.getDataChunkMaxSize() == DataSize.ofKilobytes(512)
        props.getMaxConcurrentStreams() == 10
        props.getDrainTimeout() == Duration.ofSeconds(20)
    }
}
