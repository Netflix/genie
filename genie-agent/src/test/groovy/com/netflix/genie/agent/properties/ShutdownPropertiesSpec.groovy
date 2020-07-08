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

class ShutdownPropertiesSpec extends Specification {


    ShutdownProperties shutdownProperties

    void setup() {
        shutdownProperties = new ShutdownProperties()
    }

    def "defaults, getters setters"() {
        expect:
        shutdownProperties.getExecutionCompletionLeeway() == Duration.ofSeconds(60)
        shutdownProperties.getInternalExecutorsLeeway() == Duration.ofSeconds(30)
        shutdownProperties.getInternalSchedulersLeeway() == Duration.ofSeconds(30)
        shutdownProperties.getSystemExecutorLeeway() == Duration.ofSeconds(60)
        shutdownProperties.getSystemSchedulerLeeway() == Duration.ofSeconds(60)

        when:
        shutdownProperties.setExecutionCompletionLeeway(Duration.ofSeconds(0))
        shutdownProperties.setInternalExecutorsLeeway(Duration.ofSeconds(1))
        shutdownProperties.setInternalSchedulersLeeway(Duration.ofSeconds(2))
        shutdownProperties.setSystemExecutorLeeway(Duration.ofSeconds(3))
        shutdownProperties.setSystemSchedulerLeeway(Duration.ofSeconds(4))

        then:
        shutdownProperties.getExecutionCompletionLeeway() == Duration.ofSeconds(0)
        shutdownProperties.getInternalExecutorsLeeway() == Duration.ofSeconds(1)
        shutdownProperties.getInternalSchedulersLeeway() == Duration.ofSeconds(2)
        shutdownProperties.getSystemExecutorLeeway() == Duration.ofSeconds(3)
        shutdownProperties.getSystemSchedulerLeeway() == Duration.ofSeconds(4)
    }
}
