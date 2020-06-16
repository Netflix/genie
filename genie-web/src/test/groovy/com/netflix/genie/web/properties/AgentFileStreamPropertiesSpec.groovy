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

class AgentFileStreamPropertiesSpec extends Specification {

    def "Defaults, getters, setters"() {
        when:
        AgentFileStreamProperties props = new AgentFileStreamProperties()

        then:
        props.getMaxConcurrentTransfers() == 100
        props.getUnclaimedStreamStartTimeout() == Duration.ofSeconds(10)
        props.getStalledTransferTimeout() == Duration.ofSeconds(20)
        props.getStalledTransferCheckInterval() == Duration.ofSeconds(5)
        props.getWriteRetryDelay() == Duration.ofMillis(300)
        props.getManifestCacheExpiration() == Duration.ofSeconds(30)

        when:
        props.setMaxConcurrentTransfers(3)
        props.setUnclaimedStreamStartTimeout(Duration.ofSeconds(20))
        props.setStalledTransferTimeout(Duration.ofSeconds(40))
        props.setStalledTransferCheckInterval(Duration.ofSeconds(10))
        props.setWriteRetryDelay(Duration.ofMillis(600))
        props.setManifestCacheExpiration(Duration.ofSeconds(60))

        then:
        props.getMaxConcurrentTransfers() == 3
        props.getUnclaimedStreamStartTimeout() == Duration.ofSeconds(20)
        props.getStalledTransferTimeout() == Duration.ofSeconds(40)
        props.getStalledTransferCheckInterval() == Duration.ofSeconds(10)
        props.getWriteRetryDelay() == Duration.ofMillis(600)
        props.getManifestCacheExpiration() == Duration.ofSeconds(60)
    }
}
