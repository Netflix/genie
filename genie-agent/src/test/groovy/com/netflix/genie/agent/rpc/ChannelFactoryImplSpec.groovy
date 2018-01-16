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

package com.netflix.genie.agent.rpc

import spock.lang.Specification

class ChannelFactoryImplSpec extends Specification {
    void setup() {
    }

    void cleanup() {
    }

    def "GetSharedManagedChannel"() {
        setup:
        def factory = new ChannelFactoryImpl()

        when:
        def channel1 = factory.getSharedManagedChannel("host1", 1000)
        def channel2 = factory.getSharedManagedChannel("host2", 1000)
        def channel3 = factory.getSharedManagedChannel("host1", 1001)
        def channel4 = factory.getSharedManagedChannel("host1", 1000)

        then:
        3 == factory.getSharedChannelsCount()
        channel1 == channel4
        channel1 != channel2
        channel1 != channel3
        channel2 != channel3

        when:
        factory.cleanUp()

        then:
        0 == factory.getSharedChannelsCount()
    }

}
