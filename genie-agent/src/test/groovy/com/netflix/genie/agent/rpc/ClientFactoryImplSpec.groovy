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

import com.netflix.genie.test.categories.UnitTest
import io.grpc.ManagedChannel
import org.junit.experimental.categories.Category
import org.spockframework.util.Assert
import spock.lang.Specification


@Category(UnitTest.class)
class ClientFactoryImplSpec extends Specification {
    ChannelFactory channelFactory
    ManagedChannel channel

    void setup() {
        channelFactory = Mock()
        channel = Mock()
    }

    void cleanup() {
    }

    def "GetPingClient"() {
        setup:
        def factory = new ClientFactoryImpl(channelFactory)

        when:
        def pingClient1 = factory.getBlockingPingClient("host1", 1000)
        def pingClient2 = factory.getBlockingPingClient("host1", 1000)

        then:
        2 * channelFactory.getSharedManagedChannel("host1", 1000) >> channel

        expect:
        Assert.notNull(pingClient1)
        Assert.notNull(pingClient2)
        pingClient1 != pingClient2
    }
}
