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

package com.netflix.genie.agent.rpc;

import com.netflix.genie.proto.PingServiceGrpc;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Component
@Lazy
class ClientFactoryImpl implements ClientFactory {

    private final ChannelFactory channelFactory;

    ClientFactoryImpl(final ChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PingServiceGrpc.PingServiceBlockingStub getBlockingPingClient(
        final String serverHost,
        final int serverPort
    ) {
        return PingServiceGrpc.newBlockingStub(
            channelFactory.getSharedManagedChannel(serverHost, serverPort)
        );
    }
}
