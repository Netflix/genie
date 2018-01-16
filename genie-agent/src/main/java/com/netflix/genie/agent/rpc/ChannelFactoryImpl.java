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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.validation.constraints.Min;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
@Lazy
@Slf4j
class ChannelFactoryImpl implements ChannelFactory {

    private final Map<String, ManagedChannel> channelMap = Maps.newHashMap();

    /**
     * {@inheritDoc}
     */
    @Override
    public Channel getSharedManagedChannel(
        @NotEmpty final String serverHost,
        @Min(1) final int serverPort
    ) {
        final String channelKey = getChannelKey(serverHost, serverPort);
        synchronized (channelMap) {
            if (!channelMap.containsKey(channelKey)) {
                final ManagedChannel channel = createChannel(serverHost, serverPort);
                channelMap.put(channelKey, channel);
            }
            return channelMap.get(channelKey);
        }
    }

    @VisibleForTesting
    int getSharedChannelsCount() {
        synchronized (channelMap) {
            return channelMap.size();
        }
    }

    private ManagedChannel createChannel(
        final String serverHost,
        final int serverPort
    ) {
        return ManagedChannelBuilder.forAddress(serverHost, serverPort)
            .usePlaintext(true)
            .build();
    }

    private String getChannelKey(
        final String serverHost,
        final int serverPort
    ) {
        return serverHost + ":" + serverPort;
    }

    /**
     * Shut down all channels.
     */
    @PreDestroy
    public void cleanUp() {
        synchronized (channelMap) {
            for (Map.Entry<String, ManagedChannel> channelEntry : channelMap.entrySet()) {
                log.info("Shutting down managed channel for server: {}", channelEntry.getKey());
                channelEntry.getValue().shutdownNow();
            }
            for (Map.Entry<String, ManagedChannel> channelEntry : channelMap.entrySet()) {
                try {
                    channelEntry.getValue().awaitTermination(1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Timed out waiting for channel shutdown: {}", channelEntry.getKey(), e);
                }
            }
            channelMap.clear();
        }
    }
}
