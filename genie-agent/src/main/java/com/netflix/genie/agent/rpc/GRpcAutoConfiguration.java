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

import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.proto.HeartBeatServiceGrpc;
import com.netflix.genie.proto.JobKillServiceGrpc;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.PingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;

/**
 * Spring auto configuration for gRPC components.
 *
 * @author mprimi
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@Slf4j
public class GRpcAutoConfiguration {

    /**
     * Provide a lazy {@link ChannelLoggingInterceptor} bean if none was already defined.
     *
     * @return A {@link ChannelLoggingInterceptor} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(ChannelLoggingInterceptor.class)
    public ChannelLoggingInterceptor channelLoggingInterceptor() {
        return new ChannelLoggingInterceptor();
    }

    /**
     * Provide a lazy {@link ManagedChannel} bean if none was already defined for communicating with the Genie server.
     *
     * @param serverArguments The server arguments to use
     * @return A {@link ManagedChannel} instance configured to use plain text over the wire
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(ManagedChannel.class)
    public ManagedChannel channel(final ArgumentDelegates.ServerArguments serverArguments) {
        return ManagedChannelBuilder
            .forAddress(
                serverArguments.getServerHost(),
                serverArguments.getServerPort()
            )
            .usePlaintext()
            .build();
    }

    /**
     * Provide a prototype bean definition for a {@link com.netflix.genie.proto.PingServiceGrpc.PingServiceFutureStub}
     * if none has already been defined.
     *
     * @param channel The managed channel to use to connect to the Genie server
     * @return A {@link com.netflix.genie.proto.PingServiceGrpc.PingServiceFutureStub} instance per use
     */
    @Bean
    @Scope("prototype")
    @ConditionalOnMissingBean(PingServiceGrpc.PingServiceFutureStub.class)
    public PingServiceGrpc.PingServiceFutureStub pingServiceClient(final ManagedChannel channel) {
        return PingServiceGrpc.newFutureStub(channel);
    }

    /**
     * Provide a prototype bean definition for a
     * {@link com.netflix.genie.proto.JobServiceGrpc.JobServiceFutureStub} if none has already been defined.
     *
     * @param channel The managed channel to use to connect to the Genie server
     * @return A {@link com.netflix.genie.proto.JobServiceGrpc.JobServiceFutureStub} instance per use
     */
    @Bean
    @Scope("prototype")
    @ConditionalOnMissingBean(JobServiceGrpc.JobServiceFutureStub.class)
    public JobServiceGrpc.JobServiceFutureStub jobClient(final ManagedChannel channel) {
        return JobServiceGrpc.newFutureStub(channel);
    }

    /**
     * Provide a prototype bean definition for a
     * {@link com.netflix.genie.proto.HeartBeatServiceGrpc.HeartBeatServiceStub} if none has already been defined.
     *
     * @param channel The managed channel to use to connect to the Genie server
     * @return A {@link com.netflix.genie.proto.HeartBeatServiceGrpc.HeartBeatServiceStub} instance per use
     */
    @Bean
    @Scope("prototype")
    @ConditionalOnMissingBean(HeartBeatServiceGrpc.HeartBeatServiceStub.class)
    public HeartBeatServiceGrpc.HeartBeatServiceStub heartBeatClient(final ManagedChannel channel) {
        return HeartBeatServiceGrpc.newStub(channel);
    }

    /**
     * Provide a prototype bean definition for a
     * {@link com.netflix.genie.proto.JobKillServiceGrpc.JobKillServiceFutureStub} if none has already been defined.
     *
     * @param channel The managed channel to use to connect to the Genie server
     * @return A {@link com.netflix.genie.proto.JobKillServiceGrpc.JobKillServiceFutureStub} instance per use
     */
    @Bean
    @Scope("prototype")
    @ConditionalOnMissingBean(JobKillServiceGrpc.JobKillServiceFutureStub.class)
    public JobKillServiceGrpc.JobKillServiceFutureStub jobKillClient(final ManagedChannel channel) {
        return JobKillServiceGrpc.newFutureStub(channel);
    }
}
