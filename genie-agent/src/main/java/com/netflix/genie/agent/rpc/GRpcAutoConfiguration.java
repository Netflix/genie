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

import brave.Tracing;
import brave.grpc.GrpcTracing;
import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.proto.FileStreamServiceGrpc;
import com.netflix.genie.proto.HeartBeatServiceGrpc;
import com.netflix.genie.proto.JobKillServiceGrpc;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.PingServiceGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;

import java.util.List;

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
     * Provide a lazy {@link ManagedChannel} bean if none was already defined for communicating with the Genie server.
     *
     * @param serverArguments    The server arguments to use
     * @param clientInterceptors The list of available client interceptors that should be added to the generated channel
     * @return A {@link ManagedChannel} instance configured to use plain text over the wire
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(ManagedChannel.class)
    public ManagedChannel channel(
        final ArgumentDelegates.ServerArguments serverArguments,
        final List<ClientInterceptor> clientInterceptors
    ) {
        return ManagedChannelBuilder
            .forAddress(
                serverArguments.getServerHost(),
                serverArguments.getServerPort()
            )
            .intercept(clientInterceptors)
            .usePlaintext()
            .build();
    }

    /**
     * Provide a prototype bean definition for a {@link com.netflix.genie.proto.PingServiceGrpc.PingServiceFutureStub}.
     *
     * @param channel The managed channel to use to connect to the Genie server
     * @return A {@link com.netflix.genie.proto.PingServiceGrpc.PingServiceFutureStub} instance per use
     */
    @Bean
    @Scope("prototype")
    public PingServiceGrpc.PingServiceFutureStub pingServiceClient(final ManagedChannel channel) {
        return PingServiceGrpc.newFutureStub(channel);
    }

    /**
     * Provide a prototype bean definition for a
     * {@link com.netflix.genie.proto.JobServiceGrpc.JobServiceFutureStub}.
     *
     * @param channel The managed channel to use to connect to the Genie server
     * @return A {@link com.netflix.genie.proto.JobServiceGrpc.JobServiceFutureStub} instance per use
     */
    @Bean
    @Scope("prototype")
    public JobServiceGrpc.JobServiceFutureStub jobClient(final ManagedChannel channel) {
        return JobServiceGrpc.newFutureStub(channel);
    }

    /**
     * Provide a prototype bean definition for a
     * {@link com.netflix.genie.proto.HeartBeatServiceGrpc.HeartBeatServiceStub}.
     *
     * @param channel The managed channel to use to connect to the Genie server
     * @return A {@link com.netflix.genie.proto.HeartBeatServiceGrpc.HeartBeatServiceStub} instance per use
     */
    @Bean
    @Scope("prototype")
    public HeartBeatServiceGrpc.HeartBeatServiceStub heartBeatClient(final ManagedChannel channel) {
        return HeartBeatServiceGrpc.newStub(channel);
    }

    /**
     * Provide a prototype bean definition for a
     * {@link com.netflix.genie.proto.JobKillServiceGrpc.JobKillServiceFutureStub}.
     *
     * @param channel The managed channel to use to connect to the Genie server
     * @return A {@link com.netflix.genie.proto.JobKillServiceGrpc.JobKillServiceFutureStub} instance per use
     */
    @Bean
    @Scope("prototype")
    public JobKillServiceGrpc.JobKillServiceFutureStub jobKillClient(final ManagedChannel channel) {
        return JobKillServiceGrpc.newFutureStub(channel);
    }

    /**
     * Provide a prototype bean definition for a {@link FileStreamServiceGrpc.FileStreamServiceStub}.
     *
     * @param channel The managed channel to use to connect to the Genie server
     * @return A {@link FileStreamServiceGrpc.FileStreamServiceStub} instance per use
     */
    @Bean
    @Scope("prototype")
    public FileStreamServiceGrpc.FileStreamServiceStub fileStreamClient(final ManagedChannel channel) {
        return FileStreamServiceGrpc.newStub(channel);
    }

    /**
     * Provide a {@link ClientInterceptor} which adds tracing information to gRPC calls.
     *
     * @param tracing The Brave {@link Tracing} instance
     * @return A {@link ClientInterceptor} for trace propagation
     */
    @Bean
    @ConditionalOnMissingBean(name = "genieGrpcTracingClientInterceptor")
    @Lazy
    public ClientInterceptor genieGrpcTracingClientInterceptor(final Tracing tracing) {
        return GrpcTracing.create(tracing).newClientInterceptor();
    }
}
