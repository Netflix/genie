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
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.PingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;

/**
 * Configuration for gRPC components.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Configuration
@Slf4j
class GrpcConfig {

    @Bean
    @Lazy
    ManagedChannel channel(
        final ArgumentDelegates.ServerArguments serverArguments
    ) {
        return ManagedChannelBuilder.forAddress(serverArguments.getServerHost(), serverArguments.getServerPort())
            .usePlaintext()
            .build();
    }

    @Bean
    @Scope("prototype")
    PingServiceGrpc.PingServiceFutureStub pingServiceClient(
        final ManagedChannel channel
    ) {
        return PingServiceGrpc.newFutureStub(channel);
    }

    @Bean
    @Scope("prototype")
    JobServiceGrpc.JobServiceFutureStub jobClient(final ManagedChannel channel) {
        return JobServiceGrpc.newFutureStub(channel);
    }
}
