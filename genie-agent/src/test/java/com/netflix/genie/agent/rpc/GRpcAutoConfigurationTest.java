/*
 *
 *  Copyright 2019 Netflix, Inc.
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
import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.proto.FileStreamServiceGrpc;
import com.netflix.genie.proto.HeartBeatServiceGrpc;
import com.netflix.genie.proto.JobKillServiceGrpc;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.PingServiceGrpc;
import io.grpc.ManagedChannel;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

/**
 * Test for {@link GRpcAutoConfiguration}.
 */
class GRpcAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                GRpcAutoConfiguration.class
            )
        )
        .withUserConfiguration(ExternalBeans.class);

    @Test
    void expectedBeansExist() {
        this.contextRunner.run(
            context -> Assertions
                .assertThat(context)
                .hasSingleBean(ManagedChannel.class)
                .hasSingleBean(PingServiceGrpc.PingServiceFutureStub.class)
                .hasSingleBean(JobServiceGrpc.JobServiceFutureStub.class)
                .hasSingleBean(HeartBeatServiceGrpc.HeartBeatServiceStub.class)
                .hasSingleBean(JobKillServiceGrpc.JobKillServiceFutureStub.class)
                .hasSingleBean(FileStreamServiceGrpc.FileStreamServiceStub.class)
                .hasBean("genieGrpcTracingClientInterceptor")
        );
    }

    private static class ExternalBeans {
        @Bean
        ArgumentDelegates.ServerArguments serverArguments() {
            final ArgumentDelegates.ServerArguments mock = Mockito.mock(ArgumentDelegates.ServerArguments.class);
            Mockito.when(mock.getServerHost()).thenReturn("server.com");
            Mockito.when(mock.getServerPort()).thenReturn(1234);
            Mockito.when(mock.getRpcTimeout()).thenReturn(3L);
            return mock;
        }

        @Bean
        Tracing tracing() {
            return Tracing.newBuilder().build();
        }
    }
}
