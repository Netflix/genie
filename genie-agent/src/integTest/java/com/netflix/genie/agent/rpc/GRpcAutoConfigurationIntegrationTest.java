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

import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.execution.services.impl.grpc.GRpcServicesAutoConfiguration;
import com.netflix.genie.proto.FileStreamServiceGrpc;
import com.netflix.genie.proto.HeartBeatServiceGrpc;
import com.netflix.genie.proto.JobKillServiceGrpc;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.PingServiceGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractStub;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Test for {@link GRpcServicesAutoConfiguration}.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(
    classes = {
        GRpcAutoConfiguration.class,
        GRpcAutoConfigurationIntegrationTest.MockConfig.class
    }
)
// TODO: Perhaps this should be using the context runner?
// TODO: Use mockbean instead of the whole configuration?
class GRpcAutoConfigurationIntegrationTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void channelBean() {
        final ManagedChannel channel1 = this.applicationContext.getBean(ManagedChannel.class);
        final ManagedChannel channel2 = this.applicationContext.getBean(ManagedChannel.class);
        Assertions.assertThat(channel1).isNotNull();
        Assertions.assertThat(channel2).isNotNull();
        Assertions.assertThat(channel1).isEqualTo(channel2);
    }

    @Test
    void clientsBeans() {
        final Class<?>[] clientStubClasses = {
            PingServiceGrpc.PingServiceFutureStub.class,
            JobServiceGrpc.JobServiceFutureStub.class,
            HeartBeatServiceGrpc.HeartBeatServiceStub.class,
            JobKillServiceGrpc.JobKillServiceFutureStub.class,
            FileStreamServiceGrpc.FileStreamServiceStub.class,
        };

        for (final Class<?> clientStubClass : clientStubClasses) {
            final AbstractStub<?> stub1 = (AbstractStub<?>) this.applicationContext.getBean(clientStubClass);
            final AbstractStub<?> stub2 = (AbstractStub<?>) this.applicationContext.getBean(clientStubClass);
            Assertions.assertThat(stub1).isNotNull();
            Assertions.assertThat(stub2).isNotNull();
            Assertions.assertThat(stub1).isNotEqualTo(stub2);
            Assertions.assertThat(stub1.getChannel()).isEqualTo(stub2.getChannel());
        }
    }

    @Test
    void interceptorBeans() {
        final Class<?>[] interceptorClasses = {
            ChannelLoggingInterceptor.class,
        };

        for (final Class<?> interceptorClass : interceptorClasses) {
            final ClientInterceptor interceptor = (ClientInterceptor) this.applicationContext.getBean(interceptorClass);
            Assertions.assertThat(interceptor).isNotNull();
        }
    }

    static class MockConfig {
        @Bean
        ArgumentDelegates.ServerArguments serverArguments() {
            final ArgumentDelegates.ServerArguments mock = Mockito.mock(ArgumentDelegates.ServerArguments.class);
            Mockito.when(mock.getServerHost()).thenReturn("server.com");
            Mockito.when(mock.getServerPort()).thenReturn(1234);
            Mockito.when(mock.getRpcTimeout()).thenReturn(3L);
            return mock;
        }
    }
}
