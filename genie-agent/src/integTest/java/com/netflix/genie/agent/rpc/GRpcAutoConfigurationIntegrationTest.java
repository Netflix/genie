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
import com.netflix.genie.proto.HeartBeatServiceGrpc;
import com.netflix.genie.proto.JobKillServiceGrpc;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.PingServiceGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractStub;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test for {@link GRpcServicesAutoConfiguration}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    classes = {
        GRpcAutoConfiguration.class,
        GRpcAutoConfigurationIntegrationTest.MockConfig.class
    }
)
// TODO: Perhaps this should be using the context runner?
// TODO: Use mockbean instead of the whole configuration?
public class GRpcAutoConfigurationIntegrationTest {

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Check that channel is a singleton.
     */
    @Test
    public void channelBean() {
        final ManagedChannel channel1 = applicationContext.getBean(ManagedChannel.class);
        final ManagedChannel channel2 = applicationContext.getBean(ManagedChannel.class);
        Assert.assertNotNull(channel1);
        Assert.assertNotNull(channel2);
        Assert.assertSame(channel1, channel2);
    }

    /**
     * Check that all gRPC client stubs are available in context, that they are prototype and not singletons and that
     * they all share a single channel singleton.
     */
    @Test
    public void clientsBeans() {

        Assert.assertNotNull(applicationContext);

        final Class<?>[] clientStubClasses = {
            PingServiceGrpc.PingServiceFutureStub.class,
            JobServiceGrpc.JobServiceFutureStub.class,
            HeartBeatServiceGrpc.HeartBeatServiceStub.class,
            JobKillServiceGrpc.JobKillServiceFutureStub.class,
        };

        for (final Class<?> clientStubClass : clientStubClasses) {
            final AbstractStub stub1 = (AbstractStub) applicationContext.getBean(clientStubClass);
            final AbstractStub stub2 = (AbstractStub) applicationContext.getBean(clientStubClass);
            Assert.assertNotNull(stub1);
            Assert.assertNotNull(stub2);
            Assert.assertNotSame(stub1, stub2);
            Assert.assertSame(stub1.getChannel(), stub2.getChannel());
        }
    }

    /**
     * Check that interceptor beans are resolved.
     */
    @Test
    public void interceptorBeans() {

        final Class<?>[] interceptorClasses = {
            ChannelLoggingInterceptor.class,
        };

        for (final Class<?> interceptorClass : interceptorClasses) {
            final ClientInterceptor interceptor = (ClientInterceptor) applicationContext.getBean(interceptorClass);
            Assert.assertNotNull(interceptor);
        }
    }

    @Configuration
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
