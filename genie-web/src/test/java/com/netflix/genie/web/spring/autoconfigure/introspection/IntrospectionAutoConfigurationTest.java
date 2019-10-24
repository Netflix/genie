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
package com.netflix.genie.web.spring.autoconfigure.introspection;

import com.netflix.genie.web.introspection.GenieWebHostInfo;
import com.netflix.genie.web.introspection.GenieWebRpcInfo;
import io.grpc.Server;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

import java.net.InetAddress;

/**
 * Tests for {@link IntrospectionAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class IntrospectionAutoConfigurationTest {

    private static final int EXPECTED_SERVER_PORT = 2482;

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    IntrospectionAutoConfiguration.class
                )
            );

    /**
     * Make sure when the gRPC server starts as expected the bean is created.
     */
    @Test
    void expectedBeansCreated() {
        this.contextRunner
            .withUserConfiguration(ServerStartedConfiguration.class)
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(GenieWebHostInfo.class);
                    final GenieWebHostInfo hostInfo = context.getBean(GenieWebHostInfo.class);
                    Assertions
                        .assertThat(hostInfo.getHostname())
                        .isEqualTo(InetAddress.getLocalHost().getCanonicalHostName());
                    Assertions.assertThat(context).hasSingleBean(GenieWebRpcInfo.class);
                    final GenieWebRpcInfo rpcInfo = context.getBean(GenieWebRpcInfo.class);
                    Assertions.assertThat(rpcInfo.getRpcPort()).isEqualTo(EXPECTED_SERVER_PORT);
                }
            );
    }

    /**
     * Make sure when the gRPC server doesn't start within the retry context the expected exception is thrown.
     */
    @Test
    void expectedExceptionThrownWhenServerIsNotStarted() {
        this.contextRunner
            .withUserConfiguration(ServerNeverStartsConfiguration.class)
            .run(
                context -> Assertions
                    .assertThat(context)
                    .getFailure()
                    .hasRootCauseExactlyInstanceOf(IllegalStateException.class)
            );
    }

    /**
     * Make sure when the gRPC server is already terminated we get expected exception.
     */
    @Test
    void expectedExceptionThrownWhenServerAlreadyTerminated() {
        this.contextRunner
            .withUserConfiguration(ServerAlreadyTerminatedConfiguration.class)
            .run(
                context -> Assertions
                    .assertThat(context)
                    .getFailure()
                    .hasRootCauseExactlyInstanceOf(IllegalStateException.class)
            );
    }

    /**
     * Make sure when the gRPC server is already shutdown we get expected exception.
     */
    @Test
    void expectedExceptionThrownWhenServerAlreadyShutdown() {
        this.contextRunner
            .withUserConfiguration(ServerAlreadyShutdownConfiguration.class)
            .run(
                context -> Assertions
                    .assertThat(context)
                    .getFailure()
                    .hasRootCauseExactlyInstanceOf(IllegalStateException.class)
            );
    }

    static class ServerStartedConfiguration {
        @Bean
        Server mockGRpcServer() {
            final Server server = Mockito.mock(Server.class);
            Mockito.when(server.isTerminated()).thenReturn(false);
            Mockito.when(server.isShutdown()).thenReturn(false);
            Mockito.when(server.getPort()).thenReturn(EXPECTED_SERVER_PORT);
            return server;
        }
    }

    static class ServerNeverStartsConfiguration {
        @Bean
        Server mockGRpcServer() {
            final Server server = Mockito.mock(Server.class);
            Mockito.when(server.isTerminated()).thenReturn(false);
            Mockito.when(server.isShutdown()).thenReturn(false);
            Mockito.when(server.getPort()).thenReturn(-1);
            return server;
        }
    }

    static class ServerAlreadyTerminatedConfiguration {
        @Bean
        Server mockGRpcServer() {
            final Server server = Mockito.mock(Server.class);
            Mockito.when(server.isTerminated()).thenReturn(true);
            Mockito.when(server.isShutdown()).thenReturn(false);
            return server;
        }
    }

    static class ServerAlreadyShutdownConfiguration {
        @Bean
        Server mockGRpcServer() {
            final Server server = Mockito.mock(Server.class);
            Mockito.when(server.isTerminated()).thenReturn(false);
            Mockito.when(server.isShutdown()).thenReturn(true);
            return server;
        }
    }
}
