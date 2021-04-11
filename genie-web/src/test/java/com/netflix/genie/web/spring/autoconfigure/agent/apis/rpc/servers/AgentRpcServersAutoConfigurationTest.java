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
package com.netflix.genie.web.spring.autoconfigure.agent.apis.rpc.servers;

import brave.Tracing;
import com.netflix.genie.web.agent.apis.rpc.servers.GRpcServerManager;
import com.netflix.genie.web.properties.GRpcServerProperties;
import io.grpc.Server;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

/**
 * Tests for {@link AgentRpcServersAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class AgentRpcServersAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    AgentRpcServersAutoConfiguration.class
                )
            )
            .withUserConfiguration(ExternalBeans.class);

    /**
     * Default beans should be created.
     */
    @Test
    void expectedBeansExistIfGrpcEnabledAndNoUserBeans() {
        this.contextRunner
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(GRpcServerProperties.class);
                    Assertions.assertThat(context).hasSingleBean(Server.class);
                    Assertions.assertThat(context).hasSingleBean(GRpcServerManager.class);
                }
            );
    }

    /**
     * User beans override default beans.
     */
    @Test
    void expectedBeansExistWhenUserOverrides() {
        this.contextRunner
            .withUserConfiguration(UserConfig.class)
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(GRpcServerProperties.class);
                    Assertions.assertThat(context).hasSingleBean(Server.class);
                    Assertions.assertThat(context).hasSingleBean(GRpcServerManager.class);

                    Assertions.assertThat(context.containsBean("userServer")).isTrue();
                    Assertions.assertThat(context.containsBean("userServerManager")).isTrue();
                    Assertions.assertThat(context.containsBean("gRpcServer")).isFalse();
                    Assertions.assertThat(context.containsBean("gRpcServerManager")).isFalse();
                }
            );
    }

    private static class ExternalBeans {
        @Bean
        Tracing tracing() {
            return Tracing.newBuilder().build();
        }
    }

    private static class UserConfig {
        @Bean
        Server userServer() {
            return Mockito.mock(Server.class);
        }

        @Bean
        GRpcServerManager userServerManager() {
            return Mockito.mock(GRpcServerManager.class);
        }
    }
}
