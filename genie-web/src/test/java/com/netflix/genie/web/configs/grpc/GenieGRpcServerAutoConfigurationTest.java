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
package com.netflix.genie.web.configs.grpc;

import com.netflix.genie.web.properties.GRpcServerProperties;
import com.netflix.genie.web.rpc.grpc.servers.GRpcServerManager;
import io.grpc.Server;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Tests for {@link GenieGRpcServerAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class GenieGRpcServerAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    GenieGRpcServerAutoConfiguration.class
                )
            );

    /**
     * No beans should be created.
     */
    @Test
    public void configurationNotAppliedIfNotEnabled() {
        this.contextRunner
            .withPropertyValues("genie.grpc.server.enabled=false")
            .run(
                context -> {
                    Assertions.assertThat(context).doesNotHaveBean(GRpcServerProperties.class);
                    Assertions.assertThat(context).doesNotHaveBean(Server.class);
                    Assertions.assertThat(context).doesNotHaveBean(GRpcServerManager.class);
                }
            );
    }

    /**
     * Default beans should be created.
     */
    @Test
    public void expectedBeansExistIfGrpcEnabledAndNoUserBeans() {
        this.contextRunner
            .withPropertyValues("genie.grpc.server.enabled=true")
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
    public void expectedBeansExistWhenUserOverrides() {
        this.contextRunner
            .withPropertyValues("genie.grpc.server.enabled=true")
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

    /**
     * Dummy user configuration.
     */
    @Configuration
    static class UserConfig {

        /**
         * Mocked Server.
         *
         * @return mock gRPC server
         */
        @Bean
        public Server userServer() {
            return Mockito.mock(Server.class);
        }

        /**
         * Mocked manager.
         *
         * @return user manager
         */
        @Bean
        public GRpcServerManager userServerManager() {
            return Mockito.mock(GRpcServerManager.class);
        }
    }
}
