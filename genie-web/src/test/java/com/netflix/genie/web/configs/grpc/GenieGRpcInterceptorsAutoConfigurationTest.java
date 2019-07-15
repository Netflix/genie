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

import com.netflix.genie.web.rpc.grpc.interceptors.SimpleLoggingInterceptor;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Tests for {@link GenieGRpcInterceptorsAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class GenieGRpcInterceptorsAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    GenieGRpcInterceptorsAutoConfiguration.class
                )
            );

    /**
     * Default beans created.
     */
    @Test
    public void expectedBeansExistIfGrpcEnabledAndNoUserBeans() {
        this.contextRunner
            .run(context -> Assertions.assertThat(context).hasSingleBean(SimpleLoggingInterceptor.class));
    }

    /**
     * User beans override defaults.
     */
    @Test
    public void expectedBeansExistWhenUserOverrides() {
        this.contextRunner
            .withUserConfiguration(UserConfig.class)
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(SimpleLoggingInterceptor.class);
                    Assertions.assertThat(context.containsBean("userSimpleLoggingInterceptor")).isTrue();
                }
            );
    }

    /**
     * Dummy user configuration.
     */
    @Configuration
    static class UserConfig {

        @Bean
        public SimpleLoggingInterceptor userSimpleLoggingInterceptor() {
            return Mockito.mock(SimpleLoggingInterceptor.class);
        }
    }
}
