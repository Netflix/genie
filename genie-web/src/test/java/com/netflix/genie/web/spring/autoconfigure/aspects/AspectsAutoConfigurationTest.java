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
package com.netflix.genie.web.spring.autoconfigure.aspects;

import com.netflix.genie.web.aspects.DataServiceRetryAspect;
import com.netflix.genie.web.aspects.HealthCheckMetricsAspect;
import com.netflix.genie.web.aspects.SystemArchitecture;
import com.netflix.genie.web.properties.DataServiceRetryProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.aop.aspectj.autoproxy.AspectJAwareAdvisorAutoProxyCreator;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Tests for {@link AspectsAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class AspectsAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    AspectsAutoConfiguration.class
                )
            )
            .withUserConfiguration(UserConfig.class);

    /**
     * Make sure all the expected beans are created by the auto configuration.
     */
    @Test
    public void expectedBeansCreated() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(DataServiceRetryProperties.class);
                Assertions.assertThat(context).hasSingleBean(AspectJAwareAdvisorAutoProxyCreator.class);
                Assertions.assertThat(context).hasSingleBean(DataServiceRetryAspect.class);
                Assertions.assertThat(context).hasSingleBean(HealthCheckMetricsAspect.class);
                Assertions.assertThat(context).hasSingleBean(SystemArchitecture.class);
            }
        );
    }

    /**
     * Dummy user configuration for tests.
     */
    @Configuration
    protected static class UserConfig {

        /**
         * Dummy meter registry.
         *
         * @return {@link SimpleMeterRegistry} instance
         */
        @Bean
        public MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }
}
