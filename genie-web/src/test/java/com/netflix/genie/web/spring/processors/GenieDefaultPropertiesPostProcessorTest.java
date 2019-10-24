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
package com.netflix.genie.web.spring.processors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;

/**
 * Tests to make sure default properties are loaded when {@link GenieDefaultPropertiesPostProcessor} is applied.
 *
 * @author tgianos
 * @since 4.0.0
 */
class GenieDefaultPropertiesPostProcessorTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner();
    private final GenieDefaultPropertiesPostProcessor processor = new GenieDefaultPropertiesPostProcessor();

    /**
     * Test to make sure the smoke property is in the environment after the post processor is invoked.
     */
    @Test
    void testSmokeProperty() {
        this.contextRunner
            .run(
                context -> {
                    final SpringApplication application = Mockito.mock(SpringApplication.class);
                    final ConfigurableEnvironment environment = context.getEnvironment();
                    Assertions.assertThat(
                        environment
                            .getPropertySources()
                            .contains(GenieDefaultPropertiesPostProcessor.DEFAULT_PROPERTY_SOURCE_NAME)
                    ).isFalse();
                    Assertions.assertThat(
                        environment
                            .getPropertySources()
                            .contains(GenieDefaultPropertiesPostProcessor.DEFAULT_PROD_PROPERTY_SOURCE_NAME)
                    ).isFalse();
                    Assertions.assertThat(environment.getProperty("genie.smoke")).isNull();
                    this.processor.postProcessEnvironment(environment, application);
                    Assertions.assertThat(
                        environment
                            .getPropertySources()
                            .contains(GenieDefaultPropertiesPostProcessor.DEFAULT_PROPERTY_SOURCE_NAME)
                    ).isTrue();
                    Assertions.assertThat(
                        environment
                            .getPropertySources()
                            .contains(GenieDefaultPropertiesPostProcessor.DEFAULT_PROD_PROPERTY_SOURCE_NAME)
                    ).isFalse();
                    Assertions
                        .assertThat(environment.getProperty("genie.smoke", Boolean.class, false))
                        .isTrue();
                }
            );
    }

    /**
     * Test to make sure the prod smoke property is in the environment after the post processor is invoked.
     */
    @Test
    void testProdSmokeProperty() {
        this.contextRunner
            .withPropertyValues("spring.profiles.active=prod")
            .run(
                context -> {
                    final SpringApplication application = Mockito.mock(SpringApplication.class);
                    final ConfigurableEnvironment environment = context.getEnvironment();
                    final MutablePropertySources propertySources = environment.getPropertySources();
                    Assertions.assertThat(
                        propertySources.contains(GenieDefaultPropertiesPostProcessor.DEFAULT_PROPERTY_SOURCE_NAME)
                    ).isFalse();
                    Assertions.assertThat(
                        propertySources.contains(GenieDefaultPropertiesPostProcessor.DEFAULT_PROD_PROPERTY_SOURCE_NAME)
                    ).isFalse();
                    Assertions.assertThat(environment.getProperty("genie.smoke")).isNull();
                    this.processor.postProcessEnvironment(environment, application);
                    Assertions.assertThat(
                        propertySources.contains(GenieDefaultPropertiesPostProcessor.DEFAULT_PROPERTY_SOURCE_NAME)
                    ).isTrue();
                    Assertions.assertThat(
                        propertySources.contains(GenieDefaultPropertiesPostProcessor.DEFAULT_PROD_PROPERTY_SOURCE_NAME)
                    ).isTrue();
                    final PropertySource<?> defaultPropertySource
                        = propertySources.get(GenieDefaultPropertiesPostProcessor.DEFAULT_PROPERTY_SOURCE_NAME);
                    final PropertySource<?> defaultProdPropertySource
                        = propertySources.get(GenieDefaultPropertiesPostProcessor.DEFAULT_PROD_PROPERTY_SOURCE_NAME);
                    Assertions.assertThat(defaultPropertySource).isNotNull();
                    Assertions.assertThat(defaultProdPropertySource).isNotNull();
                    // Lower value = higher precedence. Kind of confusing. had to look at source.
                    Assertions
                        .assertThat(propertySources.precedenceOf(defaultProdPropertySource))
                        .isLessThan(propertySources.precedenceOf(defaultPropertySource));
                    Assertions
                        .assertThat(environment.getProperty("genie.smoke", Boolean.class, true))
                        .isFalse();
                }
            );
    }
}
