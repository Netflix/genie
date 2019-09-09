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
package com.netflix.genie.agent.spring.processors;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Tests to make sure default agent properties are loaded.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class GenieDefaultPropertiesPostProcessorTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner();
    private final GenieDefaultPropertiesPostProcessor processor = new GenieDefaultPropertiesPostProcessor();

    /**
     * Test to make sure the smoke property is in the environment after the post processor is invoked.
     */
    @Test
    public void testSmokeProperty() {
        this.contextRunner
            .run(
                context -> {
                    final SpringApplication application = Mockito.mock(SpringApplication.class);
                    final ConfigurableEnvironment environment = context.getEnvironment();
                    Assert.assertFalse(
                        environment
                            .getPropertySources()
                            .contains(GenieDefaultPropertiesPostProcessor.DEFAULT_PROPERTY_SOURCE_NAME)
                    );
                    Assert.assertFalse(
                        environment
                            .getPropertySources()
                            .contains(GenieDefaultPropertiesPostProcessor.DEFAULT_PROPERTY_SOURCE_NAME)
                    );
                    Assert.assertThat(environment.getProperty("genie.smoke"), Matchers.nullValue());
                    this.processor.postProcessEnvironment(environment, application);
                    Assert.assertTrue(
                        environment
                            .getPropertySources()
                            .contains(GenieDefaultPropertiesPostProcessor.DEFAULT_PROPERTY_SOURCE_NAME)
                    );
                    Assert.assertTrue(environment.getProperty("genie.smoke", Boolean.class, false));
                }
            );
    }
}
