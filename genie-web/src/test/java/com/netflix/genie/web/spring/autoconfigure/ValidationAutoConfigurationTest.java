/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.spring.autoconfigure;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;

import javax.validation.Validator;

/**
 * Tests for the bean validation configuration.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class ValidationAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    ValidationAutoConfiguration.class
                )
            );

    /**
     * The auto configuration creates the expected beans.
     */
    @Test
    public void expectedBeansExist() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(MethodValidationPostProcessor.class);
                Assertions.assertThat(context).hasSingleBean(Validator.class);
                Assertions.assertThat(context).hasSingleBean(LocalValidatorFactoryBean.class);
            }
        );
    }
}
