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
package com.netflix.genie.web.configs;

import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

/**
 * Tests for the bean validation configuration.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class GenieValidationAutoConfigurationUnitTests {

    private GenieValidationAutoConfiguration genieValidationAutoConfiguration;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.genieValidationAutoConfiguration = new GenieValidationAutoConfiguration();
    }

    /**
     * Make sure the validation bean is of the right type.
     */
    @Test
    public void canGetValidator() {
        Assert.assertTrue(
            this.genieValidationAutoConfiguration.localValidatorFactoryBean() instanceof LocalValidatorFactoryBean
        );
    }

    /**
     * Make sure we get a method validation post processor.
     */
    @Test
    public void canGetMethodValidator() {
        Assert.assertNotNull(this.genieValidationAutoConfiguration.methodValidationPostProcessor());
    }
}
