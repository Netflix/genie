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
package com.netflix.genie.web.spring.autoconfigure.properties.converters;

import com.netflix.genie.web.properties.converters.URIPropertyConverter;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link PropertyConvertersAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class PropertyConvertersAutoConfigurationTest {
    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    PropertyConvertersAutoConfiguration.class
                )
            );

    /**
     * Make sure the expected converters are created.
     */
    @Test
    public void canCreateExpectedConverters() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(URIPropertyConverter.class);
            }
        );
    }
}
