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
package com.netflix.genie.security.configs.processors;

import com.netflix.genie.common.internal.util.PropertySourceUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

/**
 * Apply default properties to the Spring application context before application auto configuration is evaluated.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class GenieSecurityDefaultPropertiesPostProcessor implements EnvironmentPostProcessor {

    private static final String DEFAULT_PROPERTIES_SOURCE_NAME = "genie-security-defaults";
    private static final String DEFAULT_PROPERTIES_FILE = DEFAULT_PROPERTIES_SOURCE_NAME + ".yml";

    /**
     * {@inheritDoc}
     */
    @Override
    public void postProcessEnvironment(final ConfigurableEnvironment environment, final SpringApplication application) {
        final Resource defaultProperties = new ClassPathResource(DEFAULT_PROPERTIES_FILE);
        final PropertySource<?> defaultSource
            = PropertySourceUtils.loadYamlPropertySource(DEFAULT_PROPERTIES_SOURCE_NAME, defaultProperties);
        environment.getPropertySources().addLast(defaultSource);
    }
}
