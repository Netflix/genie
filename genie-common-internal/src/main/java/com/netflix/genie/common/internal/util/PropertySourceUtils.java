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
package com.netflix.genie.common.internal.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySources;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Utilities for working with Spring {@link PropertySource}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public final class PropertySourceUtils {

    private static final YamlPropertySourceLoader YAML_PROPERTY_SOURCE_LOADER = new YamlPropertySourceLoader();

    private PropertySourceUtils() {
    }

    /**
     * Load a YAML {@link PropertySource} from the given {@code resource}.
     *
     * @param propertySourceName The name of the this property source
     * @param resource           The resource. Must exist and be YAML.
     * @return A {@link PropertySource} representing this set of properties
     */
    public static PropertySource<?> loadYamlPropertySource(final String propertySourceName, final Resource resource) {
        if (!resource.exists()) {
            throw new IllegalArgumentException("Resource " + resource + " does not exist");
        }
        try {
            return YAML_PROPERTY_SOURCE_LOADER.load(propertySourceName, resource).get(0);
        } catch (final IOException ex) {
            throw new IllegalStateException("Failed to load yaml configuration from " + resource, ex);
        }
    }


    /**
     * Takes a snapshot of the properties that match a given prefix.
     * The prefix is stripped from the property name.
     * Properties with empty and null values are not included.
     *
     * @param environment the environment
     * @param prefix      the prefix to match
     * @return an immutable map of property name and property value in string form
     */
    public static Map<String, String> createPropertiesSnapshot(
        final Environment environment,
        final String prefix
    ) {
        // Obtain properties sources from environment
        final PropertySources propertySources;
        if (environment instanceof ConfigurableEnvironment) {
            propertySources = ((ConfigurableEnvironment) environment).getPropertySources();
        } else {
            propertySources = new StandardEnvironment().getPropertySources();
        }

        // Create set of all properties that match the filter pattern
        final Set<String> filteredPropertyNames = Sets.newHashSet();
        for (final PropertySource<?> propertySource : propertySources) {
            if (propertySource instanceof EnumerablePropertySource) {
                for (final String propertyName : ((EnumerablePropertySource<?>) propertySource).getPropertyNames()) {
                    if (propertyName.startsWith(prefix)) {
                        log.debug("Adding matching property: {}", propertyName);
                        filteredPropertyNames.add(propertyName);
                    } else {
                        log.debug("Ignoring property: {}", propertyName);
                    }
                }
            }
        }

        // Create immutable properties map
        final ImmutableMap.Builder<String, String> propertiesMapBuilder = ImmutableMap.builder();
        for (final String propertyName : filteredPropertyNames) {
            final String propertyValue = environment.getProperty(propertyName);
            final String strippedPropertyName = StringUtils.removeStart(propertyName, prefix);
            if (StringUtils.isBlank(strippedPropertyName) || StringUtils.isBlank(propertyValue)) {
                log.debug("Skipping blank value property: {}", propertyName);
            } else {
                propertiesMapBuilder.put(strippedPropertyName, propertyValue);
            }
        }

        return propertiesMapBuilder.build();
    }
}
