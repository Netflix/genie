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

import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.Resource;

import java.io.IOException;

/**
 * Utilities for working with Spring {@link PropertySource}.
 *
 * @author tgianos
 * @since 4.0.0
 */
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
}
