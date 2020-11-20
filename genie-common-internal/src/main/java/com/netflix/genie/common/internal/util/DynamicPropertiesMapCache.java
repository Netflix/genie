/*
 *
 *  Copyright 2020 Netflix, Inc.
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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySources;
import org.springframework.core.env.StandardEnvironment;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Utility class that produces a map of properties and their values if they match a given pattern.
 * The map is cached for a fixed amount of time to avoid re-computing for each call.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class DynamicPropertiesMapCache {

    private static final String CACHE_KEY = "properties-map";
    private final Environment environment;
    private final Pattern propertiesNamesPattern;
    private final LoadingCache<String, Map<String, String>> cache;

    /**
     * Constructor.
     *
     * @param refreshInterval        the interval after which the map is re-calculated
     * @param environment            the environment
     * @param propertiesNamesPattern the pattern that properties need to match to get included in the map
     */
    public DynamicPropertiesMapCache(
        final Duration refreshInterval,
        final Environment environment,
        final Pattern propertiesNamesPattern
    ) {
        this.environment = environment;
        this.propertiesNamesPattern = propertiesNamesPattern;
        this.cache = Caffeine
            .newBuilder()
            .refreshAfterWrite(refreshInterval)
            .initialCapacity(1)
            .build(this::loadProperties);
    }

    /**
     * Get the current map of properties and their values.
     *
     * @return an immutable map of property name and property value, for any property that matches the pattern that has
     * a non-blank value
     */
    public Map<String, String> get() {
        return this.cache.get(CACHE_KEY);
    }

    private Map<String, String> loadProperties(@NonNull final String propertiesKey) {
        // There's only 1 item in this cache, so key is redundant
        return DynamicPropertiesMapCache.createDynamicPropertiesMap(
            this.environment,
            this.propertiesNamesPattern
        );
    }

    /**
     * Constructs a map of properties that match the given pattern, loading their latest value from the environment.
     * Properties with empty and null values are not included.
     *
     * @param environment         the environment
     * @param propertyNamePattern the regular expression to select the properties to include
     * @return an immutable map of property name and property value, for any property that matches the pattern that has
     * a non-blank value
     */
    public static Map<String, String> createDynamicPropertiesMap(
        final Environment environment,
        final Pattern propertyNamePattern
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
                    if (propertyNamePattern.matcher(propertyName).matches()) {
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
            if (StringUtils.isBlank(propertyValue)) {
                log.debug("Skipping blank value property: {}", propertyName);
            } else {
                propertiesMapBuilder.put(propertyName, propertyValue);
            }
        }

        return propertiesMapBuilder.build();
    }
}
