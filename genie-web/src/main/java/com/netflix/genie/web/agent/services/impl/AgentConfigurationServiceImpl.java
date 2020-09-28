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
package com.netflix.genie.web.agent.services.impl;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.netflix.genie.web.agent.services.AgentConfigurationService;
import com.netflix.genie.web.properties.AgentConfigurationProperties;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySources;
import org.springframework.core.env.StandardEnvironment;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * This implementation of {@link AgentConfigurationService} forwards properties set on the server that match a given
 * set of regular expressions, plus any additional ones specified in configuration.
 * It utilizes a cache to avoid recomputing the set of properties and values for every request.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class AgentConfigurationServiceImpl implements AgentConfigurationService {

    private static final String AGENT_PROPERTIES_CACHE_KEY = "agent-properties";
    private static final String RELOAD_PROPERTIES_TIMER = "genie.services.agentConfiguration.reloadProperties.timer";
    private static final String PROPERTIES_COUNT_TAG = "numProperties";

    private final AgentConfigurationProperties agentConfigurationProperties;
    private final Environment environment;
    private final MeterRegistry registry;
    private final Pattern agentPropertiesPattern;
    private final LoadingCache<String, Map<String, String>> cache;

    /**
     * Constructor.
     *
     * @param agentConfigurationProperties the properties
     * @param environment                  the environment
     * @param registry                     the metrics registry
     */
    public AgentConfigurationServiceImpl(
        final AgentConfigurationProperties agentConfigurationProperties,
        final Environment environment,
        final MeterRegistry registry
    ) {
        this.agentConfigurationProperties = agentConfigurationProperties;
        this.environment = environment;
        this.registry = registry;

        this.agentPropertiesPattern = Pattern.compile(
            this.agentConfigurationProperties.getAgentPropertiesFilterPattern(),
            Pattern.CASE_INSENSITIVE
        );

        // Use a cache to re-compute agent properties periodically, rather than for every request.
        this.cache = Caffeine
            .newBuilder()
            .expireAfterWrite(this.agentConfigurationProperties.getCacheExpirationInterval())
            .refreshAfterWrite(this.agentConfigurationProperties.getCacheRefreshInterval())
            .initialCapacity(1)
            .build(this::loadProperties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getAgentProperties() {
        return this.cache.get(AGENT_PROPERTIES_CACHE_KEY);
    }


    private Map<String, String> loadProperties(@NonNull final String propertiesKey) {
        if (!AGENT_PROPERTIES_CACHE_KEY.equals(propertiesKey)) {
            throw new IllegalArgumentException("Unknown key to load: " + propertiesKey);
        }

        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        Integer numProperties = null;
        try {
            final Map<String, String> properties = reloadAgentProperties();
            MetricsUtils.addSuccessTags(tags);
            numProperties = properties.size();
            return properties;
        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            tags.add(Tag.of(PROPERTIES_COUNT_TAG, String.valueOf(numProperties)));
            this.registry.timer(RELOAD_PROPERTIES_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Map<String, String> reloadAgentProperties() {
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
                    if (agentPropertiesPattern.matcher(propertyName).matches()) {
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
