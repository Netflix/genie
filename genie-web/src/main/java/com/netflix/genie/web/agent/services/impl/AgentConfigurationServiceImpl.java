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

import com.google.common.collect.Sets;
import com.netflix.genie.common.internal.util.PropertiesMapCache;
import com.netflix.genie.web.agent.services.AgentConfigurationService;
import com.netflix.genie.web.properties.AgentConfigurationProperties;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

    private static final String RELOAD_PROPERTIES_TIMER = "genie.services.agentConfiguration.reloadProperties.timer";
    private static final String PROPERTIES_COUNT_TAG = "numProperties";

    private final AgentConfigurationProperties agentConfigurationProperties;
    private final MeterRegistry registry;
    private final PropertiesMapCache propertiesMapCache;

    /**
     * Constructor.
     *
     * @param agentConfigurationProperties the properties
     * @param propertyMapCache             the property map cache
     * @param registry                     the metrics registry
     */
    public AgentConfigurationServiceImpl(
        final AgentConfigurationProperties agentConfigurationProperties,
        final PropertiesMapCache propertyMapCache,
        final MeterRegistry registry
    ) {
        this.agentConfigurationProperties = agentConfigurationProperties;
        this.propertiesMapCache = propertyMapCache;
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getAgentProperties() {
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        try {
            final Map<String, String> propertiesMap = this.propertiesMapCache.get();
            tags.add(Tag.of(PROPERTIES_COUNT_TAG, String.valueOf(propertiesMap.size())));
            MetricsUtils.addSuccessTags(tags);
            return propertiesMap;
        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            final long end = System.nanoTime();
            this.registry.timer(RELOAD_PROPERTIES_TIMER, tags).record(end - start, TimeUnit.NANOSECONDS);
        }
    }
}
