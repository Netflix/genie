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
package com.netflix.genie.web.events;

import com.amazonaws.services.sns.AmazonSNS;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.netflix.genie.web.properties.SNSNotificationsProperties;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Abstract SNS publisher that adds fields common to all payloads, records metrics, etc.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
abstract class AbstractSNSPublisher {
    private static final String PUBLISH_METRIC_COUNTER_NAME_FORMAT = "genie.notifications.sns.publish.%s.counter";
    private static final String EVENT_TYPE_KEY_NAME = "event-type";
    private static final String EVENT_ID_KEY_NAME = "event-id";
    private static final String EVENT_TIMESTAMP_KEY_NAME = "event-timestamp";
    private static final String EVENT_DETAILS_KEY_NAME = "event-details";

    protected final SNSNotificationsProperties properties;
    protected final MeterRegistry registry;

    private final AmazonSNS snsClient;
    private final ObjectMapper mapper;

    /**
     * Constructor.
     *
     * @param properties SNS properties
     * @param registry   metrics registry
     * @param snsClient  SNS client
     * @param mapper     JSON object mapper
     */
    AbstractSNSPublisher(
        final SNSNotificationsProperties properties,
        final MeterRegistry registry,
        final AmazonSNS snsClient,
        final ObjectMapper mapper
    ) {
        this.properties = properties;
        this.registry = registry;
        this.snsClient = snsClient;
        this.mapper = mapper;
    }

    protected void publishEvent(final EventType eventType, final HashMap<String, Object> eventDetailsMap) {
        final String topic = this.properties.getTopicARN();

        if (StringUtils.isBlank(topic)) {
            // Likely a misconfiguration. Emit a warning.
            log.warn("SNS Notifications enabled, but no topic specified");
            return;
        }

        final Map<String, Object> eventMap = Maps.newHashMap();

        // Add static keys defined in configuration
        eventMap.putAll(this.properties.getAdditionalEventKeys());

        // Add event type, timestamp, event id
        eventMap.put(EVENT_TYPE_KEY_NAME, eventType.name());
        eventMap.put(EVENT_ID_KEY_NAME, UUID.randomUUID().toString());
        eventMap.put(EVENT_TIMESTAMP_KEY_NAME, Instant.now());

        // Add event details
        eventMap.put(EVENT_DETAILS_KEY_NAME, eventDetailsMap);

        Set<Tag> metricTags = MetricsUtils.newSuccessTagsSet();

        try {
            // Serialize message
            final String serializedMessage = this.mapper.writeValueAsString(eventMap);
            // Send message
            this.snsClient.publish(topic, serializedMessage);
            log.debug("Published SNS notification (type: {})", eventType.name());
        } catch (JsonProcessingException | RuntimeException e) {
            metricTags = MetricsUtils.newFailureTagsSetForException(e);
            log.error("Failed to publish SNS notification", e);
        } finally {
            registry.counter(
                String.format(PUBLISH_METRIC_COUNTER_NAME_FORMAT, eventType.name()),
                metricTags
            ).increment();
        }
    }

    /**
     * Types of event.
     */
    protected enum EventType {
        JOB_STATUS_CHANGE,
    }
}
