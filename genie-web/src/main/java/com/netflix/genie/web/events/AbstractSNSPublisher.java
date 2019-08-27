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
import com.google.common.collect.Sets;
import com.netflix.genie.web.properties.SNSNotificationsProperties;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.AccessLevel;
import lombok.Getter;
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
    private static final String PUBLISH_METRIC_COUNTER_NAME_FORMAT = "genie.notifications.sns.publish.counter";
    private static final String EVENT_TYPE_METRIC_TAG_NAME = "type";
    private static final String EVENT_TYPE_KEY_NAME = "type";
    private static final String EVENT_ID_KEY_NAME = "id";
    private static final String EVENT_TIMESTAMP_KEY_NAME = "timestamp";
    private static final String EVENT_DETAILS_KEY_NAME = "details";

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
        final Instant timestamp = Instant.now();
        eventMap.put(EVENT_TIMESTAMP_KEY_NAME, timestamp.toEpochMilli());

        // Add event details
        eventMap.put(EVENT_DETAILS_KEY_NAME, eventDetailsMap);

        final Set<Tag> metricTags = Sets.newHashSet(eventType.getTypeTag());

        try {
            // Serialize message
            final String serializedMessage = this.mapper.writeValueAsString(eventMap);
            // Send message
            this.snsClient.publish(topic, serializedMessage);
            log.debug("Published SNS notification (type: {})", eventType.name());
            metricTags.addAll(MetricsUtils.newSuccessTagsSet());
        } catch (JsonProcessingException | RuntimeException e) {
            metricTags.addAll(MetricsUtils.newFailureTagsSetForException(e));
            log.error("Failed to publish SNS notification", e);
        } finally {
            this.registry.counter(
                PUBLISH_METRIC_COUNTER_NAME_FORMAT,
                metricTags
            ).increment();
        }
    }

    /**
     * Types of event.
     */
    @Getter(AccessLevel.PROTECTED)
    protected enum EventType {
        JOB_STATUS_CHANGE,
        JOB_FINISHED;

        private final Tag typeTag = Tag.of(EVENT_TYPE_METRIC_TAG_NAME, this.name());
    }
}
