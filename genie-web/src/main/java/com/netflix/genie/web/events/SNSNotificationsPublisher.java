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
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.web.properties.SNSNotificationsProperties;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationListener;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Publishes Amazon SNS notifications.
 * Currently, only job status changes trigger a notification.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class SNSNotificationsPublisher implements ApplicationListener<JobStateChangeEvent> {

    private static final String PUBLISH_METRIC_COUNTER_NAME = "genie.notifications.sns.publish.counter";

    // Top level keys
    private static final String EVENT_TYPE_KEY_NAME = "event-type";
    private static final String EVENT_ID_KEY_NAME = "event-id";
    private static final String EVENT_TIMESTAMP_KEY_NAME = "event-timestamp";
    private static final String EVENT_DETAILS_KEY_NAME = "event-details";

    // Keys for JOB_STATUS_CHANGE
    private static final String JOB_ID_KEY_NAME = "job-id";
    private static final String FROM_STATE_KEY_NAME = "from-state";
    private static final String TO_STATE_KEY_NAME = "to-state";

    private final AmazonSNS snsClient;
    private final SNSNotificationsProperties properties;
    private final MeterRegistry registry;
    private final ObjectMapper mapper;

    /**
     * Constructor.
     *
     * @param snsClient  Amazon SNS client
     * @param properties configuration properties
     * @param registry   metrics registry
     * @param mapper     object mapper
     */
    public SNSNotificationsPublisher(
        final AmazonSNS snsClient,
        final SNSNotificationsProperties properties,
        final MeterRegistry registry,
        final ObjectMapper mapper
    ) {
        this.snsClient = snsClient;
        this.properties = properties;
        this.registry = registry;
        this.mapper = mapper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onApplicationEvent(final JobStateChangeEvent event) {
        final String jobId = event.getJobId();
        final JobStatus fromState = event.getPreviousStatus();
        final JobStatus toState = event.getNewStatus();

        final HashMap<String, String> eventDetailsMap = Maps.newHashMap();

        eventDetailsMap.put(JOB_ID_KEY_NAME, jobId);
        eventDetailsMap.put(FROM_STATE_KEY_NAME, fromState != null ? fromState.name() : "null");
        eventDetailsMap.put(TO_STATE_KEY_NAME, toState.name());

        this.publishEvent(EventType.JOB_STATUS_CHANGE, eventDetailsMap);
    }

    private void publishEvent(final EventType eventType, final HashMap<String, String> eventDetailsMap) {
        if (!this.properties.isEnabled()) {
            // Publishing is disabled
            return;
        }

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
            log.debug("Published SNS notification)");
        } catch (JsonProcessingException | RuntimeException e) {
            metricTags = MetricsUtils.newFailureTagsSetForException(e);
            log.error("Failed to publish SNS notification", e);
        } finally {
            registry.counter(PUBLISH_METRIC_COUNTER_NAME, metricTags).increment();
        }
    }

    /**
     * Types of event.
     */
    private enum EventType {
        JOB_STATUS_CHANGE,
    }
}
