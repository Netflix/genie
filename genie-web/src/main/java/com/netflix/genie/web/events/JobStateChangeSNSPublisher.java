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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.web.properties.SNSNotificationsProperties;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;

import java.util.HashMap;

/**
 * Publishes Amazon SNS notifications for fine-grained job state changes.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class JobStateChangeSNSPublisher
    extends AbstractSNSPublisher
    implements ApplicationListener<JobStateChangeEvent> {

    private static final String JOB_ID_KEY_NAME = "jobId";
    private static final String FROM_STATE_KEY_NAME = "fromState";
    private static final String TO_STATE_KEY_NAME = "toState";

    /**
     * Constructor.
     *
     * @param snsClient  Amazon SNS client
     * @param properties configuration properties
     * @param registry   metrics registry
     * @param mapper     object mapper
     */
    public JobStateChangeSNSPublisher(
        final AmazonSNS snsClient,
        final SNSNotificationsProperties properties,
        final MeterRegistry registry,
        final ObjectMapper mapper
    ) {
        super(properties, registry, snsClient, mapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onApplicationEvent(final JobStateChangeEvent event) {
        if (!this.properties.isEnabled()) {
            // Publishing is disabled
            return;
        }

        final String jobId = event.getJobId();
        final JobStatus fromState = event.getPreviousStatus();
        final JobStatus toState = event.getNewStatus();

        final HashMap<String, Object> eventDetailsMap = Maps.newHashMap();

        eventDetailsMap.put(JOB_ID_KEY_NAME, jobId);
        eventDetailsMap.put(FROM_STATE_KEY_NAME, fromState != null ? fromState.name() : "null");
        eventDetailsMap.put(TO_STATE_KEY_NAME, toState.name());

        this.publishEvent(EventType.JOB_STATUS_CHANGE, eventDetailsMap);
    }
}
