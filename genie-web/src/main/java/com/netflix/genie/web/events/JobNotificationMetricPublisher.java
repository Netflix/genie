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

import com.google.common.collect.Sets;
import com.netflix.genie.web.util.MetricsConstants;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;

/**
 * Listens to job status changes and publishes metrics.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class JobNotificationMetricPublisher implements ApplicationListener<JobStateChangeEvent> {
    private static final String STATE_TRANSITION_METRIC_NAME = "genie.jobs.notifications.state-transition.counter";
    private static final String FINAL_STATE_METRIC_NAME = "genie.jobs.notifications.final-state.counter";
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param registry metrics registry
     */
    public JobNotificationMetricPublisher(final MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onApplicationEvent(final JobStateChangeEvent event) {

        final String fromStateString = event.getPreviousStatus() == null ? "null" : event.getPreviousStatus().name();
        final String toStateString = event.getNewStatus().name();
        final boolean isFinalState = event.getNewStatus().isFinished();

        log.info(
            "Job '{}' changed state from {} to {} {}",
            event.getJobId(),
            fromStateString,
            toStateString,
            isFinalState ? "(final state)" : ""
        );

        // Increment counter for this particular from/to transition
        this.registry.counter(
            STATE_TRANSITION_METRIC_NAME,
            Sets.newHashSet(
                Tag.of(MetricsConstants.TagKeys.FROM_STATE, fromStateString),
                Tag.of(MetricsConstants.TagKeys.TO_STATE, toStateString)
            )
        ).increment();

        // Increment counter for final state
        if (isFinalState) {
            this.registry.counter(
                FINAL_STATE_METRIC_NAME,
                MetricsConstants.TagKeys.TO_STATE,
                toStateString
            ).increment();
        }
    }
}
