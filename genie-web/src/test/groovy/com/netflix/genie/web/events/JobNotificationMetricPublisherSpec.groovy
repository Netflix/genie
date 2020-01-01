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
package com.netflix.genie.web.events

import com.google.common.collect.Sets
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.web.util.MetricsConstants
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import spock.lang.Specification
import spock.lang.Unroll

class JobNotificationMetricPublisherSpec extends Specification {
    String jobId = UUID.randomUUID().toString()

    @Unroll
    def "State change notification from: #fromState to: #toState"() {
        setup:
        boolean isFinalState = toState.isFinished()
        MeterRegistry registry = Mock(MeterRegistry)
        JobNotificationMetricPublisher publisher = new JobNotificationMetricPublisher(registry)
        Counter counter = Mock(Counter)
        Set<Tag> transitionTags = Sets.newHashSet(
            Tag.of(MetricsConstants.TagKeys.FROM_STATE, fromState == null ? "null" : fromState.name()),
            Tag.of(MetricsConstants.TagKeys.TO_STATE, toState.name()),
        )

        when:
        publisher.onApplicationEvent(
            new JobStateChangeEvent(jobId, fromState, toState, this)
        )

        then:
        1 * registry.counter(
            JobNotificationMetricPublisher.STATE_TRANSITION_METRIC_NAME,
            transitionTags
        ) >> counter
        1 * counter.increment()

        if (!isFinalState) {
            0 * registry.counter(JobNotificationMetricPublisher.FINAL_STATE_METRIC_NAME, _)
        } else {
            1 * registry.counter(
                JobNotificationMetricPublisher.FINAL_STATE_METRIC_NAME,
                MetricsConstants.TagKeys.TO_STATE,
                toState.name()
            ) >> counter
            1 * counter.increment()
        }

        where:
        fromState          | toState
        null               | JobStatus.RESERVED
        JobStatus.RESERVED | JobStatus.RESOLVED
        JobStatus.INIT     | JobStatus.KILLED
        JobStatus.RESERVED | JobStatus.FAILED
        JobStatus.RUNNING  | JobStatus.SUCCEEDED
    }
}
