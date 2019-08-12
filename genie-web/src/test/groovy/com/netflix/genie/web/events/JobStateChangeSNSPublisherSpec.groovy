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

import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.model.AuthorizationErrorException
import com.amazonaws.services.sns.model.PublishResult
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Maps
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.web.properties.SNSNotificationsProperties
import com.netflix.genie.web.util.MetricsUtils
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import spock.lang.Specification
import spock.lang.Unroll

class JobStateChangeSNSPublisherSpec extends Specification {
    JobStateChangeSNSPublisher publisher
    AmazonSNS snsClient
    SNSNotificationsProperties snsProperties
    MeterRegistry registry
    ObjectMapper mapper
    JobStateChangeEvent event
    Map<String, String> extraKeysMap
    String jobId
    String topicARN
    Counter counter

    void setup() {
        this.snsClient = Mock(AmazonSNS)
        this.snsProperties = Mock(SNSNotificationsProperties)
        this.registry = Mock(MeterRegistry)
        this.mapper = GenieObjectMapper.getMapper()
        this.publisher = new JobStateChangeSNSPublisher(snsClient, snsProperties, registry, mapper)
        this.event = Mock(JobStateChangeEvent)
        this.extraKeysMap = Maps.newHashMap()
        this.jobId = UUID.randomUUID().toString()
        this.topicARN = UUID.randomUUID().toString()
        this.counter = Mock(Counter)
    }

    def "Skip publishing if disabled"() {
        setup:
        JobStateChangeEvent event = Mock(JobStateChangeEvent)

        when:
        this.publisher.onApplicationEvent(event)

        then:
        1 * snsProperties.isEnabled() >> false
        0 * event.getJobId()
        0 * event.getPreviousStatus()
        0 * event.getNewStatus()
        0 * snsClient.publish(_, _)
        0 * registry.counter(_, _)
    }

    def "Skip publishing if no topic"() {
        setup:
        JobStateChangeEvent event = Mock(JobStateChangeEvent)

        when:
        this.publisher.onApplicationEvent(event)

        then:
        1 * event.getJobId() >> jobId
        1 * event.getPreviousStatus() >> JobStatus.CLAIMED
        1 * event.getNewStatus() >> JobStatus.INIT
        1 * snsProperties.isEnabled() >> true
        1 * snsProperties.getTopicARN() >> ""
        0 * snsClient.publish(_, _)
        0 * registry.counter(_, _)
    }

    @Unroll
    def "Publish event (when previous state is #prevState)"() {
        setup:
        this.extraKeysMap.putAll([foo: "bar", bar: "foo"])
        String message = null

        when:
        this.publisher.onApplicationEvent(event)

        then:
        1 * event.getJobId() >> jobId
        1 * event.getPreviousStatus() >> prevState
        1 * event.getNewStatus() >> JobStatus.RUNNING
        1 * snsProperties.isEnabled() >> true
        1 * snsProperties.getTopicARN() >> topicARN
        1 * snsProperties.getAdditionalEventKeys() >> extraKeysMap
        1 * snsClient.publish(topicARN, _ as String) >> {
            args ->
                message = args[1] as String
                return Mock(PublishResult)
        }
        1 * registry.counter(
            "genie.notifications.sns.publish.JOB_STATUS_CHANGE.counter",
            MetricsUtils.newSuccessTagsSet()
        ) >> counter
        1 * counter.increment()

        expect:
        message != null
        Map<String, Object> parsedMessage = GenieObjectMapper.getMapper().readValue(
            message,
            Map.class
        )

        parsedMessage.size() == 6
        parsedMessage.get("foo") as String == "bar"
        parsedMessage.get("bar") as String == "foo"
        parsedMessage.get("event-type") as String ==  "JOB_STATUS_CHANGE"
        parsedMessage.get("event-id") != null
        parsedMessage.get("event-timestamp") != null
        Map<String, String> eventDetails = parsedMessage.get("event-details") as Map<String, String>
        eventDetails != null
        eventDetails.get("job-id") == jobId
        eventDetails.get("from-state") == String.valueOf(prevState)
        eventDetails.get("to-state") == JobStatus.RUNNING.name()

        where:
        prevState      | _
        null           | _
        JobStatus.INIT | _
    }

    def "Publish event exception"() {
        setup:
        Exception e = new AuthorizationErrorException("...")

        when:
        this.publisher.onApplicationEvent(event)
        print(MetricsUtils.newFailureTagsSetForException(e))

        then:
        1 * snsProperties.isEnabled() >> true
        1 * event.getJobId() >> jobId
        1 * event.getPreviousStatus() >> JobStatus.INIT
        1 * event.getNewStatus() >> JobStatus.RUNNING
        1 * snsProperties.getAdditionalEventKeys() >> extraKeysMap
        1 * snsProperties.getTopicARN() >> topicARN
        1 * snsClient.publish(topicARN, _ as String) >> {
            throw e
        }
        1 * registry.counter(
            "genie.notifications.sns.publish.JOB_STATUS_CHANGE.counter",
            MetricsUtils.newFailureTagsSetForException(e)
        ) >> counter
        1 * counter.increment()

    }

}
