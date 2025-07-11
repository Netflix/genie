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

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Maps
import com.netflix.genie.common.external.util.GenieObjectMapper
import com.netflix.genie.common.internal.dtos.JobStatus
import com.netflix.genie.web.properties.SNSNotificationsProperties
import com.netflix.genie.web.util.MetricsUtils
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.PublishRequest
import software.amazon.awssdk.services.sns.model.PublishResponse
import spock.lang.Specification
import spock.lang.Unroll

class JobStateChangeSNSPublisherSpec extends Specification {
    JobStateChangeSNSPublisher publisher
    SnsClient snsClient
    SNSNotificationsProperties snsProperties
    MeterRegistry registry
    ObjectMapper mapper
    JobStateChangeEvent event
    Map<String, String> extraKeysMap
    String jobId
    String topicARN
    Counter counter

    void setup() {
        this.snsClient = Mock(SnsClient)
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
        0 * snsClient.publish(_)
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
        0 * snsClient.publish(_)
        0 * registry.counter(_, _)
    }

    @Unroll
    def "Publish event (when previous state is #prevState)"() {
        setup:
        this.extraKeysMap.putAll([foo: "bar", bar: "foo"])
        def tags = MetricsUtils.newSuccessTagsSet()
        tags.add(AbstractSNSPublisher.EventType.JOB_STATUS_CHANGE.getTypeTag())
        PublishRequest capturedRequest = null

        when:
        this.publisher.onApplicationEvent(event)

        then:
        1 * event.getJobId() >> jobId
        1 * event.getPreviousStatus() >> prevState
        1 * event.getNewStatus() >> JobStatus.RUNNING
        1 * snsProperties.isEnabled() >> true
        1 * snsProperties.getTopicARN() >> topicARN
        1 * snsProperties.getAdditionalEventKeys() >> extraKeysMap
        1 * snsClient.publish(_ as PublishRequest) >> { args ->
            capturedRequest = args[0]
            return PublishResponse.builder().build()
        }
        1 * registry.counter(
            "genie.notifications.sns.publish.counter",
            tags
        ) >> counter
        1 * counter.increment()

        and: "Verify message content"
        capturedRequest != null
        capturedRequest.topicArn() == topicARN
        def message = capturedRequest.message()
        message != null

        def parsedMessage = new groovy.json.JsonSlurper().parseText(message)
        parsedMessage.size() == 7
        parsedMessage.foo == "bar"
        parsedMessage.bar == "foo"
        parsedMessage.type == "JOB_STATUS_CHANGE"
        parsedMessage.id != null
        parsedMessage.timestamp != null
        parsedMessage.timestamp instanceof Number
        parsedMessage.isoTimestamp != null
        parsedMessage.isoTimestamp instanceof String
        def eventDetails = parsedMessage.details

        eventDetails != null
        eventDetails.jobId == jobId
        eventDetails.fromState == String.valueOf(prevState)
        eventDetails.toState == JobStatus.RUNNING.name()

        where:
        prevState      | _
        null           | _
        JobStatus.INIT | _
    }

    def "Publish event exception"() {
        setup:
        AwsServiceException e = AwsServiceException.builder()
            .message("Authorization error")
            .build()
        def tags = MetricsUtils.newFailureTagsSetForException(e)
        tags.add(AbstractSNSPublisher.EventType.JOB_STATUS_CHANGE.getTypeTag())

        when:
        this.publisher.onApplicationEvent(event)

        then:
        1 * snsProperties.isEnabled() >> true
        1 * event.getJobId() >> jobId
        1 * event.getPreviousStatus() >> JobStatus.INIT
        1 * event.getNewStatus() >> JobStatus.RUNNING
        1 * snsProperties.getAdditionalEventKeys() >> extraKeysMap
        1 * snsProperties.getTopicARN() >> topicARN
        1 * snsClient.publish(_ as PublishRequest) >> {
            throw e
        }
        1 * registry.counter(
            "genie.notifications.sns.publish.counter",
            tags
        ) >> counter
        1 * counter.increment()
    }
}
