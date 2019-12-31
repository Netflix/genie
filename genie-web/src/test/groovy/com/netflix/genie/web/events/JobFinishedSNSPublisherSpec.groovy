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
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.exceptions.GenieNotFoundException
import com.netflix.genie.common.external.dtos.v4.Criterion
import com.netflix.genie.common.external.util.GenieObjectMapper
import com.netflix.genie.common.internal.dtos.v4.Application
import com.netflix.genie.common.internal.dtos.v4.ApplicationMetadata
import com.netflix.genie.common.internal.dtos.v4.Cluster
import com.netflix.genie.common.internal.dtos.v4.ClusterMetadata
import com.netflix.genie.common.internal.dtos.v4.Command
import com.netflix.genie.common.internal.dtos.v4.CommandMetadata
import com.netflix.genie.common.internal.dtos.v4.FinishedJob
import com.netflix.genie.web.data.services.JobPersistenceService
import com.netflix.genie.web.properties.SNSNotificationsProperties
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.spockframework.util.Assert
import spock.lang.Specification

import java.time.Instant

class JobFinishedSNSPublisherSpec extends Specification {
    Map<String, String> extraKeysMap
    String jobId
    String topicARN
    AmazonSNS snsClient
    SNSNotificationsProperties snsProperties
    JobPersistenceService jobPersistenceService
    MeterRegistry registry
    Counter counter
    ObjectMapper mapper
    JobStateChangeEvent event
    JobFinishedSNSPublisher publisher

    void setup() {
        this.extraKeysMap = Maps.newHashMap()
        this.jobId = UUID.randomUUID().toString()
        this.topicARN = UUID.randomUUID().toString()
        this.snsClient = Mock(AmazonSNS)
        this.snsProperties = Mock(SNSNotificationsProperties)
        this.jobPersistenceService = Mock(JobPersistenceService)
        this.registry = Mock(MeterRegistry)
        this.counter = Mock(Counter)
        this.mapper = GenieObjectMapper.getMapper()
        this.event = Mock(JobStateChangeEvent)
        this.publisher = new JobFinishedSNSPublisher(
            snsClient,
            snsProperties,
            jobPersistenceService,
            registry,
            mapper
        )
    }

    def "Ignore event if disabled"() {
        when:
        publisher.onApplicationEvent(event)

        then:
        1 * snsProperties.isEnabled() >> false
        0 * event._
        0 * jobPersistenceService._
        0 * snsClient._
    }

    def "Ignore event if not final state"() {
        when:
        publisher.onApplicationEvent(event)

        then:
        1 * snsProperties.isEnabled() >> true
        1 * event.getNewStatus() >> JobStatus.RUNNING
        0 * event._
        0 * jobPersistenceService._
        0 * snsClient._
    }

    def "Handle persistence service lookup exception"() {
        when:
        publisher.onApplicationEvent(event)

        then:
        1 * snsProperties.isEnabled() >> true
        1 * event.getNewStatus() >> JobStatus.SUCCEEDED
        1 * event.getJobId() >> jobId
        1 * jobPersistenceService.getFinishedJob(jobId) >> { throw new GenieNotFoundException("...") }
        0 * snsClient._
    }

    def "Publish event for job with none of the optional fields"() {
        Criterion criterion = new Criterion.Builder().withName("foo").build()

        def empty = Optional.empty()

        FinishedJob finishedJob = Mock(FinishedJob) {
            getName() >> "name"
            getUser() >> "user"
            getVersion() >> "version"
            getTags() >> Lists.newArrayList()
            getCreated() >> Instant.now()
            getStatus() >> JobStatus.FAILED
            getCommandCriterion() >> criterion
            getClusterCriteria() >> Lists.newArrayList(criterion)
            getApplications() >> Lists.newArrayList()
            getCommandArgs() >> Lists.newArrayList()
            _ >> empty
        }

        when:
        publisher.onApplicationEvent(event)

        then:
        1 * snsProperties.isEnabled() >> true
        1 * event.getNewStatus() >> JobStatus.FAILED
        1 * event.getJobId() >> jobId
        1 * jobPersistenceService.getFinishedJob(jobId) >> finishedJob
        1 * snsProperties.topicARN >> topicARN
        1 * snsProperties.getAdditionalEventKeys() >> extraKeysMap
        1 * snsClient.publish(topicARN, _ as String) >> {
            args ->
                Map<String, Object> eventDetails = mapper.convertValue(mapper.readTree(args[1] as String).get(AbstractSNSPublisher.EVENT_DETAILS_KEY_NAME), Map.class)
                Assert.that(eventDetails.entrySet().size() == 47)
                Assert.that(eventDetails.entrySet().stream().filter({ entry -> entry.getValue() == null }).count() == 35)
        }
        1 * registry.counter("genie.notifications.sns.publish.counter", _) >> counter
        1 * counter.increment()
    }

    def "Publish event for job with all of the optional fields"() {
        Criterion criterion = new Criterion.Builder().withName("foo").build()
        JsonNode jobMetadata = mapper.readTree("{\"foo\": \"foo\"}")
        ApplicationMetadata applicationMetadata = Mock(ApplicationMetadata) {
            getVersion() >> "1.0.0"
        }
        Application app1 = Mock(Application) {
            getMetadata() >> applicationMetadata
            getId() >> "app1"
        }
        Application app2 = Mock(Application) {
            getMetadata() >> applicationMetadata
            getId() >> "app2"
        }
        CommandMetadata commandMetadata = Mock() {
            getName() >> "Command 1"
            getVersion() >> "1.2.3"
            getDescription() >> Optional.of("A command!")
        }
        Command command = Mock(Command) {
            getMetadata() >> commandMetadata
            getId() >> "cmd1"
            getCreated() >> Instant.now()
            getUpdated() >> Instant.now()
            getExecutable() >> ["spark"]
        }

        ClusterMetadata clusterMetadata = Mock(ClusterMetadata) {
            getName() >> "Cluster 1"
            getVersion() >> "3.2.1"
            getDescription() >> Optional.of("A cluster!")
        }
        Cluster cluster = Mock(Cluster) {
            getMetadata() >> clusterMetadata
            getId() >> "cluster1"
            getCreated() >> Instant.now()
            getUpdated() >> Instant.now()
        }

        FinishedJob finishedJob = Mock(FinishedJob) {
            getName() >> "name"
            getUser() >> "user"
            getVersion() >> "version"
            getTags() >> Lists.newArrayList()
            getCreated() >> Instant.now()
            getStatus() >> JobStatus.FAILED
            getCommandCriterion() >> criterion
            getClusterCriteria() >> Lists.newArrayList(criterion)
            getApplications() >> Lists.newArrayList(app1, app2)
            getCommand() >> Optional.of(command)
            getCluster() >> Optional.of(cluster)
            getCommandArgs() >> Lists.newArrayList("arg1", "arg2")

            // Non-string fields
            getMetadata() >> Optional.of(jobMetadata)
            getStarted() >> Optional.of(Instant.now())
            getFinished() >> Optional.of(Instant.now())
            getRequestedMemory() >> Optional.of(512)
            getMemoryUsed() >> Optional.of(1024)
            getNumAttachments() >> Optional.of(1)
            getExitCode() >> Optional.of(1)

            // String fields
            _ >> Optional.of("random_string-" + UUID.randomUUID().toString())
        }

        when:
        publisher.onApplicationEvent(event)

        then:
        1 * snsProperties.isEnabled() >> true
        1 * event.getNewStatus() >> JobStatus.FAILED
        1 * event.getJobId() >> jobId
        1 * jobPersistenceService.getFinishedJob(jobId) >> finishedJob
        1 * snsProperties.topicARN >> topicARN
        1 * snsProperties.getAdditionalEventKeys() >> extraKeysMap
        1 * snsClient.publish(topicARN, _ as String) >> {
            args ->
                Map<String, Object> eventDetails = mapper.convertValue(mapper.readTree(args[1] as String).get(AbstractSNSPublisher.EVENT_DETAILS_KEY_NAME), Map.class)
                Assert.that(eventDetails.entrySet().size() == 47)
                Assert.that(eventDetails.entrySet().stream().filter({ entry -> entry.getValue() == null }).count() == 0)
        }
        1 * registry.counter("genie.notifications.sns.publish.counter", _) >> counter
        1 * counter.increment()
    }
}
