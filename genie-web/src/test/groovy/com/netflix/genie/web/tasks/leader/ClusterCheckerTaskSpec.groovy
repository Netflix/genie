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
package com.netflix.genie.web.tasks.leader

import com.google.common.collect.Sets
import com.netflix.genie.common.dto.Job
import com.netflix.genie.common.dto.JobExecution
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.exceptions.GenieNotFoundException
import com.netflix.genie.common.internal.util.GenieHostInfo
import com.netflix.genie.web.data.services.AgentConnectionPersistenceService
import com.netflix.genie.web.data.services.JobPersistenceService
import com.netflix.genie.web.data.services.JobSearchService
import com.netflix.genie.web.properties.ClusterCheckerProperties
import com.netflix.genie.web.tasks.GenieTaskScheduleType
import com.netflix.genie.web.util.MetricsConstants
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties
import org.springframework.http.HttpStatus
import org.springframework.web.client.HttpStatusCodeException
import org.springframework.web.client.RestClientException
import org.springframework.web.client.RestTemplate
import spock.lang.Specification

class ClusterCheckerTaskSpec extends Specification {

    ClusterCheckerTask task
    String hostname
    ClusterCheckerProperties properties
    JobSearchService jobSearchService
    JobPersistenceService jobPersistenceService
    AgentConnectionPersistenceService agentConnectionPersistenceService
    RestTemplate restTemplate
    MeterRegistry meterRegistry

    void setup() {

        this.hostname = UUID.randomUUID().toString()
        this.properties = Mock(ClusterCheckerProperties) {
            _ * getPort() >> 8080
            _ * getHealthIndicatorsToIgnore() >> "memory,genie "
            _ * getScheme() >> "http"
            _ * getLostThreshold() >> 3
        }

        this.jobSearchService = Mock(JobSearchService.class)
        this.jobPersistenceService = Mock(JobPersistenceService.class)
        this.agentConnectionPersistenceService = Mock(AgentConnectionPersistenceService)
        this.restTemplate = Mock(RestTemplate.class)
        this.meterRegistry = new SimpleMeterRegistry()

        final GenieHostInfo genieHostInfo = new GenieHostInfo(this.hostname)
        final WebEndpointProperties serverProperties = Mock(WebEndpointProperties.class) {
            _ * getBasePath() >> "/actuator"
        }

        this.task = new ClusterCheckerTask(
            genieHostInfo,
            this.properties,
            this.jobSearchService,
            this.jobPersistenceService,
            this.agentConnectionPersistenceService,
            this.restTemplate,
            serverProperties,
            meterRegistry
        )
    }

    def "Run"() {
        final String host1 = "node1.genie.com"
        final String host1url = "http://" + host1 + ":8080/actuator/health"
        final String host2 = "node2.genie.com"
        final String host2url = "http://" + host2 + ":8080/actuator/health"
        final String host3 = "node3.genie.com"
        final String host3url = "http://" + host3 + ":8080/actuator/health"

        Set<String> hosts = Sets.newHashSet(this.hostname, host1, host2, host3)

        def job1 = Mock(Job) {
            getId() >> Optional.of(UUID.randomUUID().toString())
        }

        def job2 = Mock(Job) {
            getId() >> Optional.of(UUID.randomUUID().toString())
        }

        def restException = new RestClientException("blah")
        def outOfServiceHealthException = Mock(HttpStatusCodeException) {
            getStatusCode() >> HttpStatus.SERVICE_UNAVAILABLE
            getResponseBodyAsString() >> "{" +
                "\"status\":\"OUT_OF_SERVICE\", " +
                "\"details\": {" +
                "  \"db\": { \"status\": \"OUT_OF_SERVICE\"}," +
                "  \"memory\": { \"status\": \"UP\"}" +
                "  }" +
                "}"
        }

        def healthyResponse = "{" +
            "\"status\":\"UP\", " +
            "\"details\": {" +
            "  \"db\": { \"status\": \"UP\"}," +
            "  \"memory\": { \"status\": \"UP\"}" +
            "  }" +
            "}"

        def outOfServiceButHealthyException = Mock(HttpStatusCodeException) {
            getStatusCode() >> HttpStatus.SERVICE_UNAVAILABLE
            getResponseBodyAsString() >> "{" +
                "\"status\":\"OUT_OF_SERVICE\", " +
                "\"details\": {" +
                "  \"db\": { \"status\": \"UP\"}," +
                "  \"memory\": { \"status\": \"OUT_OF_SERVICE\", \"details\":{\"foo\":\"bar\"} }" +
                "  }" +
                "}"
            getMessage() >> "Error"
            getStackTrace() >> new StackTraceElement[0]
            getCause() >> null
        }

        def jobPersistenceServiceException = new GenieNotFoundException("No such job")

        when:
        this.task.run()

        then:
        1 * this.jobSearchService.getAllHostsWithActiveJobs() >> hosts
        1 * this.restTemplate.getForObject(host1url, String.class) >> ""
        1 * this.restTemplate.getForObject(host2url, String.class) >> { throw restException }
        1 * this.restTemplate.getForObject(host3url, String.class) >> { throw outOfServiceHealthException }

        this.task.getErrorCountsSize() == 3
        this.meterRegistry.counter(
            ClusterCheckerTask.BAD_RESPONSE_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host1
        ).count() == 1
        this.meterRegistry.counter(
            ClusterCheckerTask.BAD_HOST_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host2
        ).count() == 1

        this.meterRegistry.counter(
            ClusterCheckerTask.BAD_HEALTH_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host3,
            MetricsConstants.TagKeys.HEALTH_INDICATOR, "db",
            MetricsConstants.TagKeys.HEALTH_STATUS, "OUT_OF_SERVICE"
        ).count() == 1

        when:
        this.task.run()

        then:
        1 * this.jobSearchService.getAllHostsWithActiveJobs() >> hosts
        1 * this.restTemplate.getForObject(host1url, String.class) >> healthyResponse
        1 * this.restTemplate.getForObject(host2url, String.class) >> { throw restException }
        1 * this.restTemplate.getForObject(host3url, String.class) >> { throw outOfServiceButHealthyException }

        this.task.getErrorCountsSize() == 1
        this.meterRegistry.counter(
            ClusterCheckerTask.BAD_RESPONSE_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host1
        ).count() == 1
        this.meterRegistry.counter(
            ClusterCheckerTask.BAD_HOST_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host2
        ).count() == 2

        this.meterRegistry.counter(
            ClusterCheckerTask.BAD_HEALTH_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host3,
            MetricsConstants.TagKeys.HEALTH_INDICATOR, "db",
            MetricsConstants.TagKeys.HEALTH_STATUS, "OUT_OF_SERVICE"
        ).count() == 1
        this.meterRegistry.counter(
            ClusterCheckerTask.BAD_HEALTH_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host3,
            MetricsConstants.TagKeys.HEALTH_INDICATOR, "memory",
            MetricsConstants.TagKeys.HEALTH_STATUS, "OUT_OF_SERVICE"
        ).count() == 0

        when:
        this.task.run()

        then:
        1 * this.jobSearchService.getAllHostsWithActiveJobs() >> hosts
        1 * this.restTemplate.getForObject(host1url, String.class) >> healthyResponse
        1 * this.restTemplate.getForObject(host2url, String.class) >> { throw restException }
        1 * this.restTemplate.getForObject(host3url, String.class) >> { throw outOfServiceButHealthyException }

        this.task.getErrorCountsSize() == 1
        this.meterRegistry.counter(
            ClusterCheckerTask.BAD_RESPONSE_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host1
        ).count() == 1
        this.meterRegistry.counter(
            ClusterCheckerTask.BAD_HOST_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host2
        ).count() == 3

        this.meterRegistry.counter(
            ClusterCheckerTask.BAD_HEALTH_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host3,
            MetricsConstants.TagKeys.HEALTH_INDICATOR, "db",
            MetricsConstants.TagKeys.HEALTH_STATUS, "OUT_OF_SERVICE"
        ).count() == 1
        this.meterRegistry.counter(
            ClusterCheckerTask.BAD_HEALTH_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host3,
            MetricsConstants.TagKeys.HEALTH_INDICATOR, "memory",
            MetricsConstants.TagKeys.HEALTH_STATUS, "OUT_OF_SERVICE"
        ).count() == 0

        1 * this.jobSearchService.getAllActiveJobsOnHost(host2) >> [job1, job2]
        1 * this.jobPersistenceService.setJobCompletionInformation(
            job1.getId().get(),
            JobExecution.LOST_EXIT_CODE,
            JobStatus.FAILED,
            "Genie leader can't reach node running job. Assuming node and job are lost.",
            null,
            null
        )
        1 * this.meterRegistry.counter(
            ClusterCheckerTask.FAILED_JOBS_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host2,
            MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS
        ).count() == 1

        1 * this.jobPersistenceService.setJobCompletionInformation(
            job2.getId().get(),
            JobExecution.LOST_EXIT_CODE,
            JobStatus.FAILED,
            "Genie leader can't reach node running job. Assuming node and job are lost.",
            null,
            null
        ) >> { throw jobPersistenceServiceException }
        1 * this.meterRegistry.counter(
            ClusterCheckerTask.FAILED_JOBS_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host2,
            MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.FAILURE,
            MetricsConstants.TagKeys.EXCEPTION_CLASS, jobPersistenceServiceException.class.getCanonicalName()
        ).count() == 1

        1 * this.agentConnectionPersistenceService.removeAllAgentConnectionToServer(host2) >> {
            throw new RuntimeException("...")
        }
        1 * this.meterRegistry.counter(
            ClusterCheckerTask.REAPED_CONNECTIONS_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host2,
            MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.FAILURE,
            MetricsConstants.TagKeys.EXCEPTION_CLASS, RuntimeException.class.getCanonicalName()
        ).count() == 1

        when:
        this.task.run()

        then:
        1 * this.jobSearchService.getAllHostsWithActiveJobs() >> hosts
        1 * this.restTemplate.getForObject(host1url, String.class) >> healthyResponse
        1 * this.restTemplate.getForObject(host2url, String.class) >> { throw restException }
        1 * this.restTemplate.getForObject(host3url, String.class) >> healthyResponse

        this.task.getErrorCountsSize() == 0
        this.meterRegistry.counter(
            ClusterCheckerTask.BAD_HOST_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host2
        ).count() == 4

        1 * this.jobSearchService.getAllActiveJobsOnHost(host2) >> [job2]
        1 * this.jobPersistenceService.setJobCompletionInformation(
            job2.getId().get(),
            JobExecution.LOST_EXIT_CODE,
            JobStatus.FAILED,
            "Genie leader can't reach node running job. Assuming node and job are lost.",
            null,
            null
        )
        1 * this.meterRegistry.counter(
            ClusterCheckerTask.FAILED_JOBS_COUNT_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host2,
            MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS
        ).count() == 2

        1 * this.agentConnectionPersistenceService.removeAllAgentConnectionToServer(host2) >> 4
        1 * this.meterRegistry.counter(
            ClusterCheckerTask.REAPED_CONNECTIONS_METRIC_NAME,
            MetricsConstants.TagKeys.HOST, host2,
            MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS
        ).count() == 4

        when:
        this.task.cleanup()

        then:
        this.task.getErrorCountsSize() == 0
    }

    def "GetScheduleType"() {
        when:
        def scheduleType = this.task.getScheduleType()

        then:
        scheduleType == GenieTaskScheduleType.FIXED_RATE
    }

    def "GetFixedRate"() {
        when:
        def rate = this.task.getFixedRate()

        then:
        1 * properties.getRate() >> 10000L
        rate == 10000L
    }
}
