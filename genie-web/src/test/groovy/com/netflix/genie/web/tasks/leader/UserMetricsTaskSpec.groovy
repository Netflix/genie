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

import com.netflix.genie.common.dto.UserResourcesSummary
import com.netflix.genie.web.properties.UserMetricsProperties
import com.netflix.genie.web.services.JobSearchService
import com.netflix.genie.web.tasks.GenieTaskScheduleType
import com.netflix.genie.web.util.MetricsConstants
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import org.apache.curator.shaded.com.google.common.collect.Maps
import spock.lang.Specification

import java.util.function.ToDoubleFunction

class UserMetricsTaskSpec extends Specification {
    MeterRegistry registry
    JobSearchService jobSearchService
    UserMetricsProperties userMetricProperties
    UserMetricsTask task
    Map<String, Closure<Double>> gaugesFunctions

    void setup() {
        this.registry = Mock(MeterRegistry)
        this.jobSearchService = Mock(JobSearchService)
        this.userMetricProperties = Mock(UserMetricsProperties)
        this.gaugesFunctions = Maps.newHashMap()
    }

    def "Run"() {
        setup:
        Map<String, UserResourcesSummary> summariesMap1 = [
            "foo": new UserResourcesSummary("foo", 10, 1024),
            "bar": new UserResourcesSummary("bar", 20, 2048)
        ]

        Map<String, UserResourcesSummary> summariesMap2 = [
            "foo": new UserResourcesSummary("foo", 30, 4096),
            "boo": new UserResourcesSummary("boo", 1, 512)
        ]

        Map<String, UserResourcesSummary> summariesMap3 = Maps.newHashMap()

        when:
        this.task = new UserMetricsTask(registry, jobSearchService, userMetricProperties)

        then:
        1 * registry.gauge(_ as Meter.Id, _, _ as ToDoubleFunction) >> {
            args -> return captureGauge(args[0] as Meter.Id, args[1] as Object, args[2] as ToDoubleFunction<Object>)
        }

        measureActiveUsers() == 0

        when:
        GenieTaskScheduleType scheduleType = this.task.getScheduleType()
        long refreshInterval = this.task.getFixedRate()

        then:
        1 * userMetricProperties.getRefreshInterval() >> 10_000
        scheduleType == GenieTaskScheduleType.FIXED_RATE
        10_000L == refreshInterval

        when:
        this.task.run()

        then:
        1 * jobSearchService.getUserResourcesSummaries() >> summariesMap1
        4 * registry.gauge(_ as Meter.Id, _, _ as ToDoubleFunction) >> {
            args -> return captureGauge(args[0] as Meter.Id, args[1] as Object, args[2] as ToDoubleFunction<Object>)
        }

        measureActiveUsers() == 2
        measureJobs("foo") == 10
        measureMemory("foo") == 1024
        measureJobs("bar") == 20
        measureMemory("bar") == 2048

        when:
        this.task.run()

        then:
        1 * jobSearchService.getUserResourcesSummaries() >> summariesMap2
        2 * registry.gauge(_ as Meter.Id, _, _ as ToDoubleFunction) >> {
            args -> return captureGauge(args[0] as Meter.Id, args[1] as Object, args[2] as ToDoubleFunction<Object>)
        }

        measureActiveUsers() == 2
        measureJobs("foo") == 30
        measureMemory("foo") == 4096
        measureJobs("bar") == 0
        measureMemory("bar") == 0
        measureJobs("boo") == 1
        measureMemory("boo") == 512

        when:
        this.task.run()

        then:
        1 * jobSearchService.getUserResourcesSummaries() >> summariesMap3
        measureActiveUsers() == 0
        measureJobs("foo") == 0
        measureMemory("foo") == 0
        measureJobs("bar") == 0
        measureMemory("bar") == 0
        measureJobs("boo") == 0
        measureMemory("boo") == 0

        when:
        this.task.cleanup()

        then:
        measureActiveUsers() == 0
        measureJobs("foo") == 0
        measureMemory("foo") == 0
        measureJobs("bar") == 0
        measureMemory("bar") == 0
        measureJobs("boo") == 0
        measureMemory("boo") == 0

        when:
        this.task.run()

        then:
        1 * jobSearchService.getUserResourcesSummaries() >> summariesMap1

        4 * registry.gauge(_ as Meter.Id, _, _ as ToDoubleFunction) >> {
            args -> return captureGauge(args[0] as Meter.Id, args[1] as Object, args[2] as ToDoubleFunction<Object>)
        }

        measureActiveUsers() == 2
        measureJobs("foo") == 10
        measureMemory("foo") == 1024
        measureJobs("bar") == 20
        measureMemory("bar") == 2048
        measureJobs("boo") == 0
        measureMemory("boo") == 0

        when:
        this.task.cleanup()

        then:
        measureActiveUsers() == 0
        measureJobs("foo") == 0
        measureMemory("foo") == 0
        measureJobs("bar") == 0
        measureMemory("bar") == 0
        measureJobs("boo") == 0
        measureMemory("boo") == 0
    }

    Gauge captureGauge(final Meter.Id id, final Object obj, final ToDoubleFunction<Object> f) {
        String userTagValue = id.getTag(MetricsConstants.TagKeys.USER)
        String gaugeKey = id.getName() + (userTagValue == null ? "" : ("-" + userTagValue))
        this.gaugesFunctions.put(gaugeKey, { -> f.applyAsDouble(obj) })
        return Mock(Gauge)
    }

    double measureActiveUsers() {
        return gaugesFunctions.get(UserMetricsTask.USER_ACTIVE_USERS_METRIC_NAME).call()
    }

    double measureJobs(String user) {
        String gaugeKey = UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME + "-" + user
        return gaugesFunctions.get(gaugeKey).call()
    }

    double measureMemory(String user) {
        String gaugeKey = UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME + "-" + user
        return gaugesFunctions.get(gaugeKey).call()
    }

}
