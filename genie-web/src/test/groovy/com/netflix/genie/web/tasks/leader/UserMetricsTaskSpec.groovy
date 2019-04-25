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

import com.google.common.collect.Lists
import com.netflix.genie.common.dto.UserResourcesSummary
import com.netflix.genie.web.properties.UserMetricsProperties
import com.netflix.genie.web.services.JobSearchService
import com.netflix.genie.web.tasks.GenieTaskScheduleType
import com.netflix.genie.web.util.MetricsConstants
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import org.apache.curator.shaded.com.google.common.collect.Maps
import spock.lang.Specification

class UserMetricsTaskSpec extends Specification {
    MeterRegistry registry
    JobSearchService jobSearchService
    UserMetricsProperties userMetricProperties
    UserMetricsTask task

    void setup() {
        this.registry = Mock(MeterRegistry)
        this.jobSearchService = Mock(JobSearchService)
        this.userMetricProperties = Mock(UserMetricsProperties)
    }

    def "Run"() {
        setup:
        this.task = new UserMetricsTask(registry, jobSearchService, userMetricProperties)

        Collection<Tag> fooUserTag = Lists.newArrayList(Tag.of(MetricsConstants.TagKeys.USER, "foo"))
        Collection<Tag> barUserTag = Lists.newArrayList(Tag.of(MetricsConstants.TagKeys.USER, "bar"))
        Collection<Tag> booUserTag = Lists.newArrayList(Tag.of(MetricsConstants.TagKeys.USER, "boo"))

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
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
            fooUserTag,
            10
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
            barUserTag,
            20
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
            fooUserTag,
            1024
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
            barUserTag,
            2048
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_USERS_METRIC_NAME,
            2
        )

        when:
        this.task.run()

        then:
        1 * jobSearchService.getUserResourcesSummaries() >> summariesMap2
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
            fooUserTag,
            30
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
            barUserTag,
            0
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
            booUserTag,
            1
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
            fooUserTag,
            4096
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
            barUserTag,
            0
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
            booUserTag,
            512
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_USERS_METRIC_NAME,
            2
        )

        when:
        this.task.run()

        then:
        1 * jobSearchService.getUserResourcesSummaries() >> summariesMap3
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
            fooUserTag,
            0
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
            booUserTag,
            0
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
            fooUserTag,
            0
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
            booUserTag,
            0
        )
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_USERS_METRIC_NAME,
            0
        )

        when:
        this.task.run()

        then:
        1 * jobSearchService.getUserResourcesSummaries() >> summariesMap3
        0 * registry.gauge(UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME, _, _)
        0 * registry.gauge(UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME, _, _)
        1 * registry.gauge(
            UserMetricsTask.USER_ACTIVE_USERS_METRIC_NAME,
            0
        )
    }
}
