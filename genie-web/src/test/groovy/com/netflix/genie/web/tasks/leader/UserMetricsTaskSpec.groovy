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

import com.netflix.genie.common.dto.UserJobCount
import com.netflix.genie.common.dto.UserMemoryAmount
import com.netflix.genie.core.services.JobSearchService
import com.netflix.genie.core.util.MetricsConstants
import com.netflix.genie.web.properties.UserMetricsProperties
import com.netflix.genie.web.tasks.GenieTaskScheduleType
import com.netflix.spectator.api.Gauge
import com.netflix.spectator.api.Registry
import spock.lang.Specification

class UserMetricsTaskSpec extends Specification {
    UserMetricsTask task
    Registry registry
    JobSearchService jobSearchService
    UserMetricsProperties userMetricsProperties

    void setup() {
        this.registry = Mock(Registry)
        this.jobSearchService = Mock(JobSearchService)
        this.userMetricsProperties = Mock(UserMetricsProperties)
        this.task = new UserMetricsTask(registry, jobSearchService, userMetricsProperties)
    }

    def "Run"() {
        Gauge fooJobs = Mock(Gauge)
        Gauge barJobs = Mock(Gauge)
        Gauge booJobs = Mock(Gauge)
        Gauge  fooMem = Mock(Gauge)
        Gauge  barMem = Mock(Gauge)
        Gauge  bazMem = Mock(Gauge)

        when:
        assert task.getScheduleType() == GenieTaskScheduleType.FIXED_RATE
        assert task.getFixedRate() == 3000L

        then:
        1 * userMetricsProperties.getRate() >> 3000L

        when:
        task.run()

        then:
        1 * jobSearchService.getActiveJobCountsPerUser() >> {
            return [
                new UserJobCount("foo", 3),
                new UserJobCount("bar", 4),
            ]
        }
        1 * jobSearchService.getActiveJobMemoryPerUser() >> {
            return [
                    new UserMemoryAmount("foo", 1024),
                    new UserMemoryAmount("bar", 2048),
            ]
        }
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "foo"
        ) >> fooJobs
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "bar"
        ) >> barJobs
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "foo"
        ) >> fooMem
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "bar"
        ) >> barMem
        1 * fooJobs.set(3.0)
        1 * barJobs.set(4.0)
        1 * fooMem.set(1024.0)
        1 * barMem.set(2048.0)

        when:

        when:
        task.run()

        then:
        1 * jobSearchService.getActiveJobCountsPerUser() >> {
            return [
                    new UserJobCount("boo", 5),
                    new UserJobCount("bar", 2),
            ]
        }
        1 * jobSearchService.getActiveJobMemoryPerUser() >> {
            return [
                    new UserMemoryAmount("foo", 512),
                    new UserMemoryAmount("baz", 2048),
            ]
        }
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "boo"
        ) >> booJobs
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "bar"
        ) >> barJobs
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "foo"
        ) >> fooMem
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "baz"
        ) >> bazMem
        // Reset
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "foo"
        ) >> fooJobs
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "bar"
        ) >> barMem

        1 * fooJobs.set(0.0)
        1 * booJobs.set(5.0)
        1 * barJobs.set(2.0)
        1 * fooMem.set(512.0)
        1 * barMem.set(0.0)
        1 * bazMem.set(2048.0)

        when:
        task.run()

        then:
        1 * jobSearchService.getActiveJobCountsPerUser() >> { return [] }
        1 * jobSearchService.getActiveJobMemoryPerUser() >> { return [] }
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "boo"
        ) >> booJobs
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_JOBS_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "bar"
        ) >> barJobs
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "foo"
        ) >> fooMem
        1 * registry.gauge(
                UserMetricsTask.USER_ACTIVE_MEMORY_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                "baz"
        ) >> bazMem
        1 * booJobs.set(0.0)
        1 * barJobs.set(0.0)
        1 * fooMem.set(0.0)
        1 * bazMem.set(0.0)

    }
}
