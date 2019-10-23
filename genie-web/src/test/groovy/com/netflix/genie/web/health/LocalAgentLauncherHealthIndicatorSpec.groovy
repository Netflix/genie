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
package com.netflix.genie.web.health

import com.netflix.genie.common.internal.util.GenieHostInfo
import com.netflix.genie.web.data.services.JobSearchService
import com.netflix.genie.web.properties.LocalAgentLauncherProperties
import org.springframework.boot.actuate.health.Status
import spock.lang.Specification

/**
 * Specifications for {@link LocalAgentLauncherHealthIndicator}.
 *
 * @author tgianos
 */
class LocalAgentLauncherHealthIndicatorSpec extends Specification {

    def "Can report health"() {
        def hostname = UUID.randomUUID().toString()
        def genieHostInfo = Mock(GenieHostInfo) {
            getHostname() >> hostname
        }
        def jobSearchService = Mock(JobSearchService)
        def maxTotalJobMemory = 100_003L
        def maxJobMemory = 10_000
        def properties = Mock(LocalAgentLauncherProperties) {
            getMaxTotalJobMemory() >> maxTotalJobMemory
            getMaxJobMemory() >> maxJobMemory
        }

        def healthIndicator = new LocalAgentLauncherHealthIndicator(
            jobSearchService,
            properties,
            genieHostInfo
        )

        when: "Available memory is equal to one max job"
        def health = healthIndicator.health()

        then: "The system reports healthy"
        1 * jobSearchService.getAllocatedMemoryOnHost(hostname) >> maxTotalJobMemory - maxJobMemory
        1 * jobSearchService.getUsedMemoryOnHost(hostname) >> maxTotalJobMemory - 2 * maxJobMemory
        1 * jobSearchService.getActiveJobCountOnHost(hostname) >> 335L
        health.getStatus() == Status.UP
        health.getDetails().get(LocalAgentLauncherHealthIndicator.NUMBER_RUNNING_JOBS_KEY) == 335L
        health.getDetails().get(LocalAgentLauncherHealthIndicator.ALLOCATED_MEMORY_KEY) == maxTotalJobMemory - maxJobMemory
        health.getDetails().get(LocalAgentLauncherHealthIndicator.AVAILABLE_MEMORY) == maxTotalJobMemory - (maxTotalJobMemory - maxJobMemory)
        health.getDetails().get(LocalAgentLauncherHealthIndicator.USED_MEMORY_KEY) == maxTotalJobMemory - 2 * maxJobMemory
        health.getDetails().get(LocalAgentLauncherHealthIndicator.AVAILABLE_MAX_JOB_CAPACITY) == ((maxTotalJobMemory - (maxTotalJobMemory - maxJobMemory)) / maxJobMemory).toInteger()

        when: "Available memory is equal to more than one max job"
        health = healthIndicator.health()

        then: "The system reports healthy"
        1 * jobSearchService.getAllocatedMemoryOnHost(hostname) >> maxTotalJobMemory - maxJobMemory - 1
        1 * jobSearchService.getUsedMemoryOnHost(hostname) >> maxTotalJobMemory - 2 * maxJobMemory
        1 * jobSearchService.getActiveJobCountOnHost(hostname) >> 337L
        health.getStatus() == Status.UP
        health.getDetails().get(LocalAgentLauncherHealthIndicator.NUMBER_RUNNING_JOBS_KEY) == 337L
        health.getDetails().get(LocalAgentLauncherHealthIndicator.ALLOCATED_MEMORY_KEY) == maxTotalJobMemory - maxJobMemory - 1
        health.getDetails().get(LocalAgentLauncherHealthIndicator.AVAILABLE_MEMORY) == maxTotalJobMemory - (maxTotalJobMemory - maxJobMemory - 1)
        health.getDetails().get(LocalAgentLauncherHealthIndicator.USED_MEMORY_KEY) == maxTotalJobMemory - 2 * maxJobMemory
        health.getDetails().get(LocalAgentLauncherHealthIndicator.AVAILABLE_MAX_JOB_CAPACITY) == ((maxTotalJobMemory - (maxTotalJobMemory - maxJobMemory - 1)) / maxJobMemory).toInteger()

        when: "Available memory is less than one max job"
        health = healthIndicator.health()

        then: "The system reports down"
        1 * jobSearchService.getAllocatedMemoryOnHost(hostname) >> maxTotalJobMemory - maxJobMemory + 1
        1 * jobSearchService.getUsedMemoryOnHost(hostname) >> maxTotalJobMemory - 2 * maxJobMemory
        1 * jobSearchService.getActiveJobCountOnHost(hostname) >> 343L
        health.getStatus() == Status.DOWN
        health.getDetails().get(LocalAgentLauncherHealthIndicator.NUMBER_RUNNING_JOBS_KEY) == 343L
        health.getDetails().get(LocalAgentLauncherHealthIndicator.ALLOCATED_MEMORY_KEY) == maxTotalJobMemory - maxJobMemory + 1
        health.getDetails().get(LocalAgentLauncherHealthIndicator.AVAILABLE_MEMORY) == maxTotalJobMemory - (maxTotalJobMemory - maxJobMemory + 1)
        health.getDetails().get(LocalAgentLauncherHealthIndicator.USED_MEMORY_KEY) == maxTotalJobMemory - 2 * maxJobMemory
        health.getDetails().get(LocalAgentLauncherHealthIndicator.AVAILABLE_MAX_JOB_CAPACITY) == ((maxTotalJobMemory - (maxTotalJobMemory - maxJobMemory + 1)) / maxJobMemory).toInteger()
    }
}
