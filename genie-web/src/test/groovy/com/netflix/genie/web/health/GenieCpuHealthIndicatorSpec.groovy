/*
 *
 *  Copyright 2017 Netflix, Inc.
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

import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.properties.HealthProperties
import com.sun.management.OperatingSystemMXBean
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.experimental.categories.Category
import org.springframework.boot.actuate.health.Status
import org.springframework.scheduling.concurrent.DefaultManagedTaskScheduler
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit tests for GenieCpuHealthIndicator.
 *
 * @author amajumdar
 * @since 3.0.0
 */
@Category(UnitTest.class)
@Unroll
class GenieCpuHealthIndicatorSpec extends Specification {
    OperatingSystemMXBean operatingSystemMXBean
    GenieCpuHealthIndicator cpuHealthIndicator
    DistributionSummary summary

    def setup() {
        operatingSystemMXBean = Mock(OperatingSystemMXBean)
        summary = Mock(DistributionSummary)
        def props = new HealthProperties()
        cpuHealthIndicator = new GenieCpuHealthIndicator(
                props.getMaxCpuLoadPercent(),
                1,
                operatingSystemMXBean,
                summary,
                new DefaultManagedTaskScheduler())
    }

    def 'Health should be #status when totalCpuLoad is #cpuLoad'() {
        given:
        1 * summary.totalAmount() >> cpuLoad
        1 * summary.count() >> count
        expect:
        cpuHealthIndicator.health().getStatus() == status
        where:
        cpuLoad | count | status
        90      | 1     | Status.OUT_OF_SERVICE
        171     | 2     | Status.OUT_OF_SERVICE
        81.1    | 1     | Status.OUT_OF_SERVICE
        80.1    | 0     | Status.UP
        80      | 5     | Status.UP
        20.2    | 1     | Status.UP
        60      | 2     | Status.UP
        50      | 5     | Status.UP
    }

    def checkHealth() {
        when:
        def okOperatingSystemMXBean = Mock(OperatingSystemMXBean)
        okOperatingSystemMXBean.getSystemCpuLoad() >> 0.75
        def indicator = new GenieCpuHealthIndicator(
                80,
                1,
                okOperatingSystemMXBean,
                DistributionSummary.builder("s").register(new SimpleMeterRegistry()),
                new DefaultManagedTaskScheduler()
        )
        then:
        indicator.health().getStatus() == Status.UP
        when:
        def outOperatingSystemMXBean = Mock(OperatingSystemMXBean)
        outOperatingSystemMXBean.getSystemCpuLoad() >> 0.85
        indicator = new GenieCpuHealthIndicator(
                80,
                1,
                outOperatingSystemMXBean,
                DistributionSummary.builder("s").register(new SimpleMeterRegistry()),
                new DefaultManagedTaskScheduler()
        )
        then:
        indicator.health().getStatus() == Status.OUT_OF_SERVICE
    }
}
