/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.genie.web.health

import com.sun.management.OperatingSystemMXBean
import org.springframework.boot.actuate.health.Status
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit tests for MemoryHealthIndicator.
 *
 * @author amajumdar
 * @since 3.0.0
 */
@Unroll
class MemoryHealthIndicatorSpec extends Specification {
    OperatingSystemMXBean operatingSystemMXBean
    MemoryHealthIndicator memoryHealthIndicator;

    def setup() {
        operatingSystemMXBean = Mock(OperatingSystemMXBean)
        memoryHealthIndicator = new MemoryHealthIndicator(80, operatingSystemMXBean)
    }

    def 'Health should be #status when free memory is #freeMemory and total is #totalMemory'() {
        given:
        operatingSystemMXBean.getFreePhysicalMemorySize() >> freeMemory
        operatingSystemMXBean.getTotalPhysicalMemorySize() >> totalMemory
        expect:
        memoryHealthIndicator.health().getStatus() == status
        where:
        freeMemory | totalMemory | status
        10.0       | 100.0       | Status.OUT_OF_SERVICE
        20.0       | 100.0       | Status.UP
        30.0       | 100.0       | Status.UP
        30.0       | 50.0        | Status.UP
    }
}
