package com.netflix.genie.web.health

import com.sun.management.OperatingSystemMXBean
import org.springframework.boot.actuate.health.Status
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit tests for GenieCpuHealthIndicator.
 *
 * @author amajumdar
 * @since 3.0.0
 */
@Unroll
class GenieCpuHealthIndicatorSpec extends Specification{
    OperatingSystemMXBean operatingSystemMXBean
    GenieCpuHealthIndicator cpuHealthIndicator;

    def setup(){
        operatingSystemMXBean = Mock(OperatingSystemMXBean)
        cpuHealthIndicator = new GenieCpuHealthIndicator(80, operatingSystemMXBean)
    }

    def 'Health should be #status when free memory is #freeMemory and totla is #totalMemory'(){
        given:
        1 * operatingSystemMXBean.getSystemCpuLoad() >> cpuLoad
        expect:
        cpuHealthIndicator.health().getStatus() == status
        where:
        cpuLoad     | status
        0.90        | Status.OUT_OF_SERVICE
        0.81        | Status.OUT_OF_SERVICE
        0.801       | Status.OUT_OF_SERVICE
        0.80        | Status.UP
        0.202       | Status.UP
        0.30        | Status.UP
        0.10        | Status.UP
    }
}
