package com.netflix.genie.web.health

import com.netflix.genie.web.configs.PropertiesConfig
import com.netflix.servo.monitor.BasicDistributionSummary
import com.netflix.servo.monitor.MonitorConfig
import com.sun.management.OperatingSystemMXBean
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
@Unroll
class GenieCpuHealthIndicatorSpec extends Specification{
    OperatingSystemMXBean operatingSystemMXBean
    GenieCpuHealthIndicator cpuHealthIndicator;
    BasicDistributionSummary summary;

    def setup(){
        operatingSystemMXBean = Mock(OperatingSystemMXBean)
        summary = Mock(BasicDistributionSummary)
        def props = new PropertiesConfig().healthProperties()
        cpuHealthIndicator = new GenieCpuHealthIndicator(
                props.getMaxCpuLoadPercent(),
                1,
                operatingSystemMXBean,
                summary,
                new DefaultManagedTaskScheduler())
    }

    def 'Health should be #status when totalCpuLoad is #cpuLoad'(){
        given:
        1 * summary.getTotalAmount() >> cpuLoad
        1 * summary.getCount() >> count
        expect:
        cpuHealthIndicator.health().getStatus() == status
        where:
        cpuLoad  | count    | status
        90       | 1        | Status.OUT_OF_SERVICE
        171      | 2        | Status.OUT_OF_SERVICE
        81.1     | 1        | Status.OUT_OF_SERVICE
        80.1     | 0        | Status.UP
        80       | 5        | Status.UP
        20.2     | 1        | Status.UP
        60       | 2        | Status.UP
        50       | 5        | Status.UP
    }

    def checkHealth(){
        when:
        def okOperatingSystemMXBean = Mock(OperatingSystemMXBean)
        okOperatingSystemMXBean.getSystemCpuLoad() >> 0.75 >> 0.78
        def indicator = new GenieCpuHealthIndicator( 80, 1, okOperatingSystemMXBean,
                new BasicDistributionSummary(MonitorConfig.builder('s').build()), new DefaultManagedTaskScheduler())
        then:
        indicator.health().getStatus() == Status.UP
        when:
        def outOperatingSystemMXBean = Mock(OperatingSystemMXBean)
        outOperatingSystemMXBean.getSystemCpuLoad() >> 0.85 >> 0.88
        indicator = new GenieCpuHealthIndicator( 80, 1, outOperatingSystemMXBean,
                new BasicDistributionSummary(MonitorConfig.builder('s').build()), new DefaultManagedTaskScheduler())
        then:
        indicator.health().getStatus() == Status.OUT_OF_SERVICE
    }
}
