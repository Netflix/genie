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
package com.netflix.genie.web.aspect

import com.google.common.collect.Sets
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.health.GenieCpuHealthIndicator
import com.netflix.genie.web.util.MetricsConstants
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import org.aspectj.lang.ProceedingJoinPoint
import org.junit.experimental.categories.Category
import org.springframework.boot.actuate.health.Health
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * Unit tests for HealthCheckMetricsAspect
 *
 * @author mprimi
 * @since 3.2.4
 */
@Category(UnitTest.class)
class HealthCheckMetricsAspectSpec extends Specification {
    MeterRegistry registry
    io.micrometer.core.instrument.Timer timer
    Counter counter
    HealthCheckMetricsAspect aspect

    def setup() {
        registry = Mock(MeterRegistry.class)
        timer = Mock(io.micrometer.core.instrument.Timer.class)
        counter = Mock(Counter.class)
//        registry.counter(_ as String, _ as Set<Tag>) >> counter
        aspect = new HealthCheckMetricsAspect(registry)
    }

    def testHealthEndpointTimer() {
        given:
        def expectedTags = Sets.newHashSet(Tag.of(MetricsConstants.TagKeys.STATUS, "UP"))
        def joinPoint = Mock(ProceedingJoinPoint.class)
        def health = new Health.Builder().up().build()
        joinPoint.getArgs() >> new Object[0]
        joinPoint.proceed(_ as Object[]) >> health
        Health retVal

        when:
        retVal = aspect.healthEndpointInvokeMonitor(joinPoint)

        then:
        1 * registry.timer(HealthCheckMetricsAspect.HEALTH_ENDPOINT_TIMER_NAME, expectedTags) >> timer
        1 * timer.record(_ as Long, TimeUnit.NANOSECONDS)
        retVal == health
    }

    def testHealthEndpointTimerException() {
        given:
        def expectedTags = Sets.newHashSet(
                Tag.of("status", "UNKNOWN"),
                Tag.of("exceptionClass", "java.lang.RuntimeException")
        )
        def joinPoint = Mock(ProceedingJoinPoint.class)
        joinPoint.getArgs() >> new Object[0]
        joinPoint.proceed(_ as Object[]) >> { throw new RuntimeException("test") }

        when:
        aspect.healthEndpointInvokeMonitor(joinPoint)

        then:
        1 * registry.timer(HealthCheckMetricsAspect.HEALTH_ENDPOINT_TIMER_NAME, expectedTags) >> timer
        1 * timer.record(_ as Long, TimeUnit.NANOSECONDS)
        thrown(RuntimeException.class)
    }

    def testHealthIndicatorTimer() {
        given:
        def health = new Health.Builder().up().build()
        def target = Mock(GenieCpuHealthIndicator.class)
        target.getClass() >> GenieCpuHealthIndicator.class
        def joinPoint = Mock(ProceedingJoinPoint.class)
        joinPoint.getTarget() >> target
        joinPoint.getArgs() >> new Object[0]
        joinPoint.proceed(_ as Object[]) >> health
        Set<Tag> timerTagsCapture
        registry.timer(HealthCheckMetricsAspect.HEALTH_INDICATOR_TIMER_METRIC_NAME, _ as Set<Tag>) >> {
            args ->
                timerTagsCapture = (Set<Tag>) args[1]
                return timer
        }
        Health retVal

        when:
        retVal = aspect.healthIndicatorHealthMonitor(joinPoint)

        then:
        1 * timer.record(_ as Long, TimeUnit.NANOSECONDS)
        timerTagsCapture.size() == 2
        timerTagsCapture.each { tag ->
            if (tag.key == "status") {
                tag.value == "success"
            } else if (tag.key == "healthIndicatorClass") {
                tag.value.startsWith(GenieCpuHealthIndicator.class.getSimpleName())
            }
        }
        retVal == health
    }

    def testHealthIndicatorTimerException() {
        given:
        def target = Mock(GenieCpuHealthIndicator.class)
        target.getClass() >> GenieCpuHealthIndicator.class
        def joinPoint = Mock(ProceedingJoinPoint.class)
        joinPoint.getTarget() >> target
        joinPoint.getArgs() >> new Object[0]
        joinPoint.proceed(_ as Object[]) >> { throw new RuntimeException("test") }

        when:
        aspect.healthIndicatorHealthMonitor(joinPoint)

        then:
        1 * timer.record(_ as Long, TimeUnit.NANOSECONDS)
        1 * registry.timer(
                HealthCheckMetricsAspect.HEALTH_INDICATOR_TIMER_METRIC_NAME,
                Sets.newHashSet(
                        Tag.of("status", "failure"),
                        Tag.of("exceptionClass", RuntimeException.getCanonicalName()),
                        Tag.of("healthIndicatorClass", target.getClass().getSimpleName())
                )
        ) >> timer
        thrown(RuntimeException.class)
    }

    def testAbstractHealthIndicatorTimer() {
        given:
        def target = Mock(GenieCpuHealthIndicator.class)
        target.getClass() >> GenieCpuHealthIndicator.class
        def joinPoint = Mock(ProceedingJoinPoint.class)
        joinPoint.getTarget() >> target
        joinPoint.getArgs() >> new Object[0]
        joinPoint.proceed(_ as Object[]) >> new Health.Builder().up().build()
        Set<Tag> timerTagsCapture
        registry.timer(HealthCheckMetricsAspect.HEALTH_INDICATOR_TIMER_METRIC_NAME, _ as Set<Tag>) >> {
            args ->
                timerTagsCapture = (Set<Tag>) args[1]
                return timer
        }

        when:
        aspect.abstractHealthIndicatorDoHealthCheckMonitor(joinPoint)

        then:
        1 * timer.record(_ as Long, TimeUnit.NANOSECONDS)
        timerTagsCapture.size() == 2
        timerTagsCapture.each { tag ->
            if (tag.key == "status") {
                tag.value == "success"
            } else if (tag.key == "healthIndicatorClass") {
                tag.value.startsWith(GenieCpuHealthIndicator.class.getSimpleName())
            }
        }
    }

    def testAbstractHealthIndicatorTimerException() {
        given:
        def target = Mock(GenieCpuHealthIndicator.class)
        target.getClass() >> GenieCpuHealthIndicator.class
        def joinPoint = Mock(ProceedingJoinPoint.class)
        joinPoint.getTarget() >> target
        joinPoint.getArgs() >> new Object[0]
        joinPoint.proceed(_ as Object[]) >> { throw new RuntimeException("test") }

        when:
        aspect.abstractHealthIndicatorDoHealthCheckMonitor(joinPoint)

        then:
        1 * timer.record(_ as Long, TimeUnit.NANOSECONDS)
        1 * registry.timer(
                HealthCheckMetricsAspect.HEALTH_INDICATOR_TIMER_METRIC_NAME,
                Sets.newHashSet(
                        Tag.of("status", "failure"),
                        Tag.of("exceptionClass", RuntimeException.class.getCanonicalName()),
                        Tag.of("healthIndicatorClass", target.getClass().getSimpleName())
                )
        ) >> timer
        thrown(RuntimeException.class)
    }

    def testAbstractHealthAggregatorCounters() {
        given:
        def joinPoint = Mock(ProceedingJoinPoint.class)
        def detailsMap = [
                indicator1: new Health.Builder().up().build(),
                indicator2: new Health.Builder().outOfService().build(),
        ]
        joinPoint.getArgs() >> [detailsMap]

        def expectedTags1 = Sets.newHashSet(
                Tag.of("healthIndicatorName", "indicator1"),
                Tag.of("status", "UP")
        )
        def expectedTags2 = Sets.newHashSet(
                Tag.of("healthIndicatorName", "indicator2"),
                Tag.of("status", "OUT_OF_SERVICE")
        )

        def counterTagsCapture = new ArrayList<Set<Tag>>()
        registry.counter(_ as String, _ as Set<Tag>) >> { args ->
            counterTagsCapture.add((Set<Tag>) args[1])
            return counter
        }

        when:
        aspect.abstractHealthAggregatorAggregateDetailsMonitor(joinPoint)

        then:
        2 * counter.increment()
        1 * counter.increment(0)
        1 * counter.increment(1)

        counterTagsCapture.size() == 4

        counterTagsCapture.get(0) == expectedTags1
        counterTagsCapture.get(1) == expectedTags1
        counterTagsCapture.get(2) == expectedTags2
        counterTagsCapture.get(3) == expectedTags2
    }

    def testAbstractHealthAggregatorCountersError() {
        given:
        def joinPoint = Mock(ProceedingJoinPoint.class)
        joinPoint.getArgs() >> new Object[0]

        when:
        aspect.abstractHealthAggregatorAggregateDetailsMonitor(joinPoint)

        then:
        0 * counter.increment()
        0 * counter.increment(_ as double)
    }

}
