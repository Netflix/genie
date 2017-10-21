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

import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.health.GenieCpuHealthIndicator
import com.netflix.spectator.api.Counter
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Timer
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
class HealthCheckMetricsAspectSpec extends Specification{
    Registry registry;
    Id metricId;
    Timer timer;
    Counter counter;
    HealthCheckMetricsAspect aspect

    def setup(){
        registry = Mock(Registry.class)
        metricId = Mock(Id.class)
        timer = Mock(Timer.class)
        counter = Mock(Counter.class)
        registry.createId(_) >> metricId
        registry.timer(_) >> timer
        registry.counter(_) >> counter
        aspect = new HealthCheckMetricsAspect(registry)
    }

    def testHealtEndpointTimer() {
        given:
        def expectedTags = [
                status:'UP',
        ]
        def joinPoint = Mock(ProceedingJoinPoint.class)
        def health = new Health.Builder().up().build()
        joinPoint.proceed(_) >> health;
        Health retVal

        when:
        retVal = aspect.healthEndpointInvokeMonitor(joinPoint)

        then:
        1 * metricId.withTags(expectedTags)
        1 * timer.record(_, TimeUnit.NANOSECONDS);
        retVal == health
    }

    def testHealtEndpointTimerException() {
        given:
        def expectedTags = [
                status:'UNKNOWN',
                exceptionClass: 'java.lang.RuntimeException'
        ]
        def joinPoint = Mock(ProceedingJoinPoint.class)
        joinPoint.proceed(_) >> { throw new RuntimeException("test") }

        when:
        aspect.healthEndpointInvokeMonitor(joinPoint)

        then:
        1 * metricId.withTags(expectedTags)
        1 * timer.record(_, TimeUnit.NANOSECONDS);
        thrown(RuntimeException.class)
    }

    def testHealthIndicatorTimer() {
        given:
        def health = new Health.Builder().up().build()
        def target = Mock(GenieCpuHealthIndicator.class)
        target.getClass() >> GenieCpuHealthIndicator.class
        def joinPoint = Mock(ProceedingJoinPoint.class)
        joinPoint.getTarget() >> target
        joinPoint.proceed(_) >> health
        def timerTagsCapture
        metricId.withTags(_) >> { args ->
            timerTagsCapture = args[0]
            return metricId;
        }
        Health retVal

        when:
        retVal = aspect.healthIndicatorHealthMonitor(joinPoint)

        then:
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        timerTagsCapture.get("status") == 'success'
        timerTagsCapture.get("healthIndicatorClass").startsWith(GenieCpuHealthIndicator.class.getSimpleName())
        retVal == health
    }

    def testHealthIndicatorTimerException() {
        given:
        def target = Mock(GenieCpuHealthIndicator.class)
        target.getClass() >> GenieCpuHealthIndicator.class
        def joinPoint = Mock(ProceedingJoinPoint.class)
        joinPoint.getTarget() >> target
        joinPoint.proceed(_) >> {throw new RuntimeException("test")}
        def timerTagsCapture
        metricId.withTags(_) >> { args ->
            timerTagsCapture = args[0]
            return metricId;
        }

        when:
        aspect.healthIndicatorHealthMonitor(joinPoint)

        then:
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        timerTagsCapture.get("status") == 'failure'
        timerTagsCapture.get("exceptionClass") == RuntimeException.getCanonicalName()
        thrown(RuntimeException.class)
    }

    def testAbstractHealthIndicatorTimer() {
        given:
        def target = Mock(GenieCpuHealthIndicator.class)
        target.getClass() >> GenieCpuHealthIndicator.class
        def joinPoint = Mock(ProceedingJoinPoint.class)
        joinPoint.getTarget() >> target
        joinPoint.proceed(_) >> new Health.Builder().up().build()
        def timerTagsCapture
        metricId.withTags(_) >> { args ->
            timerTagsCapture = args[0]
            return metricId;
        }

        when:
        aspect.abstractHealthIndicatorDoHealthCheckMonitor(joinPoint)

        then:
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        timerTagsCapture.get("status") == 'success'
        timerTagsCapture.get("healthIndicatorClass").startsWith(GenieCpuHealthIndicator.class.getSimpleName())
    }

    def testAbstractHealthIndicatorTimerException() {
        given:
        def target = Mock(GenieCpuHealthIndicator.class)
        target.getClass() >> GenieCpuHealthIndicator.class
        def joinPoint = Mock(ProceedingJoinPoint.class)
        joinPoint.getTarget() >> target
        joinPoint.proceed(_) >> {throw new RuntimeException("test")}
        def timerTagsCapture = new ArrayList()
        metricId.withTags(_) >> { args ->
            timerTagsCapture = args[0]
            return metricId;
        }

        when:
        aspect.abstractHealthIndicatorDoHealthCheckMonitor(joinPoint)

        then:
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        timerTagsCapture.get("status") == 'failure'
        timerTagsCapture.get("exceptionClass") == RuntimeException.getCanonicalName()
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

        def expectedTags1 = [
                healthIndicatorName: 'indicator1',
                status:'UP',
        ]
        def expectedTags2 = [
                healthIndicatorName: 'indicator2',
                status:'OUT_OF_SERVICE',
        ]

        def counterTagsCapture = new ArrayList()
        metricId.withTags(_) >> { args ->
            counterTagsCapture.add(args[0])
            return metricId;

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
        joinPoint.getArgs() >> null

        metricId.withTags(_) >> metricId

        when:
        aspect.abstractHealthAggregatorAggregateDetailsMonitor(joinPoint)

        then:
        0 * counter.increment()
        0 * counter.increment(_)
    }

}
