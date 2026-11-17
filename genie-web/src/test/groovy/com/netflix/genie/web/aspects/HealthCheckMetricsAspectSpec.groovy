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
package com.netflix.genie.web.aspects

import com.google.common.collect.ImmutableSet
import com.netflix.genie.web.health.GenieCpuHealthIndicator
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.Signature
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.boot.health.contributor.Status
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * Unit tests for HealthCheckMetricsAspect
 *
 * @author mprimi
 */
class HealthCheckMetricsAspectSpec extends Specification {
    MeterRegistry registry
    io.micrometer.core.instrument.Timer timer
    HealthCheckMetricsAspect aspect
    HealthIndicator healthIndicator
    Object[] args
    Health health
    ProceedingJoinPoint joinPoint
    Signature signature

    def setup() {
        this.registry = Mock(MeterRegistry.class)
        this.timer = Mock(io.micrometer.core.instrument.Timer.class)
        this.healthIndicator = Mock(GenieCpuHealthIndicator.class)
        this.joinPoint = Mock(ProceedingJoinPoint.class)
        this.signature = Mock(Signature.class)

        this.health = Health.up().build()
        this.args = new Object[0]

        this.aspect = new HealthCheckMetricsAspect(registry)
    }

    def "Around HealthIndicator::getHealth(...)"() {
        when:
        Health h = this.aspect.aroundHealthIndicatorGetHealth(joinPoint)

        then:
        1 * joinPoint.getTarget() >> healthIndicator
        1 * joinPoint.getSignature() >> signature
        1 * signature.getName() >> "getHealth"
        2 * joinPoint.getArgs() >> args
        1 * joinPoint.proceed(args) >> health
        1 * registry.timer(
            HealthCheckMetricsAspect.HEALTH_INDICATOR_TIMER_METRIC_NAME,
            ImmutableSet.of(
                Tag.of(HealthCheckMetricsAspect.HEALTH_INDICATOR_CLASS_TAG_NAME, GenieCpuHealthIndicator.class.getSimpleName()),
                Tag.of(HealthCheckMetricsAspect.HEALTH_INDICATOR_STATUS_TAG_NAME, Status.UP.getCode())
            )
        ) >> timer
        1 * timer.record(_ as Long, TimeUnit.NANOSECONDS)
        h == health

        when:
        Throwable exception = new RuntimeException("...")
        this.aspect.aroundHealthIndicatorGetHealth(joinPoint)

        then:
        1 * joinPoint.getTarget() >> healthIndicator
        1 * joinPoint.getSignature() >> signature
        1 * signature.getName() >> "getHealth"
        2 * joinPoint.getArgs() >> args
        1 * joinPoint.proceed(args) >> { throw exception }
        1 * registry.timer(
            HealthCheckMetricsAspect.HEALTH_INDICATOR_TIMER_METRIC_NAME,
            ImmutableSet.of(
                Tag.of(HealthCheckMetricsAspect.HEALTH_INDICATOR_CLASS_TAG_NAME, GenieCpuHealthIndicator.class.getSimpleName()),
                Tag.of(HealthCheckMetricsAspect.HEALTH_INDICATOR_STATUS_TAG_NAME, Status.UNKNOWN.getCode())
            )
        ) >> timer
        1 * timer.record(_ as Long, TimeUnit.NANOSECONDS)
        Throwable t = thrown(RuntimeException.class)
        t == exception
    }

    def "Pointcut matches HealthIndicator::health(boolean) on a real proxy"() {
        setup:
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry()
        AspectJProxyFactory proxyFactory = new AspectJProxyFactory(new UpHealthIndicator())
        proxyFactory.addAspect(new HealthCheckMetricsAspect(meterRegistry))
        HealthIndicator proxy = proxyFactory.getProxy() as HealthIndicator

        when: "the health endpoint invokes the boolean overload through the proxy"
        Health result = proxy.health(true)

        then: "the call is intercepted and recorded exactly once"
        result.getStatus() == Status.UP
        meterRegistry
            .get(HealthCheckMetricsAspect.HEALTH_INDICATOR_TIMER_METRIC_NAME)
            .tag(HealthCheckMetricsAspect.HEALTH_INDICATOR_CLASS_TAG_NAME, UpHealthIndicator.getSimpleName())
            .tag(HealthCheckMetricsAspect.HEALTH_INDICATOR_STATUS_TAG_NAME, Status.UP.getCode())
            .timer()
            .count() == 1L
    }

    def "Pointcut does not match non-HealthIndicator beans"() {
        setup:
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry()
        AspectJProxyFactory proxyFactory = new AspectJProxyFactory(new NotAHealthIndicator())
        // Groovy classes implement GroovyObject, so force CGLIB rather than a JDK interface proxy
        proxyFactory.setProxyTargetClass(true)
        proxyFactory.addAspect(new HealthCheckMetricsAspect(meterRegistry))
        NotAHealthIndicator proxy = proxyFactory.getProxy() as NotAHealthIndicator

        when:
        proxy.health(true)

        then:
        meterRegistry.find(HealthCheckMetricsAspect.HEALTH_INDICATOR_TIMER_METRIC_NAME).timer() == null
    }

    static class UpHealthIndicator implements HealthIndicator {
        @Override
        Health health() {
            return Health.up().build()
        }
    }

    static class NotAHealthIndicator {
        @SuppressWarnings("unused")
        Health health(boolean includeDetails) {
            return Health.up().build()
        }
    }
}
