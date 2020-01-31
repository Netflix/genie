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
package com.netflix.genie.web.aspects;

import com.google.common.collect.ImmutableSet;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Aspect around Spring Boot 'HealthIndicator' to publish metrics for status of individual indicator, as well as their
 * turnaround time.
 *
 * @author mprimi
 * @since 3.2.4
 */
@Aspect
@Slf4j
public class HealthCheckMetricsAspect {

    static final String HEALTH_INDICATOR_TIMER_METRIC_NAME = "genie.health.indicator.timer";
    private static final String HEALTH_INDICATOR_CLASS_TAG_NAME = "healthIndicatorClass";
    private static final String HEALTH_INDICATOR_STATUS_TAG_NAME = "healthIndicatorStatus";

    private final MeterRegistry registry;

    /**
     * Autowired constructor.
     *
     * @param registry metrics registry
     */
    public HealthCheckMetricsAspect(final MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * Pointcut that matches invocations of {@code HealthIndicator::getHealth(..)}.
     */
    @Pointcut(""
        + "target(org.springframework.boot.actuate.health.HealthIndicator+) && "
        + "execution(org.springframework.boot.actuate.health.Health getHealth(..))"
    )
    public void healthIndicatorGetHealth() {
    }

    /**
     * Aspect around join point for {@code HealthIndicator::getHealth(..)}.
     * Measures the turnaround time for the indicator call and publishes it as metric, tagged with indicator class
     * and the status reported.
     *
     * @param joinPoint the join point
     * @return health as reported by the indicator
     * @throws Throwable in case of exception in the join point
     */
    @Around("healthIndicatorGetHealth()")
    @SuppressWarnings("checkstyle:IllegalThrows") // For propagating Throwable from joinPoint.proceed()
    public Health aroundHealthIndicatorGetHealth(final ProceedingJoinPoint joinPoint) throws Throwable {
        final String healthIndicatorClass = joinPoint.getTarget().getClass().getSimpleName();
        log.debug(
            "Intercepted: {}::{}({})",
            healthIndicatorClass,
            joinPoint.getSignature().getName(),
            Arrays.toString(joinPoint.getArgs())
        );

        final long start = System.nanoTime();
        Health h = null;
        try {
            h = (Health) joinPoint.proceed(joinPoint.getArgs());
        } finally {
            final long turnaround = System.nanoTime() - start;
            final String healthStatus = (h != null) ? h.getStatus().getCode() : Status.UNKNOWN.getCode();

            log.debug("Indicator {} status: {} (took {}ns)", healthIndicatorClass, healthStatus, turnaround);

            final Set<Tag> tags = ImmutableSet.of(
                Tag.of(HEALTH_INDICATOR_CLASS_TAG_NAME, healthIndicatorClass),
                Tag.of(HEALTH_INDICATOR_STATUS_TAG_NAME, healthStatus)
            );
            this.registry
                .timer(HEALTH_INDICATOR_TIMER_METRIC_NAME, tags)
                .record(turnaround, TimeUnit.NANOSECONDS);
        }
        return h;
    }
}
