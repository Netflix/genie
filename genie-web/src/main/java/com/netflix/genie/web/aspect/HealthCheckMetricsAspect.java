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
package com.netflix.genie.web.aspect;

import com.google.common.collect.Maps;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Aspect woven into Spring Boot 'Health' machinery to publish metrics such as time taken, errors and other signals
 * useful for dashboards and alerting.
 *
 * @author mprimi
 * @since 3.2.4
 */
@Aspect
//@Component
@Slf4j
public class HealthCheckMetricsAspect {

    private static final String HEALTH_ENDPOINT_TIMER_NAME = "genie.health.endpoint.timer";
    private static final String HEALTH_INDICATOR_TIMER_METRIC_NAME = "genie.health.indicator.timer";
    private static final String HEALTH_INDICATOR_COUNTER_METRIC_NAME = "genie.health.indicator.counter";
    private static final String HEALTH_FAILURES_COUNTER_METRIC_NAME = "genie.health.failure.counter";
    private static final String HEALTH_INDICATOR_CLASS_TAG_NAME = "healthIndicatorClass";
    private static final String HEALTH_INDICATOR_NAME_TAG_NAME = "healthIndicatorName";

    private final Registry registry;
    private final Id healthEndpointTimerId;
    private final Id healthIndicatorTimerId;
    private final Id healthIndicatorCounterId;
    private final Id healthIndicatorFailureCounterId;

    /**
     * Autowired constructor.
     *
     * @param registry metrics registry
     */
    @Autowired
    public HealthCheckMetricsAspect(final Registry registry) {
        this.registry = registry;
        this.healthEndpointTimerId = registry.createId(HEALTH_ENDPOINT_TIMER_NAME);
        this.healthIndicatorTimerId = registry.createId(HEALTH_INDICATOR_TIMER_METRIC_NAME);
        this.healthIndicatorCounterId = registry.createId(HEALTH_INDICATOR_COUNTER_METRIC_NAME);
        this.healthIndicatorFailureCounterId = registry.createId(HEALTH_FAILURES_COUNTER_METRIC_NAME);
    }

    /**
     * Intercept call to the Health endpoint publish a timer tagged with error, status.
     *
     * @param joinPoint joinPoint for the actual call to invoke()
     * @return Health, as returned by the actual invocation
     * @throws Throwable as thrown by joinPoint.proceed()
     */
    @Around(
        "execution("
            + "  org.springframework.boot.actuate.health.Health"
            + "  org.springframework.boot.actuate.health.HealthEndpoint.health()"
            + ")"
    )
    @SuppressWarnings("checkstyle:IllegalThrows") // For propagating Throwable from joinPoint.proceed()
    public Health healthEndpointInvokeMonitor(final ProceedingJoinPoint joinPoint) throws Throwable {
        final long start = System.nanoTime();
        final Health health;
        Status status = Status.UNKNOWN;
        final Map<String, String> tags = MetricsUtils.newSuccessTagsMap();
        try {
            health = (Health) joinPoint.proceed(joinPoint.getArgs());
            status = health.getStatus();
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            final long turnaround = System.nanoTime() - start;
            tags.put(MetricsConstants.TagKeys.STATUS, status.toString());
            log.debug("HealthEndpoint.invoke() completed in {} ns", turnaround);
            registry
                .timer(healthEndpointTimerId.withTags(tags))
                .record(turnaround, TimeUnit.NANOSECONDS);
        }
        return health;
    }

    /**
     * Intercept call to HealthIndicator beans loaded and publish a timer tagged with error, if any.
     *
     * @param joinPoint joinPoint for the actual call to health()
     * @return Health, as returned by the actual invocation
     * @throws Throwable as thrown by joinPoint.proceed()
     */
    @Around(
        "execution("
            + "  org.springframework.boot.actuate.health.Health"
            + "  org.springframework.boot.actuate.health.HealthIndicator.health()"
            + ")"
    )
    @SuppressWarnings("checkstyle:IllegalThrows") // For propagating Throwable from joinPoint.proceed()
    public Health healthIndicatorHealthMonitor(final ProceedingJoinPoint joinPoint) throws Throwable {
        final long start = System.nanoTime();
        final Health h;
        Throwable throwable = null;
        try {
            h = (Health) joinPoint.proceed(joinPoint.getArgs());
        } catch (final Throwable t) {
            throwable = t;
            throw t;
        } finally {
            final long turnaround = System.nanoTime() - start;
            recordHealthIndicatorTurnaround(turnaround, joinPoint, throwable);
        }
        return h;
    }

    /**
     * Intercept call to AbstractHealthIndicator beans loaded and publish a timer tagged with error, if any.
     * This interception is required because these beans are not captured by HealthIndicator.doHealthCheck(),
     * even tho they implement that interface.
     *
     * @param joinPoint joinPoint for the actual call to doHealthCheck()
     * @throws Throwable as thrown by joinPoint.proceed()
     */
    @Around(
        "execution("
            + "  void org.springframework.boot.actuate.health.AbstractHealthIndicator.doHealthCheck("
            + "      org.springframework.boot.actuate.health.Health.Builder"
            + "  )"
            + ")"
    )
    @SuppressWarnings("checkstyle:IllegalThrows") // For propagating Throwable from joinPoint.proceed()
    public void abstractHealthIndicatorDoHealthCheckMonitor(final ProceedingJoinPoint joinPoint) throws Throwable {
        final long start = System.nanoTime();
        Throwable throwable = null;
        try {
            joinPoint.proceed(joinPoint.getArgs());
        } catch (final Throwable t) {
            throwable = t;
            throw t;
        } finally {
            final long turnaround = System.nanoTime() - start;
            recordHealthIndicatorTurnaround(turnaround, joinPoint, throwable);
        }
    }

    private void recordHealthIndicatorTurnaround(
        final long turnaround,
        final ProceedingJoinPoint joinPoint,
        @Nullable final Throwable throwable
    ) {
        log.debug(
            "{} completed in {} ns (exception: {})",
            joinPoint.getTarget().getClass().getSimpleName(),
            turnaround,
            throwable != null ? throwable.getClass().getSimpleName() : "none"
        );
        final Map<String, String> tags;
        if (throwable == null) {
            tags = MetricsUtils.newSuccessTagsMap();
        } else {
            tags = MetricsUtils.newFailureTagsMapForException(throwable);
        }
        tags.put(HEALTH_INDICATOR_CLASS_TAG_NAME, joinPoint.getTarget().getClass().getSimpleName());
        registry
            .timer(healthIndicatorTimerId.withTags(tags))
            .record(turnaround, TimeUnit.NANOSECONDS);
    }

    /**
     * Intercept calls to the main AbstractHealthAggregator and publish counters for number of invocation and failures,
     * tagged with status and indicator name.
     *
     * @param joinPoint joinPoint for the aggregateDetails() call
     */
    @Before(
        "execution("
            + "  java.util.Map<String, Object> "
            + "  org.springframework.boot.actuate.health.AbstractHealthAggregator.aggregateDetails("
            + "    java.util.Map<String, org.springframework.boot.actuate.health.Health>"
            + "  )"
            + ")"
    )
    public void abstractHealthAggregatorAggregateDetailsMonitor(final JoinPoint joinPoint) {

        final Map<String, Health> healthDetailsMap;
        try {
            @SuppressWarnings("unchecked") final Map<String, Health> map = (Map<String, Health>) joinPoint.getArgs()[0];
            healthDetailsMap = map;
        } catch (final Throwable t) {
            log.warn(
                "Failed to cast AbstractHealthAggregator health details argument: {}",
                joinPoint.getArgs(),
                t
            );
            return;
        }

        healthDetailsMap.forEach(
            (name, health) -> {

                final HashMap<String, String> tags = Maps.newHashMap();
                tags.put(HEALTH_INDICATOR_NAME_TAG_NAME, name);
                tags.put(MetricsConstants.TagKeys.STATUS, health.getStatus().getCode());
                final boolean isUp = Status.UP.equals(health.getStatus());

                // Count individual health-check executed, and publish tagged with name and status.
                // Good for dashboards, can be grouped and filtered by either.
                registry
                    .counter(healthIndicatorCounterId.withTags(tags))
                    .increment();

                //Count failures (if a healthcheck passees, this counter is incremented by 0), publish tagged with
                // name and status.
                // Good for alerting, any aggregate that is greater than zero for some period of time signals a
                // check that is consistently failing.
                registry
                    .counter(healthIndicatorFailureCounterId.withTags(tags))
                    .increment(isUp ? 0 : 1);
            }
        );
    }
}
