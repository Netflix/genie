package com.netflix.genie.web.controllers;

import com.netflix.genie.core.util.MetricsConstants;
import com.netflix.genie.core.util.MetricsUtils;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Aspect
@Component
@Slf4j
public class JobSearchAspect {

    private static final String HEALTH_INDICATOR_NAME = "healthIndicator";

    @Autowired
    private Registry registry;

    @Around(
        "execution("
            + "  org.springframework.boot.actuate.health.Health "
            + "  org.springframework.boot.actuate.endpoint.HealthEndpoint.invoke()"
            + ")"
    )
    public Health healthEndpointInvokeMonitor(ProceedingJoinPoint joinPoint) throws Throwable {
        final long start = System.nanoTime();
        final Health h;
        final Map<String, String> tags = MetricsUtils.newSuccessTagsMap();
        try {
            h = (Health) joinPoint.proceed(joinPoint.getArgs());
            tags.put(MetricsConstants.TagKeys.STATUS, h.getStatus().toString());
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            final long turnaround = System.nanoTime() - start;
            log.debug("HealthEndpoint.invoke() completed in {} ns", turnaround);
            registry
                .timer(registry.createId("health-check.endpoint.timer", tags))
                .record(turnaround, TimeUnit.NANOSECONDS);
        }
        return h;
    }

    @Around(
        "execution("
            + "  org.springframework.boot.actuate.health.Health "
            + "  org.springframework.boot.actuate.health.HealthIndicator.health()"
            + ")"
    )
    public Health healthIndicatorHealthMonitor(ProceedingJoinPoint joinPoint) throws Throwable {
        final long start = System.nanoTime();
        final Health h;
        Throwable throwable = null;
        try {
            h = (Health) joinPoint.proceed(joinPoint.getArgs());
        } catch (Throwable t) {
            throwable = t;
            throw t;
        } finally {
            final long turnaround = System.nanoTime() - start;
            recordHealthIndicatorTurnaround(turnaround, joinPoint, throwable);
        }
        return h;
    }

    @Around(
        "execution("
            + "  void " +
            "    org.springframework.boot.actuate.health.AbstractHealthIndicator.doHealthCheck("
            + "    org.springframework.boot.actuate.health.Health.Builder"
            + "  )"
            + ")"
    )
    public void AbstractHealthIndicatorDoHealthCheckMonitor(ProceedingJoinPoint joinPoint) throws Throwable {
        final long start = System.nanoTime();
        Throwable throwable = null;
        try {
            joinPoint.proceed(joinPoint.getArgs());
        } catch (Throwable t) {
            throwable = t;
            throw t;
        } finally {
            final long turnaround = System.nanoTime() - start;
            recordHealthIndicatorTurnaround(turnaround, joinPoint, throwable);
        }
    }

    private void recordHealthIndicatorTurnaround(
        long turnaround,
        ProceedingJoinPoint joinPoint,
        @Nullable Throwable throwable
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
        tags.put(HEALTH_INDICATOR_NAME, joinPoint.getTarget().getClass().getSimpleName());
        registry
            .timer(registry.createId("health-check.indicator.timer", tags))
            .record(turnaround, TimeUnit.NANOSECONDS);
    }
}
