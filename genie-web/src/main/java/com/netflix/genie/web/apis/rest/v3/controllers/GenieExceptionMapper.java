/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.web.apis.rest.v3.controllers;

import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieUserLimitExceededException;
import com.netflix.genie.web.util.MetricsConstants;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import java.io.IOException;
import java.util.Set;

/**
 * Exception mapper for Genie Exceptions.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
@ControllerAdvice
public class GenieExceptionMapper {

    // TODO: Not changing this while changing controller package due to need to keep dashboards in sync but we should
    //       rename it going forward - TJG 7/17/19
    static final String CONTROLLER_EXCEPTION_COUNTER_NAME = "genie.web.controllers.exception";
    private static final String NEW_LINE = "\n";
    private static final String USER_NAME_TAG_KEY = "user";
    private static final String LIMIT_TAG_KEY = "limit";

    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param registry The metrics registry
     */
    @Autowired
    public GenieExceptionMapper(final MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * Handle Genie Exceptions.
     *
     * @param response The HTTP response
     * @param e        The exception to handle
     * @throws IOException on error in sending error
     */
    @ExceptionHandler(GenieException.class)
    public void handleGenieException(
        final HttpServletResponse response,
        final GenieException e
    ) throws IOException {
        this.countException(e);
        log.error(e.getLocalizedMessage(), e);
        response.sendError(e.getErrorCode(), e.getLocalizedMessage());
    }

    /**
     * Handle constraint violation exceptions.
     *
     * @param response The HTTP response
     * @param cve      The exception to handle
     * @throws IOException on error in sending error
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public void handleConstraintViolation(
        final HttpServletResponse response,
        final ConstraintViolationException cve
    ) throws IOException {
        final StringBuilder builder = new StringBuilder();
        if (cve.getConstraintViolations() != null) {
            for (final ConstraintViolation<?> cv : cve.getConstraintViolations()) {
                if (builder.length() != 0) {
                    builder.append(NEW_LINE);
                }
                builder.append(cv.getMessage());
            }
        }
        this.countException(cve);
        log.error(cve.getLocalizedMessage(), cve);
        response.sendError(HttpStatus.PRECONDITION_FAILED.value(), builder.toString());
    }

    /**
     * Handle MethodArgumentNotValid  exceptions.
     *
     * @param response The HTTP response
     * @param e        The exception to handle
     * @throws IOException on error in sending error
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public void handleMethodArgumentNotValidException(
        final HttpServletResponse response,
        final MethodArgumentNotValidException e
    ) throws IOException {
        this.countException(e);
        final String errorMessage = e.getMessage();
        log.error(errorMessage, e);
        response.sendError(HttpStatus.PRECONDITION_FAILED.value(), errorMessage);
    }

    private void countException(final Exception e) {
        final Set<Tag> tags = Sets.newHashSet(
            Tags.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, e.getClass().getCanonicalName())
        );

        if (e instanceof GenieUserLimitExceededException) {
            final GenieUserLimitExceededException userLimitExceededException = (GenieUserLimitExceededException) e;
            tags.add(Tag.of(USER_NAME_TAG_KEY, userLimitExceededException.getUser()));
            tags.add(Tag.of(LIMIT_TAG_KEY, userLimitExceededException.getExceededLimitName()));
        }

        this.registry.counter(CONTROLLER_EXCEPTION_COUNTER_NAME, tags).increment();
    }
}
