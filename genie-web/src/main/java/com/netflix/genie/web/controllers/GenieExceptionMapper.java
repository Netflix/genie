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
package com.netflix.genie.web.controllers;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieUserLimitExceededException;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
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

/**
 * Exception mapper for Genie Exceptions.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
@ControllerAdvice
public class GenieExceptionMapper {

    private static final String NEW_LINE = "\n";

    private final Registry registry;
    private final Id exceptionCounterId;

    /**
     * Constructor.
     *
     * @param registry The metrics registry
     */
    @Autowired
    public GenieExceptionMapper(final Registry registry) {
        this.registry = registry;
        this.exceptionCounterId = registry.createId("genie.web.controllers.exception");
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
        Id taggedId = this.exceptionCounterId
            .withTag(MetricsConstants.TagKeys.EXCEPTION_CLASS, e.getClass().getCanonicalName());

        if (e instanceof GenieUserLimitExceededException) {
            final GenieUserLimitExceededException userLimitExceededException = (GenieUserLimitExceededException) e;
            taggedId = taggedId
                .withTag("user", userLimitExceededException.getUser())
                .withTag("limit", userLimitExceededException.getExceededLimitName());
        }

        this.registry.counter(taggedId).increment();
    }
}
