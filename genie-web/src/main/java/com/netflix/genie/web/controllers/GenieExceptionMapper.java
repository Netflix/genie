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

import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.exceptions.GenieTimeoutException;
import com.netflix.genie.common.exceptions.GenieUserLimitExceededException;
import com.netflix.genie.core.util.MetricsConstants;
import com.netflix.spectator.api.Counter;
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
    private final Counter badRequestRate;
    private final Counter conflictRate;
    private final Counter notFoundRate;
    private final Counter preconditionRate;
    private final Counter serverRate;
    private final Counter serverUnavailableRate;
    private final Counter timeoutRate;
    private final Counter genieRate;
    private final Counter constraintViolationRate;
    private final Id userLimitExceededRateId;
    private final Counter methodArgumentNotValidRate;

    /**
     * Constructor.
     *
     * @param registry The metrics registry
     */
    @Autowired
    public GenieExceptionMapper(final Registry registry) {
        this.registry = registry;
        this.badRequestRate = registry.counter(MetricsConstants.GENIE_EXCEPTIONS_BAD_REQUEST_RATE);
        this.conflictRate = registry.counter(MetricsConstants.GENIE_EXCEPTIONS_CONFLICT_RATE);
        this.notFoundRate = registry.counter(MetricsConstants.GENIE_EXCEPTIONS_NOT_FOUND_RATE);
        this.preconditionRate = registry.counter(MetricsConstants.GENIE_EXCEPTIONS_PRECONDITION_RATE);
        this.serverRate = registry.counter(MetricsConstants.GENIE_EXCEPTIONS_SERVER_RATE);
        this.serverUnavailableRate = registry.counter(MetricsConstants.GENIE_EXCEPTIONS_SERVER_UNAVAILABLE_RATE);
        this.timeoutRate = registry.counter(MetricsConstants.GENIE_EXCEPTIONS_TIMEOUT_RATE);
        this.genieRate = registry.counter(MetricsConstants.GENIE_EXCEPTIONS_OTHER_RATE);
        this.constraintViolationRate = registry.counter(MetricsConstants.GENIE_EXCEPTIONS_CONSTRAINT_VIOLATION_RATE);
        this.userLimitExceededRateId = registry.createId(MetricsConstants.GENIE_EXCEPTIONS_USER_LIMIT_EXCEEDED_RATE);
        this.methodArgumentNotValidRate
            = registry.counter(MetricsConstants.GENIE_EXCEPTIONS_METHOD_ARGUMENT_NOT_VALID_RATE);
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
        if (e instanceof GenieBadRequestException) {
            this.badRequestRate.increment();
        } else if (e instanceof GenieConflictException) {
            this.conflictRate.increment();
        } else if (e instanceof GenieNotFoundException) {
            this.notFoundRate.increment();
        } else if (e instanceof GeniePreconditionException) {
            this.preconditionRate.increment();
        } else if (e instanceof GenieServerException) {
            this.serverRate.increment();
        } else if (e instanceof GenieServerUnavailableException) {
            this.serverUnavailableRate.increment();
        } else if (e instanceof GenieTimeoutException) {
            this.timeoutRate.increment();
        } else if (e instanceof GenieUserLimitExceededException) {
            final GenieUserLimitExceededException userLimitExceededException = (GenieUserLimitExceededException) e;
            final Id taggedId = this.userLimitExceededRateId
                .withTag("user", userLimitExceededException.getUser())
                .withTag("limit", userLimitExceededException.getExceededLimitName());
            this.registry.counter(taggedId).increment();
        } else {
            this.genieRate.increment();
        }
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
        this.constraintViolationRate.increment();
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
        this.methodArgumentNotValidRate.increment();
        log.error(e.getMessage(), e);
        response.sendError(HttpStatus.PRECONDITION_FAILED.value(), e.getMessage());
    }
}
