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
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
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
@ControllerAdvice
public class GenieExceptionMapper {

    private static final String NEW_LINE = "\n";

    private final Counter badRequestRate;
    private final Counter conflictRate;
    private final Counter notFoundRate;
    private final Counter preconditionRate;
    private final Counter serverRate;
    private final Counter serverUnavailableRate;
    private final Counter timeoutRate;
    private final Counter genieRate;
    private final Counter constraintViolationRate;

    /**
     * Constructor.
     *
     * @param registry The metrics registry
     */
    @Autowired
    public GenieExceptionMapper(final Registry registry) {
        this.badRequestRate = registry.counter("genie.exceptions.badRequest.rate");
        this.conflictRate = registry.counter("genie.exceptions.conflict.rate");
        this.notFoundRate = registry.counter("genie.exceptions.notFound.rate");
        this.preconditionRate = registry.counter("genie.exceptions.precondition.rate");
        this.serverRate = registry.counter("genie.exceptions.server.rate");
        this.serverUnavailableRate = registry.counter("genie.exceptions.serverUnavailable.rate");
        this.timeoutRate = registry.counter("genie.exceptions.timeout.rate");
        this.genieRate = registry.counter("genie.exceptions.other.rate");
        this.constraintViolationRate = registry.counter("genie.exceptions.constraintViolation.rate");
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
        } else {
            this.genieRate.increment();
        }
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
        response.sendError(HttpStatus.PRECONDITION_FAILED.value(), builder.toString());
    }
}
