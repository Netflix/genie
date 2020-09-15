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
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieUserLimitExceededException;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieApplicationNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieCommandNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieIdAlreadyExistsException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobSpecificationNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import com.netflix.genie.web.exceptions.checked.AttachmentTooLargeException;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.JobNotFoundException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.exceptions.checked.PreconditionFailedException;
import com.netflix.genie.web.util.MetricsConstants;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.validation.ConstraintViolationException;
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
     * @param e The exception to handle
     * @return An {@link ResponseEntity} instance
     */
    @ExceptionHandler(GenieException.class)
    public ResponseEntity<GenieException> handleGenieException(final GenieException e) {
        this.countExceptionAndLog(e);
        HttpStatus status = HttpStatus.resolve(e.getErrorCode());
        if (status == null) {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        return new ResponseEntity<>(e, status);
    }

    /**
     * Handle Genie runtime exceptions.
     *
     * @param e The Genie exception to handle
     * @return A {@link ResponseEntity} with the exception mapped to a {@link HttpStatus}
     */
    @ExceptionHandler(GenieRuntimeException.class)
    public ResponseEntity<GenieRuntimeException> handleGenieRuntimeException(final GenieRuntimeException e) {
        this.countExceptionAndLog(e);
        if (
            e instanceof GenieApplicationNotFoundException
                || e instanceof GenieCommandNotFoundException
                || e instanceof GenieClusterNotFoundException
                || e instanceof GenieJobNotFoundException
                || e instanceof GenieJobSpecificationNotFoundException
        ) {
            return new ResponseEntity<>(e, HttpStatus.NOT_FOUND);
        } else if (e instanceof GenieIdAlreadyExistsException) {
            return new ResponseEntity<>(e, HttpStatus.CONFLICT);
        } else {
            return new ResponseEntity<>(e, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Handle {@link GenieCheckedException} instances.
     *
     * @param e The exception to map
     * @return A {@link ResponseEntity} with the exception mapped to a {@link HttpStatus}
     */
    @ExceptionHandler(GenieCheckedException.class)
    public ResponseEntity<GenieCheckedException> handleGenieCheckedException(final GenieCheckedException e) {
        this.countExceptionAndLog(e);
        if (e instanceof GenieJobResolutionException) {
            // Mapped to Precondition failed to maintain existing contract with V3
            return new ResponseEntity<>(e, HttpStatus.PRECONDITION_FAILED);
        } else if (e instanceof IdAlreadyExistsException) {
            return new ResponseEntity<>(e, HttpStatus.CONFLICT);
        } else if (e instanceof JobNotFoundException | e instanceof NotFoundException) {
            return new ResponseEntity<>(e, HttpStatus.NOT_FOUND);
        } else if (e instanceof PreconditionFailedException) {
            return new ResponseEntity<>(e, HttpStatus.BAD_REQUEST);
        } else if (e instanceof AttachmentTooLargeException) {
            return new ResponseEntity<>(e, HttpStatus.PAYLOAD_TOO_LARGE);
        } else {
            return new ResponseEntity<>(e, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Handle constraint violation exceptions.
     *
     * @param cve The exception to handle
     * @return A {@link ResponseEntity} instance
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<GeniePreconditionException> handleConstraintViolation(
        final ConstraintViolationException cve
    ) {
        this.countExceptionAndLog(cve);
        return new ResponseEntity<>(
            new GeniePreconditionException(cve.getMessage(), cve),
            HttpStatus.PRECONDITION_FAILED
        );
    }

    /**
     * Handle MethodArgumentNotValid  exceptions.
     *
     * @param e The exception to handle
     * @return A {@link ResponseEntity} instance
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<GeniePreconditionException> handleMethodArgumentNotValidException(
        final MethodArgumentNotValidException e
    ) {
        this.countExceptionAndLog(e);
        return new ResponseEntity<>(
            new GeniePreconditionException(e.getMessage(), e),
            HttpStatus.PRECONDITION_FAILED
        );
    }

    private void countExceptionAndLog(final Exception e) {
        final Set<Tag> tags = Sets.newHashSet(
            Tags.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, e.getClass().getCanonicalName())
        );

        if (e instanceof GenieUserLimitExceededException) {
            final GenieUserLimitExceededException userLimitExceededException = (GenieUserLimitExceededException) e;
            tags.add(Tag.of(USER_NAME_TAG_KEY, userLimitExceededException.getUser()));
            tags.add(Tag.of(LIMIT_TAG_KEY, userLimitExceededException.getExceededLimitName()));
        }

        this.registry.counter(CONTROLLER_EXCEPTION_COUNTER_NAME, tags).increment();

        log.error("{}: {}", e.getClass().getSimpleName(), e.getLocalizedMessage());
        log.debug("{}: {}", e.getClass().getCanonicalName(), e.getMessage(), e);
    }
}
